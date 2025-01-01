"""SQLite storage backend implementation."""
import logging
from typing import List, Dict, Any
from urllib.parse import urlparse
import os
from pathlib import Path

from ..base import StorageBackend
from .connection import SQLiteConnectionPool
from .schema import initialize_schema
from .operations.entity_ops import EntityOperations
from .operations.relation_ops import RelationOperations
from .operations.search_ops import SearchOperations
from ...interfaces import Entity, Relation
from ...exceptions import EntityNotFoundError, EntityAlreadyExistsError
from .utils.sanitization import sanitize_input as _sanitize_input

logger = logging.getLogger(__name__)

class SQLiteStorageBackend(StorageBackend):
    """SQLite implementation of the storage backend."""
    
    def __init__(self, database_url: str, echo: bool = False):
        """Initialize SQLite backend with database path extracted from URL."""
        parsed_url = urlparse(database_url)
        if not parsed_url.path:
            raise ValueError("Database path not specified in URL")
            
        # Handle absolute and relative paths
        if parsed_url.path.startswith('/'):
            db_path = parsed_url.path
        else:
            path = parsed_url.path.lstrip('/')
            if '/' in path:  # If path contains directories
                db_path = str(Path(path).absolute())
                os.makedirs(os.path.dirname(db_path), exist_ok=True)
            else:  # Simple filename in current directory
                db_path = path

        # Initialize connection pool and operation handlers
        self.pool = SQLiteConnectionPool(db_path, echo=echo)
        self.entity_ops = EntityOperations(self.pool)
        self.relation_ops = RelationOperations(self.pool)
        self.search_ops = SearchOperations(self.pool)

    async def initialize(self) -> None:
        """Initialize database schema."""
        async with self.pool.get_connection() as conn:
            async with self.pool.transaction(conn):
                await initialize_schema(conn)

    async def cleanup(self) -> None:
        """Clean up database connections."""
        await self.pool.cleanup()

    async def create_entities(self, entities: List[Dict[str, Any]], batch_size: int = 1000) -> List[Dict[str, Any]]:
        """Create multiple new entities in the database using batch processing."""
        created_entities = []
        
        async with self.pool.get_connection() as conn:
            async with self.pool.transaction(conn):
                for i in range(0, len(entities), batch_size):
                    batch = entities[i:i + batch_size]
                    entity_objects = [Entity.from_dict(e) for e in batch]
                    
                    # Validate entities before insertion
                    for entity in entity_objects:
                        cursor = await conn.execute(
                            "SELECT 1 FROM entities WHERE name = ?",
                            (_sanitize_input(entity.name),)
                        )
                        if await cursor.fetchone():
                            raise EntityAlreadyExistsError(entity.name)
                    
                    # Insert batch
                    await conn.executemany(
                        "INSERT INTO entities (name, entity_type, observations) VALUES (?, ?, ?)",
                        [(e.name, e.entityType, ','.join(e.observations)) for e in entity_objects]
                    )
                    created_entities.extend([e.to_dict() for e in entity_objects])
                    
        return created_entities

    async def create_relations(self, relations: List[Dict[str, Any]], batch_size: int = 1000) -> List[Dict[str, Any]]:
        """Create multiple new relations in the database using batch processing."""
        created_relations = []
        
        async with self.pool.get_connection() as conn:
            async with self.pool.transaction(conn):
                for i in range(0, len(relations), batch_size):
                    batch = relations[i:i + batch_size]
                    relation_objects = [Relation.from_dict(r) for r in batch]
                    
                    # Verify all entities exist before batch insertion
                    for relation in relation_objects:
                        cursor = await conn.execute(
                            "SELECT 1 FROM entities WHERE name = ?",
                            (_sanitize_input(relation.from_),)
                        )
                        if not await cursor.fetchone():
                            raise EntityNotFoundError(relation.from_)
                        
                        cursor = await conn.execute(
                            "SELECT 1 FROM entities WHERE name = ?",
                            (_sanitize_input(relation.to),)
                        )
                        if not await cursor.fetchone():
                            raise EntityNotFoundError(relation.to)
                    
                    # Insert batch
                    await conn.executemany(
                        """
                        INSERT INTO relations (from_entity, to_entity, relation_type) 
                        VALUES (?, ?, ?)
                        ON CONFLICT DO NOTHING
                        """,
                        [(r.from_, r.to, r.relationType) for r in relation_objects]
                    )
                    created_relations.extend([r.to_dict() for r in relation_objects])
                    
        return created_relations

    async def read_graph(self) -> Dict[str, List[Dict[str, Any]]]:
        """Read the entire graph and return serializable format."""
        async with self.pool.get_connection() as conn:
            # Get all entities
            cursor = await conn.execute("SELECT * FROM entities")
            rows = await cursor.fetchall()
            entities = []
            for row in rows:
                entity = Entity(
                    name=row['name'],
                    entityType=row['entity_type'],
                    observations=row['observations'].split(',') if row['observations'] else []
                )
                entities.append(entity.to_dict())

            # Get all relations
            cursor = await conn.execute("SELECT * FROM relations")
            rows = await cursor.fetchall()
            relations = []
            for row in rows:
                relation = Relation(
                    from_=row['from_entity'],
                    to=row['to_entity'],
                    relationType=row['relation_type']
                )
                relations.append(relation.to_dict())

            return {"entities": entities, "relations": relations}

    async def search_nodes(self, query: str) -> Dict[str, List[Dict[str, Any]]]:
        """Search for nodes and return serializable format."""
        if not query:
            raise ValueError("Search query cannot be empty")

        async with self.pool.get_connection() as conn:
            search_pattern = f"%{_sanitize_input(query)}%"

            # Search entities
            cursor = await conn.execute(
                """
                SELECT * FROM entities 
                WHERE name LIKE ? 
                OR entity_type LIKE ? 
                OR observations LIKE ?
                """,
                (search_pattern, search_pattern, search_pattern)
            )
            rows = await cursor.fetchall()
            
            entities = []
            entity_names = set()
            for row in rows:
                entity = Entity(
                    name=row['name'],
                    entityType=row['entity_type'],
                    observations=row['observations'].split(',') if row['observations'] else []
                )
                entities.append(entity.to_dict())
                entity_names.add(entity.name)

            if entity_names:
                # Get related relations
                placeholders = ','.join('?' * len(entity_names))
                cursor = await conn.execute(
                    f"""
                    SELECT * FROM relations 
                    WHERE from_entity IN ({placeholders})
                    AND to_entity IN ({placeholders})
                    """,
                    list(entity_names) * 2
                )
                rows = await cursor.fetchall()
                relations = []
                for row in rows:
                    relation = Relation(
                        from_=row['from_entity'],
                        to=row['to_entity'],
                        relationType=row['relation_type']
                    )
                    relations.append(relation.to_dict())
            else:
                relations = []

            return {"entities": entities, "relations": relations}

    async def add_observations(self, observations: List[Dict[str, Any]], batch_size: int = 1000) -> Dict[str, List[str]]:
        """Add new observations to existing entities using batch processing."""
        added_observations = {}
        
        async with self.pool.get_connection() as conn:
            async with self.pool.transaction(conn):
                for i in range(0, len(observations), batch_size):
                    batch = observations[i:i + batch_size]
                    
                    for obs in batch:
                        entity_name = _sanitize_input(obs["entityName"])
                        new_contents = obs["contents"]

                        cursor = await conn.execute(
                            "SELECT observations FROM entities WHERE name = ?",
                            (entity_name,)
                        )
                        result = await cursor.fetchone()
                        if not result:
                            raise EntityNotFoundError(entity_name)

                        current_obs = result['observations'].split(',') if result['observations'] else []
                        current_obs.extend(new_contents)
                        
                        await conn.execute(
                            "UPDATE entities SET observations = ? WHERE name = ?",
                            (','.join(current_obs), entity_name)
                        )
                        added_observations[entity_name] = new_contents

        return added_observations

    async def delete_entities(self, entityNames: List[str], batch_size: int = 1000) -> None:
        """Remove entities and their relations using batch processing."""
        async with self.pool.get_connection() as conn:
            async with self.pool.transaction(conn):
                for i in range(0, len(entityNames), batch_size):
                    batch = entityNames[i:i + batch_size]
                    sanitized_names = [_sanitize_input(name) for name in batch]
                    
                    placeholders = ','.join(['?' for _ in sanitized_names])
                    await conn.execute(
                        f"""
                        DELETE FROM relations 
                        WHERE from_entity IN ({placeholders})
                        OR to_entity IN ({placeholders})
                        """,
                        sanitized_names * 2
                    )
                    
                    await conn.execute(
                        f"DELETE FROM entities WHERE name IN ({placeholders})",
                        sanitized_names
                    )

    async def delete_observations(self, deletions: List[Dict[str, Any]], batch_size: int = 1000) -> None:
        """Remove specific observations from entities using batch processing."""
        async with self.pool.get_connection() as conn:
            async with self.pool.transaction(conn):
                for i in range(0, len(deletions), batch_size):
                    batch = deletions[i:i + batch_size]
                    
                    for deletion in batch:
                        entity_name = _sanitize_input(deletion["entityName"])
                        to_delete = set(deletion["observations"])

                        cursor = await conn.execute(
                            "SELECT observations FROM entities WHERE name = ?",
                            (entity_name,)
                        )
                        result = await cursor.fetchone()
                        if result:
                            current_obs = result['observations'].split(',') if result['observations'] else []
                            updated_obs = [obs for obs in current_obs if obs not in to_delete]
                            
                            await conn.execute(
                                "UPDATE entities SET observations = ? WHERE name = ?",
                                (','.join(updated_obs), entity_name)
                            )

    async def delete_relations(self, relations: List[Dict[str, Any]], batch_size: int = 1000) -> None:
        """Remove specific relations from the graph using batch processing."""
        async with self.pool.get_connection() as conn:
            async with self.pool.transaction(conn):
                for i in range(0, len(relations), batch_size):
                    batch = relations[i:i + batch_size]
                    relation_objects = [Relation.from_dict(r) for r in batch]
                    
                    await conn.executemany(
                        """
                        DELETE FROM relations 
                        WHERE from_entity = ? 
                        AND to_entity = ? 
                        AND relation_type = ?
                        """,
                        [(r.from_, r.to, r.relationType) for r in relation_objects]
                    )

    async def open_nodes(self, names: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """Retrieve specific nodes by name and their relations."""
        async with self.pool.get_connection() as conn:
            sanitized_names = [_sanitize_input(name) for name in names]
            placeholders = ','.join('?' * len(sanitized_names))
            
            cursor = await conn.execute(
                f"SELECT * FROM entities WHERE name IN ({placeholders})",
                sanitized_names
            )
            rows = await cursor.fetchall()
            entities = []
            for row in rows:
                entity = Entity(
                    name=row['name'],
                    entityType=row['entity_type'],
                    observations=row['observations'].split(',') if row['observations'] else []
                )
                entities.append(entity.to_dict())

            cursor = await conn.execute(
                f"""
                SELECT * FROM relations 
                WHERE from_entity IN ({placeholders})
                AND to_entity IN ({placeholders})
                """,
                sanitized_names * 2
            )
            rows = await cursor.fetchall()
            relations = []
            for row in rows:
                relation = Relation(
                    from_=row['from_entity'],
                    to=row['to_entity'],
                    relationType=row['relation_type']
                )
                relations.append(relation.to_dict())

            return {"entities": entities, "relations": relations}
