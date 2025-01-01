"""SQLite database operations with optimized queries."""
import logging
from typing import List, Dict, Any, Optional, Set
from datetime import datetime
import json

from ...interfaces import Entity, Relation, CloudResource, CloudResourceType
from .connection import SQLiteConnectionPool
from ..base import StorageBackend
from .utils.sanitization import sanitize_input
from ...exceptions import EntityNotFoundError, EntityAlreadyExistsError

logger = logging.getLogger(__name__)

class DatabaseOperations:
    """Handles all SQLite database operations with optimizations."""
    
    def __init__(self, pool: SQLiteConnectionPool):
        self.pool = pool

    async def get_entity_statistics(self) -> Dict[str, Any]:
        """Get entity statistics from materialized view."""
        async with self.pool.get_connection() as conn:
            cursor = await conn.execute("""
                SELECT * FROM mv_entity_stats 
                ORDER BY count DESC
            """)
            rows = await cursor.fetchall()
            return {row['entity_type']: dict(row) for row in rows}

    async def get_relation_summary(self) -> Dict[str, Any]:
        """Get relation summary from materialized view."""
        async with self.pool.get_connection() as conn:
            cursor = await conn.execute("""
                SELECT * FROM mv_relation_summary 
                ORDER BY count DESC
            """)
            rows = await cursor.fetchall()
            return {row['relation_type']: dict(row) for row in rows}
        
    # Entity Operations
    async def create_entities(
        self,
        entities: List[Dict[str, Any]],
        batch_size: int = 1000
    ) -> List[Dict[str, Any]]:
        """Create entities using partitioned tables."""
        created_entities = []
        
        async with self.pool.get_connection() as conn:
            async with self.pool.transaction(conn):
                stmt = await self.pool.prepare_cached(conn, """
                    INSERT INTO entities_recent (
                        name, entity_type, observations, created_at,
                        last_updated, confidence_score, context_source, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """)
                
                for i in range(0, len(entities), batch_size):
                    batch = entities[i:i + batch_size]
                    entity_objects = [Entity.from_dict(e) for e in batch]
                    
                    # Validate and insert batch
                    for entity in entity_objects:
                        # Check existence across all partitions
                        cursor = await conn.execute("""
                            SELECT 1 FROM entities WHERE name = ?
                        """, (sanitize_input(entity.name),))
                        
                        if await cursor.fetchone():
                            raise EntityAlreadyExistsError(entity.name)
                        
                        # Insert into recent partition
                        await stmt.execute((
                            entity.name,
                            entity.entityType,
                            ','.join(entity.observations),
                            entity.created_at.isoformat(),
                            entity.last_updated.isoformat(),
                            entity.confidence_score,
                            entity.context_source,
                            json.dumps(entity.metadata or {})
                        ))
                        
                        created_entities.append(entity.to_dict())
                
        return created_entities

    # Relation Operations
    async def create_relations(self, relations: List[Dict[str, Any]], batch_size: int = 1000) -> List[Dict[str, Any]]:
        """Create multiple new relations in batches."""
        created_relations = []
        
        async with self.pool.get_connection() as conn:
            async with self.pool.transaction(conn):
                for i in range(0, len(relations), batch_size):
                    batch = relations[i:i + batch_size]
                    relation_objects = [Relation.from_dict(r) for r in batch]
                    
                    for relation in relation_objects:
                        for entity_name in (relation.from_, relation.to):
                            cursor = await conn.execute(
                                "SELECT 1 FROM entities WHERE name = ?",
                                (sanitize_input(entity_name),)
                            )
                            if not await cursor.fetchone():
                                raise EntityNotFoundError(entity_name)
                    
                    await conn.executemany(
                        """
                        INSERT INTO relations (
                            from_entity, to_entity, relation_type, created_at,
                            valid_from, valid_until, confidence_score, context_source
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT DO NOTHING
                        """,
                        [(
                            r.from_, r.to, r.relationType, r.created_at.isoformat(),
                            r.valid_from.isoformat() if r.valid_from else None,
                            r.valid_until.isoformat() if r.valid_until else None,
                            r.confidence_score, r.context_source
                        ) for r in relation_objects]
                    )
                    created_relations.extend([r.to_dict() for r in relation_objects])
                    
        return created_relations

    # Cloud Resource Operations
    async def create_cloud_resource(self, resource: CloudResource) -> Dict[str, Any]:
        """Create a new cloud resource record."""
        async with self.pool.get_connection() as conn:
            await conn.execute(
                """
                INSERT INTO cloud_resources (
                    resource_id, resource_type, region, account_id,
                    metadata, entity_name, created_at, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    resource.resource_id,
                    resource.resource_type.value,
                    resource.region,
                    resource.account_id,
                    json.dumps(resource.metadata),
                    resource.entity_name,
                    resource.created_at.isoformat(),
                    resource.last_updated.isoformat()
                )
            )
            return resource.to_dict()

    # Search Operations
    async def search_nodes(self, query: str) -> Dict[str, List[Dict[str, Any]]]:
        """Optimized search using partitioned tables."""
        if not query:
            raise ValueError("Search query cannot be empty")

        # Check cache
        cache_key = f"search:{query}"
        cached_result = self.pool.get_cached_query(cache_key)
        if cached_result is not None:
            return cached_result

        async with self.pool.get_connection() as conn:
            search_pattern = f"%{sanitize_input(query)}%"
            
            # Search recent partition first
            stmt = await self.pool.prepare_cached(conn, """
                SELECT * FROM entities_recent 
                WHERE name LIKE ? 
                OR entity_type LIKE ? 
                OR observations LIKE ?
                UNION ALL
                SELECT * FROM entities_intermediate 
                WHERE name LIKE ? 
                OR entity_type LIKE ? 
                OR observations LIKE ?
                UNION ALL
                SELECT * FROM entities_archive 
                WHERE name LIKE ? 
                OR entity_type LIKE ? 
                OR observations LIKE ?
                ORDER BY created_at DESC
            """)
            
            cursor = await stmt.execute((
                search_pattern, search_pattern, search_pattern,
                search_pattern, search_pattern, search_pattern,
                search_pattern, search_pattern, search_pattern
            ))
            
            rows = await cursor.fetchall()
            entities = []
            entity_names = set()
            
            for row in rows:
                entity = Entity(
                    name=row['name'],
                    entityType=row['entity_type'],
                    observations=row['observations'].split(',') if row['observations'] else [],
                    created_at=datetime.fromisoformat(row['created_at']),
                    last_updated=datetime.fromisoformat(row['last_updated']),
                    confidence_score=row['confidence_score'],
                    context_source=row['context_source'],
                    metadata=json.loads(row['metadata']) if row['metadata'] else None
                )
                entities.append(entity.to_dict())
                entity_names.add(entity.name)

            # Get relations
            relations = await self._get_relations_for_entities(conn, entity_names)
            result = {"entities": entities, "relations": relations}
            
            # Cache results
            self.pool.cache_query(cache_key, result)
            return result

    async def _get_relations_for_entities(self, conn, entity_names: Set[str]) -> List[Dict[str, Any]]:
        """Helper to get relations for a set of entities."""
        if not entity_names:
            return []
            
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
        return [Relation.from_dict(dict(row)).to_dict() for row in rows]
    async def get_entity_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Optimized entity lookup using partitioned tables."""
        async with self.pool.get_connection() as conn:
            stmt = await self.pool.prepare_cached(conn, """
                SELECT * FROM entities_recent WHERE name = ?
                UNION ALL
                SELECT * FROM entities_intermediate WHERE name = ?
                UNION ALL
                SELECT * FROM entities_archive WHERE name = ?
                LIMIT 1
            """)
            
            cursor = await stmt.execute((name, name, name))
            row = await cursor.fetchone()
            
            if row:
                return Entity(
                    name=row['name'],
                    entityType=row['entity_type'],
                    observations=row['observations'].split(',') if row['observations'] else [],
                    created_at=datetime.fromisoformat(row['created_at']),
                    last_updated=datetime.fromisoformat(row['last_updated']),
                    confidence_score=row['confidence_score'],
                    context_source=row['context_source'],
                    metadata=json.loads(row['metadata']) if row['metadata'] else None
                ).to_dict()
            
            return None
