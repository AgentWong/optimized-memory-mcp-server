"""Optimized SQLite storage backend implementation."""
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse
import os
from pathlib import Path
import logging

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

class OptimizedSQLiteManager(StorageBackend):
    """Optimized SQLite implementation of the storage backend."""
    
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
        return await self.entity_ops.create_entities(entities, batch_size)

    async def create_relations(self, relations: List[Dict[str, Any]], batch_size: int = 1000) -> List[Dict[str, Any]]:
        """Create multiple new relations in the database using batch processing."""
        return await self.relation_ops.create_relations(relations, batch_size)

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
        return await self.search_ops.search_nodes(query)

    async def add_observations(self, observations: List[Dict[str, Any]], batch_size: int = 1000) -> Dict[str, List[str]]:
        """Add new observations to existing entities using batch processing."""
        return await self.entity_ops.add_observations(observations, batch_size)

    async def delete_entities(self, entityNames: List[str], batch_size: int = 1000) -> None:
        """Remove entities and their relations using batch processing."""
        await self.entity_ops.delete_entities(entityNames, batch_size)

    async def delete_observations(self, deletions: List[Dict[str, Any]], batch_size: int = 1000) -> None:
        """Remove specific observations from entities using batch processing."""
        await self.entity_ops.delete_observations(deletions, batch_size)

    async def delete_relations(self, relations: List[Dict[str, Any]], batch_size: int = 1000) -> None:
        """Remove specific relations from the graph using batch processing."""
        await self.relation_ops.delete_relations(relations, batch_size)

    async def open_nodes(self, names: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """Retrieve specific nodes by name and their relations."""
        return await self.search_ops.open_nodes(names)
