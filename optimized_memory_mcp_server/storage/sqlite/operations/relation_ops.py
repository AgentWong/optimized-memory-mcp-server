"""Relation operations for SQLite storage backend."""
from typing import List, Dict, Any
from ....interfaces import Relation
from ....exceptions import EntityNotFoundError
from ..utils.sanitization import sanitize_input

class RelationOperations:
    """Handles relation-related database operations."""
    
    def __init__(self, pool):
        self.pool = pool
        
    async def create_relations(
        self, 
        relations: List[Dict[str, Any]], 
        batch_size: int = 1000
    ) -> List[Dict[str, Any]]:
        """Create multiple new relations in batches."""
        created_relations = []
        
        async with self.pool.get_connection() as conn:
            async with self.pool.transaction(conn):
                for i in range(0, len(relations), batch_size):
                    batch = relations[i:i + batch_size]
                    relation_objects = [Relation.from_dict(r) for r in batch]
                    
                    # Verify entities exist
                    for relation in relation_objects:
                        cursor = await conn.execute(
                            "SELECT 1 FROM entities WHERE name = ?",
                            (sanitize_input(relation.from_),)
                        )
                        if not await cursor.fetchone():
                            raise EntityNotFoundError(relation.from_)
                        
                        cursor = await conn.execute(
                            "SELECT 1 FROM entities WHERE name = ?",
                            (sanitize_input(relation.to),)
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

    async def delete_relations(
        self, 
        relations: List[Dict[str, Any]], 
        batch_size: int = 1000
    ) -> None:
        """Remove specific relations using batch processing."""
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
