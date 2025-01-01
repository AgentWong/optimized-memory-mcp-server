"""Entity operations for SQLite storage backend."""
from typing import List, Dict, Any
import aiosqlite
from ..utils.sanitization import sanitize_input
from ....core.validation import validate_entity
from ....interfaces import Entity
from ....exceptions import EntityNotFoundError, EntityAlreadyExistsError

class EntityOperations:
    """Handles entity-related database operations."""
    
    def __init__(self, pool):
        self.pool = pool
        
    async def create_entities(
        self, 
        entities: List[Dict[str, Any]], 
        batch_size: int = 1000
    ) -> List[Dict[str, Any]]:
        """Create multiple new entities in batches."""
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
                            (sanitize_input(entity.name),)
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

    async def delete_entities(
        self, 
        entityNames: List[str], 
        batch_size: int = 1000
    ) -> None:
        """Remove entities and their relations using batch processing."""
        async with self.pool.get_connection() as conn:
            async with self.pool.transaction(conn):
                for i in range(0, len(entityNames), batch_size):
                    batch = entityNames[i:i + batch_size]
                    sanitized_names = [sanitize_input(name) for name in batch]
                    
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

    async def add_observations(
        self, 
        observations: List[Dict[str, Any]], 
        batch_size: int = 1000
    ) -> Dict[str, List[str]]:
        """Add new observations to existing entities using batch processing."""
        added_observations = {}
        
        async with self.pool.get_connection() as conn:
            async with self.pool.transaction(conn):
                for i in range(0, len(observations), batch_size):
                    batch = observations[i:i + batch_size]
                    
                    for obs in batch:
                        entity_name = sanitize_input(obs["entityName"])
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

    async def delete_observations(
        self, 
        deletions: List[Dict[str, Any]], 
        batch_size: int = 1000
    ) -> None:
        """Remove specific observations from entities using batch processing."""
        async with self.pool.get_connection() as conn:
            async with self.pool.transaction(conn):
                for i in range(0, len(deletions), batch_size):
                    batch = deletions[i:i + batch_size]
                    
                    for deletion in batch:
                        entity_name = sanitize_input(deletion["entityName"])
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
