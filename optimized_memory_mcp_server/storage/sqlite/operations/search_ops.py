"""Search operations for SQLite storage backend."""
from typing import List, Dict, Any, Set
from ....interfaces import Entity, Relation
from ..utils.sanitization import sanitize_input

class SearchOperations:
    """Handles search-related database operations."""
    
    def __init__(self, pool):
        self.pool = pool

    async def search_nodes(self, query: str) -> Dict[str, List[Dict[str, Any]]]:
        """Search for nodes and their relations."""
        if not query:
            raise ValueError("Search query cannot be empty")

        async with self.pool.get_connection() as conn:
            search_pattern = f"%{sanitize_input(query)}%"
            
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

            relations = await self._get_relations_for_entities(conn, entity_names)
            return {"entities": entities, "relations": relations}

    async def open_nodes(self, names: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """Retrieve specific nodes and their relations."""
        async with self.pool.get_connection() as conn:
            sanitized_names = [sanitize_input(name) for name in names]
            placeholders = ','.join('?' * len(sanitized_names))
            
            # Get entities
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

            relations = await self._get_relations_for_entities(conn, set(names))
            return {"entities": entities, "relations": relations}

    async def _get_relations_for_entities(
        self, 
        conn, 
        entity_names: Set[str]
    ) -> List[Dict[str, Any]]:
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
        relations = []
        for row in rows:
            relation = Relation(
                from_=row['from_entity'],
                to=row['to_entity'],
                relationType=row['relation_type']
            )
            relations.append(relation.to_dict())
        return relations
