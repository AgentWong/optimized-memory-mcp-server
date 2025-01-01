"""Temporal operations for SQLite storage backend."""
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
from ..utils.sanitization import sanitize_input

class TemporalOperations:
    """Handles temporal queries and change tracking."""
    
    def __init__(self, pool):
        self.pool = pool
        
    async def get_entity_at_time(
        self,
        entity_name: str,
        timestamp: datetime
    ) -> Optional[Dict[str, Any]]:
        """Get entity state at a specific point in time."""
        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(
                """
                SELECT * FROM entity_versions
                WHERE entity_name = ?
                AND valid_from <= ?
                AND (valid_until > ? OR valid_until IS NULL)
                ORDER BY version_number DESC
                LIMIT 1
                """,
                (sanitize_input(entity_name), timestamp.isoformat(), timestamp.isoformat())
            )
            row = await cursor.fetchone()
            return dict(row) if row else None
            
    async def get_entity_history(
        self,
        entity_name: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Get entity change history within a time range."""
        async with self.pool.get_connection() as conn:
            query = "SELECT * FROM entity_versions WHERE entity_name = ?"
            params = [sanitize_input(entity_name)]
            
            if start_time:
                query += " AND valid_from >= ?"
                params.append(start_time.isoformat())
            if end_time:
                query += " AND (valid_until <= ? OR valid_until IS NULL)"
                params.append(end_time.isoformat())
                
            query += " ORDER BY version_number ASC"
            
            cursor = await conn.execute(query, params)
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
            
    async def get_relation_at_time(
        self,
        from_entity: str,
        to_entity: str,
        relation_type: str,
        timestamp: datetime
    ) -> Optional[Dict[str, Any]]:
        """Get relation state at a specific point in time."""
        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(
                """
                SELECT * FROM relation_versions
                WHERE from_entity = ?
                AND to_entity = ?
                AND relation_type = ?
                AND valid_from <= ?
                AND (valid_until > ? OR valid_until IS NULL)
                ORDER BY version_number DESC
                LIMIT 1
                """,
                (
                    sanitize_input(from_entity),
                    sanitize_input(to_entity),
                    sanitize_input(relation_type),
                    timestamp.isoformat(),
                    timestamp.isoformat()
                )
            )
            row = await cursor.fetchone()
            return dict(row) if row else None
            
    async def get_relation_history(
        self,
        from_entity: str,
        to_entity: str,
        relation_type: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Get relation change history within a time range."""
        async with self.pool.get_connection() as conn:
            query = """
                SELECT * FROM relation_versions 
                WHERE from_entity = ?
                AND to_entity = ?
                AND relation_type = ?
            """
            params = [
                sanitize_input(from_entity),
                sanitize_input(to_entity),
                sanitize_input(relation_type)
            ]
            
            if start_time:
                query += " AND valid_from >= ?"
                params.append(start_time.isoformat())
            if end_time:
                query += " AND (valid_until <= ? OR valid_until IS NULL)"
                params.append(end_time.isoformat())
                
            query += " ORDER BY version_number ASC"
            
            cursor = await conn.execute(query, params)
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
            
    async def get_graph_at_time(
        self,
        timestamp: datetime
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Get complete graph state at a specific point in time."""
        async with self.pool.get_connection() as conn:
            # Get entities at timestamp
            cursor = await conn.execute(
                """
                SELECT DISTINCT ON (entity_name) *
                FROM entity_versions
                WHERE valid_from <= ?
                AND (valid_until > ? OR valid_until IS NULL)
                AND change_type != 'delete'
                ORDER BY entity_name, version_number DESC
                """,
                (timestamp.isoformat(), timestamp.isoformat())
            )
            entity_rows = await cursor.fetchall()
            
            # Get relations at timestamp
            cursor = await conn.execute(
                """
                SELECT DISTINCT ON (from_entity, to_entity, relation_type) *
                FROM relation_versions
                WHERE valid_from <= ?
                AND (valid_until > ? OR valid_until IS NULL)
                AND change_type != 'delete'
                ORDER BY from_entity, to_entity, relation_type, version_number DESC
                """,
                (timestamp.isoformat(), timestamp.isoformat())
            )
            relation_rows = await cursor.fetchall()
            
            return {
                "entities": [dict(row) for row in entity_rows],
                "relations": [dict(row) for row in relation_rows]
            }
            
    async def get_changes_in_range(
        self,
        start_time: datetime,
        end_time: datetime,
        entity_types: Optional[List[str]] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Get all changes within a time range."""
        async with self.pool.get_connection() as conn:
            # Get entity changes
            query = """
                SELECT * FROM entity_versions
                WHERE valid_from >= ?
                AND valid_from <= ?
            """
            params = [start_time.isoformat(), end_time.isoformat()]
            
            if entity_types:
                query += f" AND entity_type IN ({','.join('?' * len(entity_types))})"
                params.extend(entity_types)
                
            cursor = await conn.execute(query, params)
            entity_changes = await cursor.fetchall()
            
            # Get relation changes
            cursor = await conn.execute(
                """
                SELECT * FROM relation_versions
                WHERE valid_from >= ?
                AND valid_from <= ?
                ORDER BY valid_from ASC
                """,
                (start_time.isoformat(), end_time.isoformat())
            )
            relation_changes = await cursor.fetchall()
            
            return {
                "entity_changes": [dict(row) for row in entity_changes],
                "relation_changes": [dict(row) for row in relation_changes]
            }
"""Temporal operations for SQLite storage backend."""
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import json

from ..utils.sanitization import sanitize_input

class TemporalOperations:
    """Handles temporal queries and change tracking."""
    
    def __init__(self, pool):
        self.pool = pool

    async def get_entity_at_time(
        self,
        entity_name: str,
        point_in_time: datetime
    ) -> Optional[Dict[str, Any]]:
        """Get entity state at a specific point in time."""
        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(
                """
                SELECT * FROM entity_changes 
                WHERE entity_name = ? AND changed_at <= ?
                ORDER BY changed_at DESC
                LIMIT 1
                """,
                (sanitize_input(entity_name), point_in_time.isoformat())
            )
            row = await cursor.fetchone()
            if row:
                return json.loads(row['entity_state'])
            return None

    async def get_entity_changes(
        self,
        entity_name: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Get history of entity changes within a time range."""
        async with self.pool.get_connection() as conn:
            query = "SELECT * FROM entity_changes WHERE entity_name = ?"
            params = [sanitize_input(entity_name)]

            if start_time:
                query += " AND changed_at >= ?"
                params.append(start_time.isoformat())
            if end_time:
                query += " AND changed_at <= ?"
                params.append(end_time.isoformat())

            query += " ORDER BY changed_at DESC"
            
            cursor = await conn.execute(query, params)
            rows = await cursor.fetchall()
            return [
                {
                    'entity_name': row['entity_name'],
                    'changed_at': row['changed_at'],
                    'change_type': row['change_type'],
                    'entity_state': json.loads(row['entity_state']),
                    'changed_by': row['changed_by']
                }
                for row in rows
            ]

    async def get_relations_at_time(
        self,
        entity_name: str,
        point_in_time: datetime,
        direction: str = 'both'  # 'incoming', 'outgoing', or 'both'
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Get entity's relations at a specific point in time."""
        async with self.pool.get_connection() as conn:
            queries = []
            params = []
            
            if direction in ('outgoing', 'both'):
                queries.append("""
                    SELECT * FROM relations 
                    WHERE from_entity = ? 
                    AND valid_from <= ?
                    AND (valid_until IS NULL OR valid_until > ?)
                """)
                params.extend([
                    sanitize_input(entity_name),
                    point_in_time.isoformat(),
                    point_in_time.isoformat()
                ])
                
            if direction in ('incoming', 'both'):
                queries.append("""
                    SELECT * FROM relations 
                    WHERE to_entity = ?
                    AND valid_from <= ?
                    AND (valid_until IS NULL OR valid_until > ?)
                """)
                params.extend([
                    sanitize_input(entity_name),
                    point_in_time.isoformat(),
                    point_in_time.isoformat()
                ])

            all_relations = []
            for query in queries:
                cursor = await conn.execute(query, params[len(all_relations)*3:len(all_relations)*3+3])
                rows = await cursor.fetchall()
                all_relations.extend(dict(row) for row in rows)

            return {
                'outgoing': [r for r in all_relations if r['from_entity'] == entity_name],
                'incoming': [r for r in all_relations if r['to_entity'] == entity_name]
            }

    async def track_entity_change(
        self,
        entity_name: str,
        change_type: str,
        entity_state: Dict[str, Any],
        changed_by: Optional[str] = None
    ) -> Dict[str, Any]:
        """Record an entity change in the change history."""
        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(
                """
                INSERT INTO entity_changes (
                    entity_name, change_type, entity_state, changed_by
                ) VALUES (?, ?, ?, ?)
                RETURNING *
                """,
                (
                    sanitize_input(entity_name),
                    change_type,
                    json.dumps(entity_state),
                    changed_by
                )
            )
            row = await cursor.fetchone()
            return {
                'entity_name': row['entity_name'],
                'changed_at': row['changed_at'],
                'change_type': row['change_type'],
                'entity_state': json.loads(row['entity_state']),
                'changed_by': row['changed_by']
            }

    async def get_changes_in_period(
        self,
        start_time: datetime,
        end_time: datetime,
        entity_type: Optional[str] = None,
        change_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get all changes within a time period with optional filtering."""
        async with self.pool.get_connection() as conn:
            query = """
                SELECT c.*, e.entity_type 
                FROM entity_changes c
                JOIN entities e ON c.entity_name = e.name
                WHERE c.changed_at BETWEEN ? AND ?
            """
            params = [start_time.isoformat(), end_time.isoformat()]

            if entity_type:
                query += " AND e.entity_type = ?"
                params.append(entity_type)
            if change_type:
                query += " AND c.change_type = ?"
                params.append(change_type)

            query += " ORDER BY c.changed_at DESC"
            
            cursor = await conn.execute(query, params)
            rows = await cursor.fetchall()
            return [
                {
                    'entity_name': row['entity_name'],
                    'entity_type': row['entity_type'],
                    'changed_at': row['changed_at'],
                    'change_type': row['change_type'],
                    'entity_state': json.loads(row['entity_state']),
                    'changed_by': row['changed_by']
                }
                for row in rows
            ]
