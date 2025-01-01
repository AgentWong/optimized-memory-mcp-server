"""AWS cloud resource operations."""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import json

from ....interfaces import CloudResource, CloudResourceType
from ..connection import SQLiteConnectionPool
from ...base import StorageBackend
from ..utils.sanitization import sanitize_input as _sanitize_input

logger = logging.getLogger(__name__)

class CloudResourceOperations:
    """Handles cloud resource operations in SQLite."""
    
    def __init__(self, pool: SQLiteConnectionPool):
        self.pool = pool
    
    async def create_resource(self, resource: CloudResource) -> Dict[str, Any]:
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
    
    async def get_resources_by_type(
        self,
        resource_type: CloudResourceType,
        region: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Retrieve cloud resources by type and optionally region."""
        async with self.pool.get_connection() as conn:
            if region:
                cursor = await conn.execute(
                    """
                    SELECT * FROM cloud_resources 
                    WHERE resource_type = ? AND region = ?
                    """,
                    (resource_type.value, region)
                )
            else:
                cursor = await conn.execute(
                    "SELECT * FROM cloud_resources WHERE resource_type = ?",
                    (resource_type.value,)
                )
            
            rows = await cursor.fetchall()
            return [CloudResource.from_dict(dict(row)).to_dict() for row in rows]
    
    async def update_resource_state(
        self,
        resource_id: str,
        new_metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update cloud resource metadata and state."""
        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(
                """
                UPDATE cloud_resources 
                SET metadata = ?, last_updated = ?
                WHERE resource_id = ?
                RETURNING *
                """,
                (
                    json.dumps(new_metadata),
                    datetime.utcnow().isoformat(),
                    resource_id
                )
            )
            row = await cursor.fetchone()
            if row:
                return CloudResource.from_dict(dict(row)).to_dict()
            return None
    
    async def delete_resource(self, resource_id: str) -> None:
        """Remove a cloud resource record."""
        async with self.pool.get_connection() as conn:
            await conn.execute(
                "DELETE FROM cloud_resources WHERE resource_id = ?",
                (resource_id,)
            )
