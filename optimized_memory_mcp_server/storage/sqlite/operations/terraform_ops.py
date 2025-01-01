"""Terraform state tracking operations."""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import json

from ..connection import SQLiteConnectionPool
from ....exceptions import EntityNotFoundError

logger = logging.getLogger(__name__)

class TerraformOperations:
    """Handles Terraform state operations in SQLite."""
    
    def __init__(self, pool: SQLiteConnectionPool):
        self.pool = pool

    async def sync_terraform_state(
        self,
        workspace: str,
        state_file: str,
        state_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Sync Terraform state file and its resources.
        
        Args:
            workspace: Terraform workspace name
            state_file: Path or identifier of state file
            state_data: Parsed Terraform state JSON
            
        Returns:
            List of tracked resources
        """
        async with self.pool.get_connection() as conn:
            async with self.pool.transaction(conn):
                # Update state file tracking
                await conn.execute("""
                    INSERT INTO terraform_states (workspace, state_file, metadata)
                    VALUES (?, ?, ?)
                    ON CONFLICT (workspace, state_file) DO UPDATE SET
                    last_synced = CURRENT_TIMESTAMP,
                    metadata = ?
                """, (workspace, state_file, json.dumps(state_data.get('metadata', {})),
                      json.dumps(state_data.get('metadata', {}))))

                # Process resources
                resources = []
                for resource in state_data.get('resources', []):
                    resource_id = f"{resource['type']}.{resource['name']}"
                    
                    # Try to find matching cloud resource
                    cloud_resource_id = None
                    if 'instances' in resource:
                        for instance in resource['instances']:
                            if 'attributes' in instance and 'id' in instance['attributes']:
                                cloud_resource_id = instance['attributes']['id']
                                break

                    # Insert or update resource
                    cursor = await conn.execute("""
                        INSERT INTO terraform_resources (
                            resource_id, resource_type, workspace, state_file,
                            state, cloud_resource_id
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT (resource_id, workspace, state_file) DO UPDATE SET
                        state = ?,
                        cloud_resource_id = ?,
                        last_synced = CURRENT_TIMESTAMP
                        RETURNING *
                    """, (
                        resource_id,
                        resource['type'],
                        workspace,
                        state_file,
                        json.dumps(resource),
                        cloud_resource_id,
                        json.dumps(resource),
                        cloud_resource_id
                    ))
                    
                    row = await cursor.fetchone()
                    if row:
                        resources.append(dict(row))

                return resources

    async def get_terraform_resources(
        self,
        workspace: Optional[str] = None,
        resource_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get tracked Terraform resources with optional filtering."""
        async with self.pool.get_connection() as conn:
            query = "SELECT * FROM terraform_resources WHERE 1=1"
            params = []
            
            if workspace:
                query += " AND workspace = ?"
                params.append(workspace)
            
            if resource_type:
                query += " AND resource_type = ?"
                params.append(resource_type)
                
            cursor = await conn.execute(query, params)
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_resource_state_history(
        self,
        resource_id: str,
        workspace: str,
        state_file: str
    ) -> Dict[str, Any]:
        """Get detailed state history for a Terraform resource."""
        async with self.pool.get_connection() as conn:
            cursor = await conn.execute("""
                SELECT r.*, c.metadata as cloud_metadata
                FROM terraform_resources r
                LEFT JOIN cloud_resources c ON r.cloud_resource_id = c.resource_id
                WHERE r.resource_id = ? AND r.workspace = ? AND r.state_file = ?
            """, (resource_id, workspace, state_file))
            
            row = await cursor.fetchone()
            if not row:
                raise EntityNotFoundError(f"Terraform resource {resource_id} not found")
                
            return dict(row)
