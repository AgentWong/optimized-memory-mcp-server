"""Ansible playbook tracking operations."""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import json

from ..connection import SQLiteConnectionPool
from ....exceptions import EntityNotFoundError

logger = logging.getLogger(__name__)

class AnsibleOperations:
    """Handles Ansible operations in SQLite."""
    
    def __init__(self, pool: SQLiteConnectionPool):
        self.pool = pool

    async def register_playbook(
        self,
        playbook_path: str,
        inventory_path: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Register an Ansible playbook for tracking."""
        async with self.pool.get_connection() as conn:
            cursor = await conn.execute("""
                INSERT INTO ansible_playbooks (playbook_path, inventory_path, metadata)
                VALUES (?, ?, ?)
                ON CONFLICT (playbook_path, inventory_path) DO UPDATE SET
                last_updated = CURRENT_TIMESTAMP,
                metadata = ?
                RETURNING *
            """, (
                playbook_path,
                inventory_path,
                json.dumps(metadata or {}),
                json.dumps(metadata or {})
            ))
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def start_playbook_run(
        self,
        playbook_id: int,
        host_count: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Start tracking a playbook run."""
        async with self.pool.get_connection() as conn:
            cursor = await conn.execute("""
                INSERT INTO ansible_runs (
                    playbook_id, start_time, status, host_count, metadata
                ) VALUES (?, CURRENT_TIMESTAMP, 'running', ?, ?)
                RETURNING *
            """, (playbook_id, host_count, json.dumps(metadata or {})))
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def record_task_execution(
        self,
        run_id: int,
        task_name: str,
        host: str,
        status: str,
        result: Dict[str, Any],
        cloud_resource_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Record execution of an Ansible task."""
        async with self.pool.get_connection() as conn:
            cursor = await conn.execute("""
                INSERT INTO ansible_tasks (
                    run_id, task_name, host, status,
                    start_time, end_time, result, cloud_resource_id
                ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?)
                RETURNING *
            """, (
                run_id,
                task_name,
                host,
                status,
                json.dumps(result),
                cloud_resource_id
            ))
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def complete_playbook_run(
        self,
        run_id: int,
        status: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Mark a playbook run as completed."""
        async with self.pool.get_connection() as conn:
            cursor = await conn.execute("""
                UPDATE ansible_runs
                SET status = ?,
                    end_time = CURRENT_TIMESTAMP,
                    metadata = CASE 
                        WHEN metadata IS NULL THEN ?
                        ELSE json_patch(metadata, ?)
                    END
                WHERE id = ?
                RETURNING *
            """, (status, json.dumps(metadata or {}), json.dumps(metadata or {}), run_id))
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_playbook_history(
        self,
        playbook_id: int,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get execution history for a playbook."""
        async with self.pool.get_connection() as conn:
            cursor = await conn.execute("""
                SELECT r.*, COUNT(t.id) as task_count,
                       SUM(CASE WHEN t.status = 'changed' THEN 1 ELSE 0 END) as changes_count
                FROM ansible_runs r
                LEFT JOIN ansible_tasks t ON t.run_id = r.id
                WHERE r.playbook_id = ?
                GROUP BY r.id
                ORDER BY r.start_time DESC
                LIMIT ?
            """, (playbook_id, limit))
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_task_details(
        self,
        run_id: int,
        host: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get detailed task execution information."""
        async with self.pool.get_connection() as conn:
            query = """
                SELECT t.*, c.resource_type as cloud_resource_type
                FROM ansible_tasks t
                LEFT JOIN cloud_resources c ON t.cloud_resource_id = c.resource_id
                WHERE t.run_id = ?
            """
            params = [run_id]
            
            if host:
                query += " AND t.host = ?"
                params.append(host)
                
            cursor = await conn.execute(query + " ORDER BY t.start_time", params)
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
