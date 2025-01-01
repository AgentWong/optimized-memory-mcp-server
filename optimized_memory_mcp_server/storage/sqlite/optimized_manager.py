"""SQLite storage backend implementation."""
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
from .operations.cloud_ops import CloudResourceOperations
from .operations.terraform_ops import TerraformOperations
from .operations.ansible_ops import AnsibleOperations
from .operations.snippet_ops import SnippetOperations
from ...interfaces import CloudResource, CloudResourceType
from ...interfaces import Entity, Relation
from ...exceptions import EntityNotFoundError, EntityAlreadyExistsError
from .utils.sanitization import sanitize_input as _sanitize_input

logger = logging.getLogger(__name__)

class SQLiteManager(StorageBackend):
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
        self.cloud_ops = CloudResourceOperations(self.pool)
        self.terraform_ops = TerraformOperations(self.pool)
        self.ansible_ops = AnsibleOperations(self.pool)
        self.snippet_ops = SnippetOperations(self.pool)

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

    async def create_cloud_resource(
        self,
        resource: CloudResource,
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Create a new cloud resource record."""
        return await self.cloud_ops.create_resource(resource)

    async def get_cloud_resources(
        self,
        resource_type: CloudResourceType,
        region: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Retrieve cloud resources by type and optionally region."""
        return await self.cloud_ops.get_resources_by_type(resource_type, region)

    async def update_cloud_resource(
        self,
        resource_id: str,
        new_metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update cloud resource metadata and state."""
        return await self.cloud_ops.update_resource_state(resource_id, new_metadata)

    async def delete_cloud_resource(self, resource_id: str) -> None:
        """Remove a cloud resource record."""
        await self.cloud_ops.delete_resource(resource_id)

    async def sync_terraform_state(
        self,
        workspace: str,
        state_file: str, 
        state_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Sync Terraform state file and its resources."""
        return await self.terraform_ops.sync_terraform_state(workspace, state_file, state_data)

    async def get_terraform_resources(
        self,
        workspace: Optional[str] = None,
        resource_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get tracked Terraform resources with optional filtering."""
        return await self.terraform_ops.get_terraform_resources(workspace, resource_type)

    async def get_resource_state_history(
        self,
        resource_id: str,
        workspace: str,
        state_file: str
    ) -> Dict[str, Any]:
        """Get detailed state history for a Terraform resource."""
        return await self.terraform_ops.get_resource_state_history(
            resource_id, workspace, state_file
        )

    async def register_ansible_playbook(
        self,
        playbook_path: str,
        inventory_path: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Register an Ansible playbook for tracking."""
        return await self.ansible_ops.register_playbook(playbook_path, inventory_path, metadata)

    async def start_ansible_run(
        self,
        playbook_id: int,
        host_count: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Start tracking a playbook run."""
        return await self.ansible_ops.start_playbook_run(playbook_id, host_count, metadata)

    async def record_ansible_task(
        self,
        run_id: int,
        task_name: str,
        host: str,
        status: str,
        result: Dict[str, Any],
        cloud_resource_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Record execution of an Ansible task."""
        return await self.ansible_ops.record_task_execution(
            run_id, task_name, host, status, result, cloud_resource_id
        )

    async def complete_ansible_run(
        self,
        run_id: int,
        status: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Mark a playbook run as completed."""
        return await self.ansible_ops.complete_playbook_run(run_id, status, metadata)

    async def get_ansible_history(
        self,
        playbook_id: int,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get execution history for a playbook."""
        return await self.ansible_ops.get_playbook_history(playbook_id, limit)

    async def get_ansible_task_details(
        self,
        run_id: int,
        host: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get detailed task execution information."""
        return await self.ansible_ops.get_task_details(run_id, host)
