from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime

from ..interfaces import Entity, Relation

class StorageBackend(ABC):
    """Abstract base class for storage backends."""
    
    @abstractmethod
    async def initialize(self) -> None:
        """Initialize storage backend."""
        pass
    
    @abstractmethod
    async def cleanup(self) -> None:
        """Clean up resources."""
        pass
    
    @abstractmethod
    async def create_entities(self, entities: List[Dict[str, Any]], batch_size: int = 1000) -> List[Dict[str, Any]]:
        """Create multiple entities."""
        pass
    
    @abstractmethod
    async def create_relations(self, relations: List[Dict[str, Any]], batch_size: int = 1000) -> List[Dict[str, Any]]:
        """Create multiple relations."""
        pass
    
    @abstractmethod
    async def read_graph(self) -> Dict[str, List[Dict[str, Any]]]:
        """Read entire graph."""
        pass
    
    @abstractmethod
    async def search_nodes(self, query: str) -> Dict[str, List[Dict[str, Any]]]:
        """Search nodes by query."""
        pass

    @abstractmethod
    async def get_entity_at_time(
        self,
        entity_name: str,
        point_in_time: datetime
    ) -> Optional[Dict[str, Any]]:
        """Get entity state at a specific point in time."""
        pass

    @abstractmethod
    async def get_entity_changes(
        self,
        entity_name: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Get history of entity changes within a time range."""
        pass

    @abstractmethod
    async def get_relations_at_time(
        self,
        entity_name: str,
        point_in_time: datetime,
        direction: str = 'both'
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Get entity's relations at a specific point in time."""
        pass

    @abstractmethod
    async def track_entity_change(
        self,
        entity_name: str,
        change_type: str,
        entity_state: Dict[str, Any],
        changed_by: Optional[str] = None
    ) -> Dict[str, Any]:
        """Record an entity change in the change history."""
        pass

    @abstractmethod
    async def get_changes_in_period(
        self,
        start_time: datetime,
        end_time: datetime,
        entity_type: Optional[str] = None,
        change_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get all changes within a time period with optional filtering."""
        pass
