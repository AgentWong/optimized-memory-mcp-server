from abc import ABC, abstractmethod
from typing import List, Dict, Any

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
