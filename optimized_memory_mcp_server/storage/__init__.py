"""Storage backend implementations for the memory MCP server."""

from typing import Optional
from pathlib import Path

from .base import StorageBackend
from .sqlite import SQLiteStorageBackend

def create_storage_backend(
    backend_type: str = "sqlite",
    database_url: Optional[str] = None,
    **kwargs
) -> StorageBackend:
    """Factory function to create storage backend instances.
    
    Args:
        backend_type: Type of storage backend ("sqlite")
        database_url: Database connection URL
        **kwargs: Additional backend-specific configuration
    
    Returns:
        StorageBackend implementation
    
    Raises:
        ValueError: If backend_type is not supported
    """
    if backend_type == "sqlite":
        if not database_url:
            database_url = "sqlite:///memory.db"
        return SQLiteStorageBackend(database_url=database_url, **kwargs)
    else:
        raise ValueError(f"Unsupported storage backend: {backend_type}")

__all__ = ['StorageBackend', 'create_storage_backend']
