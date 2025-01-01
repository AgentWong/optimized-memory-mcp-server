"""Validation utilities for SQLite operations."""
from typing import Any
from ....interfaces import Entity, Relation

def validate_entity(entity: Entity) -> None:
    """Validate entity fields."""
    if not entity.name or not isinstance(entity.name, str):
        raise ValueError("Entity name must be a non-empty string")
    if not entity.entityType or not isinstance(entity.entityType, str):
        raise ValueError("Entity type must be a non-empty string")
    if not isinstance(entity.observations, (list, tuple)):
        raise ValueError("Observations must be a list or tuple")
"""Validation utilities for SQLite backend."""
from typing import Any
from ....interfaces import Entity, Relation

def validate_entity(entity: Entity) -> None:
    """Validate entity fields.
    
    Args:
        entity: Entity to validate
        
    Raises:
        ValueError: If entity fields are invalid
    """
    if not entity.name or not isinstance(entity.name, str):
        raise ValueError("Entity name must be a non-empty string")
    if not entity.entityType or not isinstance(entity.entityType, str):
        raise ValueError("Entity type must be a non-empty string")
    if not isinstance(entity.observations, (list, tuple)):
        raise ValueError("Observations must be a list or tuple")
        
def validate_relation(relation: Relation) -> None:
    """Validate relation fields.
    
    Args:
        relation: Relation to validate
        
    Raises:
        ValueError: If relation fields are invalid
    """
    if not relation.from_ or not relation.to or not relation.relationType:
        raise ValueError(f"Invalid relation: {relation}")
