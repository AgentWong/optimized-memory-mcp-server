"""Validation utilities for entities and relations."""
from typing import List, Dict, Any, Union
from ..interfaces import Entity, Relation
from ..exceptions import RelationValidationError

def validate_entity(entity: Entity) -> None:
    """Validate entity fields.
    
    Args:
        entity: Entity to validate
        
    Raises:
        ValueError: If entity fields are invalid
    """
    if not entity.name or not isinstance(entity.name, str):
        raise ValueError("Entity name must be a non-empty string")
    if len(entity.name.strip()) == 0:
        raise ValueError("Entity name cannot be whitespace only")
        
    if not entity.entityType or not isinstance(entity.entityType, str):
        raise ValueError("Entity type must be a non-empty string")
    if len(entity.entityType.strip()) == 0:
        raise ValueError("Entity type cannot be whitespace only")
        
    if not isinstance(entity.observations, (list, tuple)):
        raise ValueError("Observations must be a list or tuple")
    
    # Validate individual observations
    for obs in entity.observations:
        if not isinstance(obs, str):
            raise ValueError(f"Observation must be string, got {type(obs)}")
        if len(obs.strip()) == 0:
            raise ValueError("Observations cannot be empty strings")

def validate_relation(relation: Relation) -> None:
    """Validate relation fields.
    
    Args:
        relation: Relation to validate
        
    Raises:
        RelationValidationError: If relation fields are invalid
    """
    if not isinstance(relation.from_, str):
        raise RelationValidationError("From entity must be a string")
    if not isinstance(relation.to, str):
        raise RelationValidationError("To entity must be a string")
    if not isinstance(relation.relationType, str):
        raise RelationValidationError("Relation type must be a string")
        
    if not relation.from_.strip():
        raise RelationValidationError("From entity cannot be empty")
    if not relation.to.strip():
        raise RelationValidationError("To entity cannot be empty")
    if not relation.relationType.strip():
        raise RelationValidationError("Relation type cannot be empty")
        
    if relation.from_ == relation.to:
        raise RelationValidationError("Self-referential relations are not allowed")
