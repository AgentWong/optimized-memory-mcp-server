"""Validation functions for entities and relations."""
from ...interfaces import Entity

def validate_entity(entity: Entity) -> None:
    """Validate an entity object.
    
    Args:
        entity: The entity to validate
        
    Raises:
        ValueError: If the entity is invalid
    """
    if not entity.name:
        raise ValueError("Entity name cannot be empty")
    if not entity.entityType:
        raise ValueError("Entity type cannot be empty")
    if entity.confidence_score < 0.0 or entity.confidence_score > 1.0:
        raise ValueError("Confidence score must be between 0.0 and 1.0")
