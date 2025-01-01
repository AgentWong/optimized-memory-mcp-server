"""Core package containing fundamental interfaces and exceptions."""

from ..interfaces import Entity, Relation, KnowledgeGraph
from ..exceptions import (
    KnowledgeGraphError,
    EntityNotFoundError,
    EntityAlreadyExistsError,
    RelationValidationError,
)

__all__ = [
    'Entity',
    'Relation', 
    'KnowledgeGraph',
    'KnowledgeGraphError',
    'EntityNotFoundError',
    'EntityAlreadyExistsError',
    'RelationValidationError',
]
