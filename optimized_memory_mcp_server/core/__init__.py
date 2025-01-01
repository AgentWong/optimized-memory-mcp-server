"""Core package containing fundamental interfaces and exceptions."""

from ..exceptions import (
    KnowledgeGraphError,
    EntityNotFoundError,
    EntityAlreadyExistsError,
    RelationValidationError,
)

__all__ = [
    'KnowledgeGraphError',
    'EntityNotFoundError',
    'EntityAlreadyExistsError',
    'RelationValidationError',
]
