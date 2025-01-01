from dataclasses import dataclass, field
from typing import List, Dict, Any, Union, Tuple, Optional
from datetime import datetime
import json

@dataclass(frozen=True)
class Entity:
    name: str
    entityType: str
    observations: Tuple[str, ...]
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_updated: datetime = field(default_factory=datetime.utcnow)
    confidence_score: float = 1.0
    context_source: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    category_id: Optional[int] = None

    def __init__(
        self,
        name: str,
        entityType: str,
        observations: Union[List[str], Tuple[str, ...]],
        created_at: Optional[datetime] = None,
        last_updated: Optional[datetime] = None,
        confidence_score: float = 1.0,
        context_source: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        category_id: Optional[int] = None
    ):
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "entityType", entityType)
        object.__setattr__(self, "observations", tuple(observations))
        object.__setattr__(self, "created_at", created_at or datetime.utcnow())
        object.__setattr__(self, "last_updated", last_updated or datetime.utcnow())
        object.__setattr__(self, "confidence_score", max(0.0, min(1.0, confidence_score)))
        object.__setattr__(self, "context_source", context_source)
        object.__setattr__(self, "metadata", metadata or {})
        object.__setattr__(self, "category_id", category_id)

    @classmethod
    def from_dict(cls, data: dict) -> 'Entity':
        return cls(
            name=data["name"],
            entityType=data["entityType"],
            observations=data["observations"],
            created_at=datetime.fromisoformat(data["created_at"]) if "created_at" in data else None,
            last_updated=datetime.fromisoformat(data["last_updated"]) if "last_updated" in data else None,
            confidence_score=float(data.get("confidence_score", 1.0)),
            context_source=data.get("context_source"),
            metadata=data.get("metadata", {}),
            category_id=data.get("category_id")
        )

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "entityType": self.entityType,
            "observations": list(self.observations),  # Convert tuple back to list
            "created_at": self.created_at.isoformat(),
            "last_updated": self.last_updated.isoformat(),
            "confidence_score": self.confidence_score,
            "context_source": self.context_source,
            "metadata": self.metadata,
            "category_id": self.category_id
        }


@dataclass(frozen=True)
class Relation:
    from_: str
    to: str
    relationType: str
    created_at: datetime = field(default_factory=datetime.utcnow)
    valid_from: datetime = field(default_factory=datetime.utcnow)
    valid_until: Optional[datetime] = None
    confidence_score: float = 1.0
    context_source: Optional[str] = None

    def __init__(
        self,
        from_: str,
        to: str,
        relationType: str,
        created_at: Optional[datetime] = None,
        valid_from: Optional[datetime] = None,
        valid_until: Optional[datetime] = None,
        confidence_score: float = 1.0,
        context_source: Optional[str] = None
    ):
        object.__setattr__(self, "from_", from_)
        object.__setattr__(self, "to", to)
        object.__setattr__(self, "relationType", relationType)
        object.__setattr__(self, "created_at", created_at or datetime.utcnow())
        object.__setattr__(self, "valid_from", valid_from or datetime.utcnow())
        object.__setattr__(self, "valid_until", valid_until)
        object.__setattr__(self, "confidence_score", max(0.0, min(1.0, confidence_score)))
        object.__setattr__(self, "context_source", context_source)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Relation':
        """Create a Relation instance from a dictionary."""
        return cls(
            from_=data.get("from_", data.get("from")),
            to=data["to"],
            relationType=data["relationType"],
            created_at=datetime.fromisoformat(data["created_at"]) if "created_at" in data else None,
            valid_from=datetime.fromisoformat(data["valid_from"]) if "valid_from" in data else None,
            valid_until=datetime.fromisoformat(data["valid_until"]) if "valid_until" in data else None,
            confidence_score=float(data.get("confidence_score", 1.0)),
            context_source=data.get("context_source")
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to API-compatible dictionary format."""
        result = {
            "from": self.from_,
            "to": self.to,
            "relationType": self.relationType,
            "created_at": self.created_at.isoformat(),
            "valid_from": self.valid_from.isoformat(),
            "confidence_score": self.confidence_score,
            "context_source": self.context_source
        }
        if self.valid_until:
            result["valid_until"] = self.valid_until.isoformat()
        return result


@dataclass
class KnowledgeGraph:
    entities: List[Entity]
    relations: List[Relation]
