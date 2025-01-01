"""Base classes for input sanitization."""
from abc import ABC, abstractmethod
from typing import Any


class SanitizationStrategy(ABC):
    """Base class for input sanitization strategies."""
    
    @abstractmethod
    def sanitize(self, value: Any) -> Any:
        """Sanitize input value.
        
        Args:
            value: Input value to sanitize
            
        Returns:
            Sanitized value
        """
        pass
