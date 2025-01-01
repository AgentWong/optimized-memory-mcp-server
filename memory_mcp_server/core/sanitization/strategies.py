"""Concrete sanitization strategy implementations."""
from typing import Any
from .base import SanitizationStrategy
from .validators import validate_sanitization_input


class SQLiteSanitizer(SanitizationStrategy):
    """SQLite-specific sanitization strategy."""
    
    def sanitize(self, value: Any) -> Any:
        """Sanitize input for SQLite.
        
        Args:
            value: Input value to sanitize
            
        Returns:
            str: Sanitized string value
            Any: Original value if not a string
            
        Raises:
            TypeError: If value is not a string or bytes
        """
        validate_sanitization_input(value)
        if isinstance(value, bytes):
            value = value.decode('utf-8')
        return value.replace("'", "''")
