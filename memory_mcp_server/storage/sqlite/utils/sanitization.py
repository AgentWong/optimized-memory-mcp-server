"""Input sanitization utilities for SQLite backend."""
from ....core.sanitization.strategies import SQLiteSanitizer
from ....core.sanitization.validators import validate_sanitization_input

_sanitizer = SQLiteSanitizer()


def sanitize_input(value: str) -> str:
    """Sanitize input to prevent SQL injection.
    
    Args:
        value: Input string to sanitize
        
    Returns:
        str: Sanitized string safe for SQLite
        
    Raises:
        TypeError: If value is not a string or bytes
    """
    validate_sanitization_input(value)
    return _sanitizer.sanitize(value)
