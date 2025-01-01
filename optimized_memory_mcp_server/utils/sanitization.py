"""Input sanitization utilities for SQLite backend."""
from typing import Optional, Union

def sanitize_input(value: Optional[Union[str, bytes]]) -> str:
    """Sanitize input to prevent SQL injection.
    
    Args:
        value: Input string or bytes to sanitize
        
    Returns:
        str: Sanitized string safe for SQLite
        
    Raises:
        TypeError: If value is not a string/bytes or is None
    """
    if value is None:
        raise TypeError("Cannot sanitize None value")
    if isinstance(value, bytes):
        value = value.decode('utf-8')
    if not isinstance(value, str):
        raise TypeError(f"Expected string or bytes, got {type(value)}")
    return value.replace("'", "''").replace('"', '""')
