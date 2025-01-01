"""Input validation for sanitization strategies."""
from typing import Any

def validate_sanitization_input(value: Any) -> None:
    """Validate input before sanitization.
    
    Args:
        value: Input value to validate
        
    Raises:
        TypeError: If value is not a string or bytes
    """
    if not isinstance(value, (str, bytes)):
        raise TypeError(f"Expected string or bytes, got {type(value)}")
