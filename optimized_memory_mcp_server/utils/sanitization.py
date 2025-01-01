"""Input sanitization utilities."""

def sanitize_input(value: str) -> str:
    """Sanitize input to prevent SQL injection.
    
    Args:
        value: Input string to sanitize
        
    Returns:
        Sanitized string with escaped single quotes
    """
    return value.replace("'", "''")
