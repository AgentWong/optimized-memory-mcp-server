"""Input sanitization utilities for SQLite backend."""

def sanitize_input(value: str) -> str:
    """Sanitize input to prevent SQL injection."""
    return value.replace("'", "''")
