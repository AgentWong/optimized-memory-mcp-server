"""Tests for sanitization strategies."""
import pytest
from ....core.sanitization.strategies import SQLiteSanitizer


def test_sqlite_sanitizer_string():
    """Test SQLite sanitization of strings."""
    sanitizer = SQLiteSanitizer()
    assert sanitizer.sanitize("O'Reilly") == "O''Reilly"
    assert sanitizer.sanitize("Don't") == "Don''t"
    assert sanitizer.sanitize("Regular text") == "Regular text"


def test_sqlite_sanitizer_non_string():
    """Test SQLite sanitization of non-string values."""
    sanitizer = SQLiteSanitizer()
    with pytest.raises(TypeError):
        sanitizer.sanitize(123)
    with pytest.raises(TypeError):
        sanitizer.sanitize(None)
    with pytest.raises(TypeError):
        sanitizer.sanitize([1, 2, 3])
"""Tests for sanitization strategies."""
import pytest
from ....core.sanitization.strategies import SQLiteSanitizer


def test_sqlite_sanitizer_string():
    """Test SQLite sanitization of strings."""
    sanitizer = SQLiteSanitizer()
    assert sanitizer.sanitize("O'Reilly") == "O''Reilly"
    assert sanitizer.sanitize("Don't") == "Don''t"
    assert sanitizer.sanitize("Regular text") == "Regular text"


def test_sqlite_sanitizer_non_string():
    """Test SQLite sanitization of non-string values."""
    sanitizer = SQLiteSanitizer()
    with pytest.raises(TypeError):
        sanitizer.sanitize(123)
    with pytest.raises(TypeError):
        sanitizer.sanitize(None)
    with pytest.raises(TypeError):
        sanitizer.sanitize([1, 2, 3])
