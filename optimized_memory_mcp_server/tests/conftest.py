"""Test configuration and fixtures for SQLite storage backend tests."""
import os
import pytest
import asyncio
import aiosqlite
from typing import AsyncGenerator, Dict, Any
from datetime import datetime, timedelta

from ..storage.sqlite.connection import SQLiteConnectionPool
from ..storage.sqlite.schema import initialize_schema
from ..interfaces import Entity, Relation

@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="session")
async def test_db_path(tmp_path_factory) -> str:
    """Create a temporary database file."""
    db_dir = tmp_path_factory.mktemp("test_db")
    return str(db_dir / "test.db")

@pytest.fixture(scope="function")
async def connection_pool(test_db_path: str) -> AsyncGenerator[SQLiteConnectionPool, None]:
    """Create a connection pool for testing."""
    pool = SQLiteConnectionPool(test_db_path, pool_size=3, echo=True)
    async with pool.get_connection() as conn:
        await initialize_schema(conn)
    yield pool
    await pool.cleanup()
    try:
        os.remove(test_db_path)
    except FileNotFoundError:
        pass

@pytest.fixture
def sample_entities() -> Dict[str, Any]:
    """Create sample entity data for testing."""
    return [
        {
            "name": "test_entity_1",
            "entityType": "test_type",
            "observations": ["obs1", "obs2"],
            "created_at": datetime.now(),
            "last_updated": datetime.now(),
            "confidence_score": 0.9,
            "context_source": "test",
            "metadata": {"key": "value"},
            "category_id": 1
        },
        {
            "name": "test_entity_2",
            "entityType": "test_type",
            "observations": ["obs3", "obs4"],
            "created_at": datetime.now(),
            "last_updated": datetime.now(),
            "confidence_score": 0.8,
            "context_source": "test",
            "metadata": {"key": "value2"},
            "category_id": 1
        }
    ]

@pytest.fixture
def sample_relations() -> Dict[str, Any]:
    """Create sample relation data for testing."""
    return [
        {
            "from_": "test_entity_1",
            "to": "test_entity_2",
            "relationType": "test_relation",
            "created_at": datetime.now(),
            "valid_from": datetime.now(),
            "valid_until": datetime.now() + timedelta(days=30),
            "confidence_score": 0.95,
            "context_source": "test"
        }
    ]

@pytest.fixture
async def populated_db(
    connection_pool: SQLiteConnectionPool,
    sample_entities: Dict[str, Any],
    sample_relations: Dict[str, Any]
) -> AsyncGenerator[SQLiteConnectionPool, None]:
    """Create a database populated with test data."""
    async with connection_pool.get_connection() as conn:
        async with connection_pool.transaction(conn):
            # Create test category
            await conn.execute(
                """
                INSERT INTO knowledge_categories (id, name, priority, retention_period)
                VALUES (1, 'test_category', 1, 30)
                """
            )
            
            # Insert test entities
            for entity in sample_entities:
                await conn.execute(
                    """
                    INSERT INTO entities (
                        name, entity_type, observations, created_at, last_updated,
                        confidence_score, context_source, metadata, category_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        entity["name"],
                        entity["entityType"],
                        ",".join(entity["observations"]),
                        entity["created_at"].isoformat(),
                        entity["last_updated"].isoformat(),
                        entity["confidence_score"],
                        entity["context_source"],
                        str(entity["metadata"]),
                        entity["category_id"]
                    )
                )
            
            # Insert test relations
            for relation in sample_relations:
                await conn.execute(
                    """
                    INSERT INTO relations (
                        from_entity, to_entity, relation_type, created_at,
                        valid_from, valid_until, confidence_score, context_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        relation["from_"],
                        relation["to"],
                        relation["relationType"],
                        relation["created_at"].isoformat(),
                        relation["valid_from"].isoformat(),
                        relation["valid_until"].isoformat(),
                        relation["confidence_score"],
                        relation["context_source"]
                    )
                )
    
    yield connection_pool
