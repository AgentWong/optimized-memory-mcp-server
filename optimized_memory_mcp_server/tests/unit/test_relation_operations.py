"""Unit tests for relation operations."""
import pytest
from datetime import datetime, timedelta
import json
from ....exceptions import EntityNotFoundError
from ....storage.sqlite.operations.relation_ops import RelationOperations

async def test_create_relations_basic(connection_pool, populated_db):
    """Test basic relation creation."""
    relation_ops = RelationOperations(connection_pool)
    relation = {
        "from_": "test_entity_1",
        "to": "test_entity_2",
        "relationType": "test_relation",
        "confidence_score": 0.95,
        "context_source": "test",
        "valid_from": datetime.now(),
        "valid_until": datetime.now() + timedelta(days=30)
    }
    
    created = await relation_ops.create_relations([relation])
    assert len(created) == 1
    assert created[0]["from_"] == "test_entity_1"
    assert created[0]["to"] == "test_entity_2"
    
    # Verify in database
    async with connection_pool.get_connection() as conn:
        cursor = await conn.execute(
            """
            SELECT * FROM relations 
            WHERE from_entity = ? AND to_entity = ? AND relation_type = ?
            """,
            ("test_entity_1", "test_entity_2", "test_relation")
        )
        row = await cursor.fetchone()
        assert row is not None
        assert float(row["confidence_score"]) == 0.95

async def test_create_relations_batch(connection_pool, populated_db):
    """Test batch relation creation."""
    relation_ops = RelationOperations(connection_pool)
    relations = [
        {
            "from_": "test_entity_1",
            "to": "test_entity_2",
            "relationType": f"batch_relation_{i}",
            "confidence_score": 0.9,
            "context_source": "test",
            "valid_from": datetime.now(),
            "valid_until": datetime.now() + timedelta(days=30)
        }
        for i in range(5)
    ]
    
    created = await relation_ops.create_relations(relations, batch_size=2)
    assert len(created) == 5
    
    # Verify all relations were created
    async with connection_pool.get_connection() as conn:
        cursor = await conn.execute(
            "SELECT COUNT(*) as count FROM relations WHERE from_entity = ?",
            ("test_entity_1",)
        )
        row = await cursor.fetchone()
        assert row["count"] == 5

async def test_create_relation_nonexistent_entity(connection_pool):
    """Test creating relation with non-existent entity raises error."""
    relation_ops = RelationOperations(connection_pool)
    relation = {
        "from_": "nonexistent_entity",
        "to": "test_entity_2",
        "relationType": "test_relation"
    }
    
    with pytest.raises(EntityNotFoundError):
        await relation_ops.create_relations([relation])

async def test_delete_relations(connection_pool, populated_db):
    """Test relation deletion."""
    relation_ops = RelationOperations(connection_pool)
    
    # Create test relations
    test_relations = [
        {
            "from_": "test_entity_1",
            "to": "test_entity_2",
            "relationType": "delete_test_relation"
        }
    ]
    await relation_ops.create_relations(test_relations)
    
    # Delete relations
    await relation_ops.delete_relations(test_relations)
    
    # Verify deletion
    async with connection_pool.get_connection() as conn:
        cursor = await conn.execute(
            """
            SELECT COUNT(*) as count FROM relations 
            WHERE from_entity = ? AND to_entity = ? AND relation_type = ?
            """,
            ("test_entity_1", "test_entity_2", "delete_test_relation")
        )
        row = await cursor.fetchone()
        assert row["count"] == 0

async def test_get_entity_relations(connection_pool, populated_db):
    """Test retrieving relations for an entity."""
    relation_ops = RelationOperations(connection_pool)
    
    # Create multiple relations
    relations = [
        {
            "from_": "test_entity_1",
            "to": "test_entity_2",
            "relationType": "test_relation_1"
        },
        {
            "from_": "test_entity_1",
            "to": "test_entity_2",
            "relationType": "test_relation_2"
        }
    ]
    await relation_ops.create_relations(relations)
    
    # Get relations
    entity_relations = await relation_ops.get_entity_relations("test_entity_1")
    assert len(entity_relations) == 2
    relation_types = {r["relationType"] for r in entity_relations}
    assert "test_relation_1" in relation_types
    assert "test_relation_2" in relation_types

async def test_update_relation_validity(connection_pool, populated_db):
    """Test updating relation validity period."""
    relation_ops = RelationOperations(connection_pool)
    
    # Create test relation
    relation = {
        "from_": "test_entity_1",
        "to": "test_entity_2",
        "relationType": "validity_test",
        "valid_from": datetime.now(),
        "valid_until": datetime.now() + timedelta(days=30)
    }
    await relation_ops.create_relations([relation])
    
    # Update validity
    new_valid_until = datetime.now() + timedelta(days=60)
    await relation_ops.update_relation_validity(
        "test_entity_1",
        "test_entity_2",
        "validity_test",
        new_valid_until
    )
    
    # Verify update
    async with connection_pool.get_connection() as conn:
        cursor = await conn.execute(
            """
            SELECT valid_until FROM relations 
            WHERE from_entity = ? AND to_entity = ? AND relation_type = ?
            """,
            ("test_entity_1", "test_entity_2", "validity_test")
        )
        row = await cursor.fetchone()
        stored_valid_until = datetime.fromisoformat(row["valid_until"])
        assert abs((stored_valid_until - new_valid_until).total_seconds()) < 1

async def test_get_relations_by_type(connection_pool, populated_db):
    """Test retrieving relations by type."""
    relation_ops = RelationOperations(connection_pool)
    
    # Create relations with different types
    relations = [
        {
            "from_": "test_entity_1",
            "to": "test_entity_2",
            "relationType": "type_test_1"
        },
        {
            "from_": "test_entity_2",
            "to": "test_entity_1",
            "relationType": "type_test_1"
        },
        {
            "from_": "test_entity_1",
            "to": "test_entity_2",
            "relationType": "type_test_2"
        }
    ]
    await relation_ops.create_relations(relations)
    
    # Get relations by type
    type_relations = await relation_ops.get_relations_by_type("type_test_1")
    assert len(type_relations) == 2
    assert all(r["relationType"] == "type_test_1" for r in type_relations)

async def test_relation_confidence_update(connection_pool, populated_db):
    """Test updating relation confidence scores."""
    relation_ops = RelationOperations(connection_pool)
    
    # Create test relation
    relation = {
        "from_": "test_entity_1",
        "to": "test_entity_2",
        "relationType": "confidence_test",
        "confidence_score": 0.5
    }
    await relation_ops.create_relations([relation])
    
    # Update confidence
    new_confidence = 0.8
    await relation_ops.update_relation_confidence(
        "test_entity_1",
        "test_entity_2",
        "confidence_test",
        new_confidence
    )
    
    # Verify update
    async with connection_pool.get_connection() as conn:
        cursor = await conn.execute(
            """
            SELECT confidence_score FROM relations 
            WHERE from_entity = ? AND to_entity = ? AND relation_type = ?
            """,
            ("test_entity_1", "test_entity_2", "confidence_test")
        )
        row = await cursor.fetchone()
        assert float(row["confidence_score"]) == new_confidence
