"""Unit tests for entity operations."""
import pytest
from datetime import datetime, timedelta
import json
from ....exceptions import EntityNotFoundError, EntityAlreadyExistsError
from ....storage.sqlite.operations.entity_ops import EntityOperations

async def test_create_entities_basic(connection_pool):
    """Test basic entity creation."""
    entity_ops = EntityOperations(connection_pool)
    entities = [{
        "name": "test_entity",
        "entityType": "test_type",
        "observations": ["obs1", "obs2"],
        "confidence_score": 0.9,
        "context_source": "test",
        "metadata": {"key": "value"}
    }]
    
    created = await entity_ops.create_entities(entities)
    assert len(created) == 1
    assert created[0]["name"] == "test_entity"
    
    # Verify in database
    async with connection_pool.get_connection() as conn:
        cursor = await conn.execute(
            "SELECT * FROM entities WHERE name = ?",
            ("test_entity",)
        )
        row = await cursor.fetchone()
        assert row is not None
        assert row["entity_type"] == "test_type"
        assert "obs1" in row["observations"]

async def test_create_entities_batch(connection_pool):
    """Test batch entity creation."""
    entity_ops = EntityOperations(connection_pool)
    entities = [
        {
            "name": f"batch_entity_{i}",
            "entityType": "test_type",
            "observations": [f"obs_{i}"],
            "confidence_score": 0.9,
            "context_source": "test",
            "metadata": {"batch": i}
        }
        for i in range(10)
    ]
    
    created = await entity_ops.create_entities(entities, batch_size=3)
    assert len(created) == 10
    
    # Verify all entities were created
    async with connection_pool.get_connection() as conn:
        cursor = await conn.execute("SELECT COUNT(*) as count FROM entities")
        row = await cursor.fetchone()
        assert row["count"] == 10

async def test_create_duplicate_entity(connection_pool):
    """Test attempting to create a duplicate entity raises error."""
    entity_ops = EntityOperations(connection_pool)
    entity = {
        "name": "duplicate_test",
        "entityType": "test_type",
        "observations": ["obs1"]
    }
    
    await entity_ops.create_entities([entity])
    
    with pytest.raises(EntityAlreadyExistsError):
        await entity_ops.create_entities([entity])

async def test_add_observations(connection_pool):
    """Test adding observations to existing entities."""
    entity_ops = EntityOperations(connection_pool)
    
    # Create test entity
    await entity_ops.create_entities([{
        "name": "obs_test_entity",
        "entityType": "test_type",
        "observations": ["initial_obs"]
    }])
    
    # Add new observations
    new_observations = [{
        "entityName": "obs_test_entity",
        "contents": ["new_obs1", "new_obs2"]
    }]
    
    result = await entity_ops.add_observations(new_observations)
    assert "obs_test_entity" in result
    assert len(result["obs_test_entity"]) == 2
    
    # Verify in database
    async with connection_pool.get_connection() as conn:
        cursor = await conn.execute(
            "SELECT observations FROM entities WHERE name = ?",
            ("obs_test_entity",)
        )
        row = await cursor.fetchone()
        observations = row["observations"].split(",")
        assert "initial_obs" in observations
        assert "new_obs1" in observations
        assert "new_obs2" in observations

async def test_add_observations_nonexistent_entity(connection_pool):
    """Test adding observations to non-existent entity raises error."""
    entity_ops = EntityOperations(connection_pool)
    
    with pytest.raises(EntityNotFoundError):
        await entity_ops.add_observations([{
            "entityName": "nonexistent",
            "contents": ["obs1"]
        }])

async def test_delete_entities(connection_pool):
    """Test entity deletion with cascade to relations."""
    entity_ops = EntityOperations(connection_pool)
    
    # Create test entities and relations
    entities = [
        {"name": "delete_test_1", "entityType": "test_type", "observations": ["obs1"]},
        {"name": "delete_test_2", "entityType": "test_type", "observations": ["obs2"]}
    ]
    await entity_ops.create_entities(entities)
    
    # Create a relation between them
    async with connection_pool.get_connection() as conn:
        await conn.execute(
            """
            INSERT INTO relations (from_entity, to_entity, relation_type)
            VALUES (?, ?, ?)
            """,
            ("delete_test_1", "delete_test_2", "test_relation")
        )
    
    # Delete one entity
    await entity_ops.delete_entities(["delete_test_1"])
    
    # Verify entity and relations are deleted
    async with connection_pool.get_connection() as conn:
        cursor = await conn.execute(
            "SELECT COUNT(*) as count FROM entities WHERE name = ?",
            ("delete_test_1",)
        )
        row = await cursor.fetchone()
        assert row["count"] == 0
        
        cursor = await conn.execute(
            "SELECT COUNT(*) as count FROM relations WHERE from_entity = ? OR to_entity = ?",
            ("delete_test_1", "delete_test_1")
        )
        row = await cursor.fetchone()
        assert row["count"] == 0

async def test_delete_observations(connection_pool):
    """Test deleting specific observations from entities."""
    entity_ops = EntityOperations(connection_pool)
    
    # Create test entity with multiple observations
    await entity_ops.create_entities([{
        "name": "obs_delete_test",
        "entityType": "test_type",
        "observations": ["keep_obs", "delete_obs1", "delete_obs2", "keep_obs2"]
    }])
    
    # Delete specific observations
    await entity_ops.delete_observations([{
        "entityName": "obs_delete_test",
        "observations": ["delete_obs1", "delete_obs2"]
    }])
    
    # Verify remaining observations
    async with connection_pool.get_connection() as conn:
        cursor = await conn.execute(
            "SELECT observations FROM entities WHERE name = ?",
            ("obs_delete_test",)
        )
        row = await cursor.fetchone()
        observations = row["observations"].split(",")
        assert len(observations) == 2
        assert "keep_obs" in observations
        assert "keep_obs2" in observations
        assert "delete_obs1" not in observations
        assert "delete_obs2" not in observations

async def test_entity_partitioning(connection_pool):
    """Test entities are stored in correct partitions based on creation date."""
    entity_ops = EntityOperations(connection_pool)
    
    # Create entities with different dates
    recent_entity = {
        "name": "recent_entity",
        "entityType": "test_type",
        "observations": ["obs"],
        "created_at": datetime.now()
    }
    
    old_entity = {
        "name": "old_entity",
        "entityType": "test_type",
        "observations": ["obs"],
        "created_at": datetime.now() - timedelta(days=200)
    }
    
    await entity_ops.create_entities([recent_entity, old_entity])
    
    # Verify partition placement
    async with connection_pool.get_connection() as conn:
        cursor = await conn.execute(
            "SELECT COUNT(*) as count FROM entities_recent WHERE name = ?",
            ("recent_entity",)
        )
        assert (await cursor.fetchone())["count"] == 1
        
        cursor = await conn.execute(
            "SELECT COUNT(*) as count FROM entities_archive WHERE name = ?",
            ("old_entity",)
        )
        assert (await cursor.fetchone())["count"] == 1
