"""Unit tests for temporal tracking operations."""
import pytest
from datetime import datetime, timedelta
import json
from ....exceptions import EntityNotFoundError
from ....storage.sqlite.operations.temporal_ops import TemporalOperations

async def test_entity_change_tracking(connection_pool, populated_db):
    """Test entity change history tracking."""
    temporal_ops = TemporalOperations(connection_pool)
    
    # Get initial state
    initial_changes = await temporal_ops.get_entity_changes("test_entity_1")
    initial_count = len(initial_changes)
    
    # Make some changes to the entity
    async with connection_pool.get_connection() as conn:
        await conn.execute(
            """
            UPDATE entities 
            SET observations = ?, confidence_score = ?
            WHERE name = ?
            """,
            ("new_observation", 0.95, "test_entity_1")
        )
    
    # Verify changes were tracked
    changes = await temporal_ops.get_entity_changes("test_entity_1")
    assert len(changes) == initial_count + 1
    latest_change = changes[-1]
    assert latest_change["change_type"] == "update"
    assert json.loads(latest_change["entity_state"])["observations"] == "new_observation"

async def test_relation_change_tracking(connection_pool, populated_db):
    """Test relation change history tracking."""
    temporal_ops = TemporalOperations(connection_pool)
    
    # Create a new relation
    async with connection_pool.get_connection() as conn:
        await conn.execute(
            """
            INSERT INTO relations (
                from_entity, to_entity, relation_type,
                confidence_score, context_source
            ) VALUES (?, ?, ?, ?, ?)
            """,
            ("test_entity_1", "test_entity_2", "temporal_test", 0.8, "test")
        )
    
    # Get relation changes
    changes = await temporal_ops.get_relation_changes(
        "test_entity_1", 
        "test_entity_2",
        "temporal_test"
    )
    assert len(changes) > 0
    assert changes[0]["change_type"] == "create"

async def test_temporal_query_by_date(connection_pool, populated_db):
    """Test querying entity state at a specific point in time."""
    temporal_ops = TemporalOperations(connection_pool)
    
    # Record current state
    current_time = datetime.now()
    
    # Make a change
    async with connection_pool.get_connection() as conn:
        await conn.execute(
            """
            UPDATE entities 
            SET metadata = ?
            WHERE name = ?
            """,
            (json.dumps({"updated": True}), "test_entity_1")
        )
    
    # Query state at previous time
    historical_state = await temporal_ops.get_entity_state_at(
        "test_entity_1",
        current_time - timedelta(seconds=1)
    )
    assert "updated" not in json.loads(historical_state["metadata"])
    
    # Query current state
    current_state = await temporal_ops.get_entity_state_at(
        "test_entity_1",
        datetime.now()
    )
    assert json.loads(current_state["metadata"])["updated"] is True

async def test_change_metadata(connection_pool, populated_db):
    """Test change metadata tracking."""
    temporal_ops = TemporalOperations(connection_pool)
    
    # Make a change with metadata
    change_metadata = {"reason": "test update", "user": "test_user"}
    async with connection_pool.get_connection() as conn:
        await conn.execute(
            """
            UPDATE entities 
            SET confidence_score = ?
            WHERE name = ?
            """,
            (0.9, "test_entity_1")
        )
        await temporal_ops.record_change_metadata(
            "test_entity_1",
            change_metadata
        )
    
    # Verify metadata was recorded
    changes = await temporal_ops.get_entity_changes("test_entity_1")
    latest_change = changes[-1]
    assert "changed_by" in latest_change
    assert latest_change["changed_by"] == "test_user"

async def test_temporal_query_relations(connection_pool, populated_db):
    """Test querying relations at a specific point in time."""
    temporal_ops = TemporalOperations(connection_pool)
    
    # Record current time
    current_time = datetime.now()
    
    # Create a new relation
    async with connection_pool.get_connection() as conn:
        await conn.execute(
            """
            INSERT INTO relations (
                from_entity, to_entity, relation_type,
                valid_from, valid_until
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                "test_entity_1",
                "test_entity_2",
                "temporal_test",
                current_time.isoformat(),
                (current_time + timedelta(days=30)).isoformat()
            )
        )
    
    # Query relations at different times
    past_relations = await temporal_ops.get_relations_at(
        "test_entity_1",
        current_time - timedelta(days=1)
    )
    assert not any(r["relation_type"] == "temporal_test" for r in past_relations)
    
    future_relations = await temporal_ops.get_relations_at(
        "test_entity_1",
        current_time + timedelta(days=1)
    )
    assert any(r["relation_type"] == "temporal_test" for r in future_relations)

async def test_change_summary(connection_pool, populated_db):
    """Test generating change summaries for entities."""
    temporal_ops = TemporalOperations(connection_pool)
    
    # Make multiple changes
    async with connection_pool.get_connection() as conn:
        for i in range(3):
            await conn.execute(
                """
                UPDATE entities 
                SET confidence_score = ?,
                    metadata = ?
                WHERE name = ?
                """,
                (
                    0.7 + (i * 0.1),
                    json.dumps({"version": i}),
                    "test_entity_1"
                )
            )
    
    # Get change summary
    summary = await temporal_ops.get_change_summary(
        "test_entity_1",
        datetime.now() - timedelta(minutes=5),
        datetime.now()
    )
    
    assert len(summary["changes"]) == 3
    assert "confidence_score" in summary["changed_fields"]
    assert "metadata" in summary["changed_fields"]

async def test_batch_temporal_query(connection_pool, populated_db):
    """Test querying multiple entities' states at a point in time."""
    temporal_ops = TemporalOperations(connection_pool)
    
    # Record current time
    current_time = datetime.now()
    
    # Make changes to multiple entities
    async with connection_pool.get_connection() as conn:
        for entity in ["test_entity_1", "test_entity_2"]:
            await conn.execute(
                """
                UPDATE entities 
                SET metadata = ?
                WHERE name = ?
                """,
                (json.dumps({"updated": True}), entity)
            )
    
    # Query multiple historical states
    historical_states = await temporal_ops.get_entity_states_at(
        ["test_entity_1", "test_entity_2"],
        current_time - timedelta(seconds=1)
    )
    
    assert len(historical_states) == 2
    for state in historical_states.values():
        assert "updated" not in json.loads(state["metadata"])

async def test_invalid_temporal_query(connection_pool):
    """Test handling of invalid temporal queries."""
    temporal_ops = TemporalOperations(connection_pool)
    
    with pytest.raises(EntityNotFoundError):
        await temporal_ops.get_entity_state_at(
            "nonexistent_entity",
            datetime.now()
        )

async def test_change_tracking_performance(connection_pool, populated_db):
    """Test performance of change tracking with many changes."""
    temporal_ops = TemporalOperations(connection_pool)
    
    # Make many changes
    async with connection_pool.get_connection() as conn:
        for i in range(100):
            await conn.execute(
                """
                UPDATE entities 
                SET confidence_score = ?
                WHERE name = ?
                """,
                (0.5 + (i * 0.001), "test_entity_1")
            )
    
    # Measure time to query changes
    start_time = datetime.now()
    changes = await temporal_ops.get_entity_changes("test_entity_1")
    query_time = (datetime.now() - start_time).total_seconds()
    
    assert len(changes) >= 100
    assert query_time < 1.0  # Should complete within 1 second
