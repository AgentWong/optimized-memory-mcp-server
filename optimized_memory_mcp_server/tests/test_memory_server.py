import pytest
import asyncio
from pathlib import Path
import tempfile
import json
from optimized_memory_mcp_server.storage.sqlite.optimized_manager import OptimizedSQLiteManager
from optimized_memory_mcp_server.interfaces import Entity, Relation
from optimized_memory_mcp_server.exceptions import EntityNotFoundError, EntityAlreadyExistsError

@pytest.fixture
async def db_manager():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile() as tmp:
        manager = OptimizedSQLiteManager(f"sqlite:///{tmp.name}")
        await manager.initialize()
        yield manager
        await manager.cleanup()

@pytest.mark.asyncio
async def test_create_and_read_entities(db_manager):
    """Test creating entities and reading them back."""
    # Create test entities
    entities = [
        {
            "name": "test_user",
            "entityType": "person",
            "observations": ["likes python", "works remotely"]
        },
        {
            "name": "test_company",
            "entityType": "organization",
            "observations": ["tech company", "founded in 2020"]
        }
    ]
    
    # Create entities
    created = await db_manager.create_entities(entities)
    assert len(created) == 2
    
    # Read back and verify
    graph = await db_manager.read_graph()
    assert len(graph["entities"]) == 2
    
    # Verify entity details
    test_user = next(e for e in graph["entities"] if e["name"] == "test_user")
    assert test_user["entityType"] == "person"
    assert "likes python" in test_user["observations"]

@pytest.mark.asyncio
async def test_create_and_read_relations(db_manager):
    """Test creating relations between entities."""
    # Create test entities first
    entities = [
        {"name": "user1", "entityType": "person", "observations": ["test user 1"]},
        {"name": "user2", "entityType": "person", "observations": ["test user 2"]}
    ]
    await db_manager.create_entities(entities)
    
    # Create relation
    relations = [
        {
            "from": "user1",
            "to": "user2",
            "relationType": "knows"
        }
    ]
    created_relations = await db_manager.create_relations(relations)
    assert len(created_relations) == 1
    
    # Read back and verify
    graph = await db_manager.read_graph()
    assert len(graph["relations"]) == 1
    assert graph["relations"][0]["from"] == "user1"
    assert graph["relations"][0]["to"] == "user2"
    assert graph["relations"][0]["relationType"] == "knows"

@pytest.mark.asyncio
async def test_add_and_delete_observations(db_manager):
    """Test adding and removing observations from entities."""
    # Create test entity
    await db_manager.create_entities([
        {"name": "test_entity", "entityType": "test", "observations": ["initial observation"]}
    ])
    
    # Add new observations
    new_observations = {
        "entityName": "test_entity",
        "contents": ["new observation 1", "new observation 2"]
    }
    await db_manager.add_observations([new_observations])
    
    # Verify observations were added
    graph = await db_manager.read_graph()
    test_entity = next(e for e in graph["entities"] if e["name"] == "test_entity")
    assert len(test_entity["observations"]) == 3
    assert "new observation 1" in test_entity["observations"]
    
    # Delete an observation
    await db_manager.delete_observations([{
        "entityName": "test_entity",
        "observations": ["new observation 1"]
    }])
    
    # Verify observation was deleted
    graph = await db_manager.read_graph()
    test_entity = next(e for e in graph["entities"] if e["name"] == "test_entity")
    assert len(test_entity["observations"]) == 2
    assert "new observation 1" not in test_entity["observations"]

@pytest.mark.asyncio
async def test_search_nodes(db_manager):
    """Test searching for nodes based on different criteria."""
    # Create test entities
    entities = [
        {
            "name": "search_test_1",
            "entityType": "test",
            "observations": ["contains searchable content"]
        },
        {
            "name": "search_test_2",
            "entityType": "searchable_type",
            "observations": ["regular content"]
        }
    ]
    await db_manager.create_entities(entities)
    
    # Search by observation content
    results = await db_manager.search_nodes("searchable content")
    assert len(results["entities"]) == 1
    assert results["entities"][0]["name"] == "search_test_1"
    
    # Search by entity type
    results = await db_manager.search_nodes("searchable_type")
    assert len(results["entities"]) == 1
    assert results["entities"][0]["name"] == "search_test_2"

@pytest.mark.asyncio
async def test_error_handling(db_manager):
    """Test error handling for various operations."""
    # Test creating duplicate entity
    entity = {"name": "duplicate_test", "entityType": "test", "observations": ["test"]}
    await db_manager.create_entities([entity])
    
    with pytest.raises(EntityAlreadyExistsError):
        await db_manager.create_entities([entity])
    
    # Test referencing non-existent entity
    with pytest.raises(EntityNotFoundError):
        await db_manager.add_observations([{
            "entityName": "nonexistent_entity",
            "contents": ["test observation"]
        }])
    
    # Test creating relation with non-existent entity
    with pytest.raises(EntityNotFoundError):
        await db_manager.create_relations([{
            "from": "nonexistent_entity",
            "to": "duplicate_test",
            "relationType": "test_relation"
        }])

@pytest.mark.asyncio
async def test_delete_entities(db_manager):
    """Test deleting entities and their relations."""
    # Create test entities and relations
    entities = [
        {"name": "delete_test_1", "entityType": "test", "observations": ["test"]},
        {"name": "delete_test_2", "entityType": "test", "observations": ["test"]}
    ]
    await db_manager.create_entities(entities)
    
    relations = [{
        "from": "delete_test_1",
        "to": "delete_test_2",
        "relationType": "test_relation"
    }]
    await db_manager.create_relations(relations)
    
    # Delete one entity
    await db_manager.delete_entities(["delete_test_1"])
    
    # Verify entity and its relations are gone
    graph = await db_manager.read_graph()
    assert len([e for e in graph["entities"] if e["name"] == "delete_test_1"]) == 0
    assert len(graph["relations"]) == 0
