"""Unit tests for search operations."""
import pytest
from datetime import datetime
from ....storage.sqlite.operations.search_ops import SearchOperations

async def test_basic_node_search(connection_pool, populated_db):
    """Test basic search functionality."""
    search_ops = SearchOperations(connection_pool)
    
    # Search for existing entity
    result = await search_ops.search_nodes("test_entity")
    assert len(result["entities"]) > 0
    assert any("test_entity" in e["name"] for e in result["entities"])

async def test_search_result_caching(connection_pool, populated_db):
    """Test search results are properly cached."""
    search_ops = SearchOperations(connection_pool)
    
    # First search should cache
    query = "test_entity"
    result1 = await search_ops.search_nodes(query)
    
    # Second search should use cache
    cache_key = f"search:{query}"
    cached_result = connection_pool.get_cached_query(cache_key)
    assert cached_result is not None
    assert cached_result == result1

async def test_entity_name_pattern_matching(connection_pool, populated_db):
    """Test entity name pattern matching."""
    search_ops = SearchOperations(connection_pool)
    
    # Create test entities with specific patterns
    entities = [
        {
            "name": "pattern_test_1",
            "entityType": "test_type",
            "observations": ["obs1"]
        },
        {
            "name": "pattern_test_2",
            "entityType": "test_type",
            "observations": ["obs2"]
        }
    ]
    async with connection_pool.get_connection() as conn:
        for entity in entities:
            await conn.execute(
                """
                INSERT INTO entities (name, entity_type, observations)
                VALUES (?, ?, ?)
                """,
                (entity["name"], entity["entityType"], entity["observations"][0])
            )
    
    result = await search_ops.search_nodes("pattern_test")
    assert len(result["entities"]) == 2
    assert all("pattern_test" in e["name"] for e in result["entities"])

async def test_entity_type_pattern_matching(connection_pool, populated_db):
    """Test entity type pattern matching."""
    search_ops = SearchOperations(connection_pool)
    
    # Create entities with specific type
    entities = [
        {
            "name": "type_test_1",
            "entityType": "special_type",
            "observations": ["obs1"]
        },
        {
            "name": "type_test_2",
            "entityType": "special_type",
            "observations": ["obs2"]
        }
    ]
    async with connection_pool.get_connection() as conn:
        for entity in entities:
            await conn.execute(
                """
                INSERT INTO entities (name, entity_type, observations)
                VALUES (?, ?, ?)
                """,
                (entity["name"], entity["entityType"], entity["observations"][0])
            )
    
    result = await search_ops.search_nodes("special_type")
    assert len(result["entities"]) == 2
    assert all(e["entityType"] == "special_type" for e in result["entities"])

async def test_observation_content_search(connection_pool, populated_db):
    """Test searching through observation content."""
    search_ops = SearchOperations(connection_pool)
    
    # Create entity with specific observation
    async with connection_pool.get_connection() as conn:
        await conn.execute(
            """
            INSERT INTO entities (name, entity_type, observations)
            VALUES (?, ?, ?)
            """,
            ("obs_test", "test_type", "unique_observation_pattern")
        )
    
    result = await search_ops.search_nodes("unique_observation_pattern")
    assert len(result["entities"]) == 1
    assert result["entities"][0]["name"] == "obs_test"

async def test_related_entity_retrieval(connection_pool, populated_db):
    """Test retrieval of related entities in search results."""
    search_ops = SearchOperations(connection_pool)
    
    # Create related entities
    async with connection_pool.get_connection() as conn:
        await conn.execute(
            """
            INSERT INTO entities (name, entity_type)
            VALUES (?, ?), (?, ?)
            """,
            ("related_1", "test_type", "related_2", "test_type")
        )
        await conn.execute(
            """
            INSERT INTO relations (from_entity, to_entity, relation_type)
            VALUES (?, ?, ?)
            """,
            ("related_1", "related_2", "test_relation")
        )
    
    result = await search_ops.search_nodes("related")
    assert len(result["entities"]) == 2
    assert len(result["relations"]) == 1
    assert result["relations"][0]["from_"] == "related_1"
    assert result["relations"][0]["to"] == "related_2"

async def test_empty_query_handling(connection_pool):
    """Test handling of empty search queries."""
    search_ops = SearchOperations(connection_pool)
    
    with pytest.raises(ValueError, match="Search query cannot be empty"):
        await search_ops.search_nodes("")

async def test_cache_invalidation(connection_pool, populated_db):
    """Test search cache invalidation."""
    search_ops = SearchOperations(connection_pool)
    
    # Perform initial search
    query = "test_entity"
    await search_ops.search_nodes(query)
    
    # Verify result is cached
    cache_key = f"search:{query}"
    assert connection_pool.get_cached_query(cache_key) is not None
    
    # Invalidate cache
    connection_pool.invalidate_cache_pattern("search")
    
    # Verify cache was cleared
    assert connection_pool.get_cached_query(cache_key) is None

async def test_open_nodes_functionality(connection_pool, populated_db):
    """Test opening specific nodes by name."""
    search_ops = SearchOperations(connection_pool)
    
    # Create test entities
    names = ["open_test_1", "open_test_2"]
    async with connection_pool.get_connection() as conn:
        for name in names:
            await conn.execute(
                """
                INSERT INTO entities (name, entity_type)
                VALUES (?, ?)
                """,
                (name, "test_type")
            )
        await conn.execute(
            """
            INSERT INTO relations (from_entity, to_entity, relation_type)
            VALUES (?, ?, ?)
            """,
            (names[0], names[1], "test_relation")
        )
    
    result = await search_ops.open_nodes(names)
    assert len(result["entities"]) == 2
    assert len(result["relations"]) == 1
    assert all(e["name"] in names for e in result["entities"])
"""Unit tests for search operations."""
import pytest
from datetime import datetime
from ....storage.sqlite.operations.search_ops import SearchOperations

async def test_basic_search(connection_pool, populated_db):
    """Test basic search functionality."""
    search_ops = SearchOperations(connection_pool)
    
    # Search for existing entity
    result = await search_ops.search_nodes("test_entity")
    assert len(result["entities"]) > 0
    assert any(e["name"] == "test_entity_1" for e in result["entities"])
    assert any(e["name"] == "test_entity_2" for e in result["entities"])

async def test_search_empty_query(connection_pool):
    """Test search with empty query raises error."""
    search_ops = SearchOperations(connection_pool)
    
    with pytest.raises(ValueError, match="Search query cannot be empty"):
        await search_ops.search_nodes("")

async def test_search_by_entity_type(connection_pool, populated_db):
    """Test searching by entity type."""
    search_ops = SearchOperations(connection_pool)
    
    result = await search_ops.search_nodes("test_type")
    assert len(result["entities"]) > 0
    assert all(e["entityType"] == "test_type" for e in result["entities"])

async def test_search_by_observation(connection_pool, populated_db):
    """Test searching by observation content."""
    search_ops = SearchOperations(connection_pool)
    
    result = await search_ops.search_nodes("obs1")
    assert len(result["entities"]) > 0
    assert any("obs1" in e["observations"] for e in result["entities"])

async def test_search_with_relations(connection_pool, populated_db):
    """Test search returns related entities and their relations."""
    search_ops = SearchOperations(connection_pool)
    
    result = await search_ops.search_nodes("test_entity_1")
    assert len(result["entities"]) >= 1
    assert len(result["relations"]) > 0
    assert any(r["from_"] == "test_entity_1" for r in result["relations"])

async def test_search_cache(connection_pool, populated_db):
    """Test search result caching."""
    search_ops = SearchOperations(connection_pool)
    
    # First search should cache
    query = "test_entity"
    result1 = await search_ops.search_nodes(query)
    
    # Modify database directly to verify cache is used
    async with connection_pool.get_connection() as conn:
        await conn.execute(
            """
            INSERT INTO entities (name, entity_type, observations)
            VALUES (?, ?, ?)
            """,
            ("new_test_entity", "test_type", "new_observation")
        )
        await conn.commit()
    
    # Second search should return cached result
    result2 = await search_ops.search_nodes(query)
    assert result1 == result2
    
    # Clear cache and verify new entity is found
    connection_pool.invalidate_cache()
    result3 = await search_ops.search_nodes(query)
    assert len(result3["entities"]) > len(result1["entities"])

async def test_open_nodes(connection_pool, populated_db):
    """Test retrieving specific nodes by name."""
    search_ops = SearchOperations(connection_pool)
    
    result = await search_ops.open_nodes(["test_entity_1", "test_entity_2"])
    assert len(result["entities"]) == 2
    assert len(result["relations"]) > 0
    entity_names = {e["name"] for e in result["entities"]}
    assert "test_entity_1" in entity_names
    assert "test_entity_2" in entity_names

async def test_open_nodes_empty_list(connection_pool):
    """Test open_nodes with empty list returns empty result."""
    search_ops = SearchOperations(connection_pool)
    
    result = await search_ops.open_nodes([])
    assert len(result["entities"]) == 0
    assert len(result["relations"]) == 0

async def test_search_special_characters(connection_pool, populated_db):
    """Test search with special characters is properly sanitized."""
    search_ops = SearchOperations(connection_pool)
    
    # Should not raise any SQL injection errors
    result = await search_ops.search_nodes("test'; DROP TABLE entities; --")
    assert isinstance(result, dict)
    assert "entities" in result
    assert "relations" in result

async def test_search_case_sensitivity(connection_pool, populated_db):
    """Test search case sensitivity."""
    search_ops = SearchOperations(connection_pool)
    
    # Search with different cases should return same results
    result1 = await search_ops.search_nodes("TEST_ENTITY")
    result2 = await search_ops.search_nodes("test_entity")
    assert result1 == result2

async def test_get_relations_for_entities(connection_pool, populated_db):
    """Test internal helper method for getting relations."""
    search_ops = SearchOperations(connection_pool)
    
    async with connection_pool.get_connection() as conn:
        relations = await search_ops._get_relations_for_entities(
            conn, 
            {"test_entity_1", "test_entity_2"}
        )
    
    assert len(relations) > 0
    assert all(isinstance(r, dict) for r in relations)
    assert all(
        r["from_"] in ["test_entity_1", "test_entity_2"] or 
        r["to"] in ["test_entity_1", "test_entity_2"] 
        for r in relations
    )
