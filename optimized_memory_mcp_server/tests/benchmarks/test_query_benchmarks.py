"""Benchmark tests for query performance."""
import pytest
import asyncio
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
import json
import psutil
import os

async def test_query_performance(connection_pool, populated_db):
    """Benchmark basic query performance."""
    search_ops = connection_pool.search_ops
    
    # Measure search query performance
    start_time = time.time()
    for _ in range(100):
        await search_ops.search_nodes("test_entity")
    query_time = (time.time() - start_time) / 100
    
    # Assert reasonable performance
    assert query_time < 0.01  # Average query should complete within 10ms

async def test_cache_effectiveness(connection_pool, populated_db):
    """Benchmark cache hit rates and performance."""
    start_mem = psutil.Process(os.getpid()).memory_info().rss
    
    # Perform repeated queries to test cache
    for _ in range(50):
        await connection_pool.execute_cached(
            await connection_pool.get_connection().__aenter__(),
            "SELECT * FROM entities WHERE name LIKE ?",
            ("%test%",)
        )
    
    # Check cache metrics
    metrics = connection_pool.metrics.query_metrics
    assert metrics["unknown"].cache_hits > 0
    assert metrics["unknown"].cache_hit_rate > 0.8  # Expect >80% cache hit rate
    
    # Check memory usage
    end_mem = psutil.Process(os.getpid()).memory_info().rss
    mem_increase = (end_mem - start_mem) / 1024 / 1024  # MB
    assert mem_increase < 10  # Cache should use less than 10MB

async def test_concurrent_operations(connection_pool, populated_db):
    """Benchmark concurrent operation handling."""
    async def concurrent_query():
        async with connection_pool.get_connection() as conn:
            await conn.execute("SELECT * FROM entities")
    
    # Run multiple concurrent operations
    start_time = time.time()
    tasks = [concurrent_query() for _ in range(20)]
    await asyncio.gather(*tasks)
    total_time = time.time() - start_time
    
    # Check concurrent performance
    assert total_time < 1.0  # Should handle 20 concurrent queries within 1 second
    
    # Verify connection pool behavior
    assert connection_pool._pool_semaphore._value >= 0
    assert len(connection_pool._pool) <= connection_pool._pool_size

async def test_write_performance(connection_pool):
    """Benchmark write operation performance."""
    entity_ops = connection_pool.entity_ops
    
    # Prepare test data
    entities = [
        {
            "name": f"bench_entity_{i}",
            "entityType": "benchmark",
            "observations": [f"obs_{i}"],
            "confidence_score": 0.9,
            "metadata": {"benchmark": True}
        }
        for i in range(1000)
    ]
    
    # Measure batch insert performance
    start_time = time.time()
    await entity_ops.create_entities(entities, batch_size=100)
    write_time = time.time() - start_time
    
    # Assert reasonable write performance
    assert write_time < 5.0  # Should insert 1000 entities within 5 seconds

async def test_memory_usage_under_load(connection_pool):
    """Benchmark memory usage under load."""
    process = psutil.Process(os.getpid())
    initial_mem = process.memory_info().rss
    
    # Generate load
    large_data = [
        {
            "name": f"mem_test_{i}",
            "entityType": "memory_test",
            "observations": [f"obs_{j}" for j in range(100)],
            "metadata": {"large": "x" * 1000}
        }
        for i in range(1000)
    ]
    
    # Monitor memory during operations
    peak_mem = initial_mem
    async with connection_pool.get_connection() as conn:
        for chunk in [large_data[i:i+100] for i in range(0, len(large_data), 100)]:
            await conn.executemany(
                """
                INSERT INTO entities (name, entity_type, observations, metadata)
                VALUES (?, ?, ?, ?)
                """,
                [(e["name"], e["entityType"], 
                  ",".join(e["observations"]), 
                  json.dumps(e["metadata"])) for e in chunk]
            )
            current_mem = process.memory_info().rss
            peak_mem = max(peak_mem, current_mem)
    
    # Check memory usage
    mem_increase = (peak_mem - initial_mem) / 1024 / 1024  # MB
    assert mem_increase < 100  # Peak memory increase should be less than 100MB
