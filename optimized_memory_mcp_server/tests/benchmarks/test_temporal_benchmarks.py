"""Benchmark tests for temporal operations."""
import pytest
import time
from datetime import datetime, timedelta
import json
import asyncio
import psutil
import os

async def test_temporal_query_performance(connection_pool, populated_db):
    """Benchmark temporal query performance."""
    temporal_ops = connection_pool.temporal_ops
    
    # Create historical data
    async with connection_pool.get_connection() as conn:
        base_time = datetime.now() - timedelta(days=365)
        for i in range(100):
            timestamp = base_time + timedelta(days=i)
            await conn.execute(
                """
                INSERT INTO entity_changes (
                    entity_name, changed_at, change_type, entity_state
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    "temporal_bench",
                    timestamp.isoformat(),
                    "update",
                    json.dumps({"version": i})
                )
            )
    
    # Measure query performance
    start_time = time.time()
    for _ in range(50):
        point_in_time = base_time + timedelta(days=180)
        await temporal_ops.get_entity_state_at(
            "temporal_bench",
            point_in_time
        )
    query_time = (time.time() - start_time) / 50
    
    assert query_time < 0.01  # Average temporal query within 10ms

async def test_temporal_range_query_performance(connection_pool, populated_db):
    """Benchmark temporal range query performance."""
    temporal_ops = connection_pool.temporal_ops
    
    start_time = time.time()
    for _ in range(20):
        await temporal_ops.get_change_summary(
            "test_entity_1",
            datetime.now() - timedelta(days=30),
            datetime.now()
        )
    query_time = (time.time() - start_time) / 20
    
    assert query_time < 0.05  # Range queries within 50ms

async def test_temporal_concurrent_access(connection_pool, populated_db):
    """Benchmark concurrent temporal operations."""
    temporal_ops = connection_pool.temporal_ops
    
    async def concurrent_temporal_query(entity_name: str):
        for _ in range(10):
            await temporal_ops.get_entity_changes(entity_name)
    
    # Run concurrent temporal queries
    start_time = time.time()
    tasks = [
        concurrent_temporal_query(f"test_entity_{i}")
        for i in range(10)
    ]
    await asyncio.gather(*tasks)
    total_time = time.time() - start_time
    
    assert total_time < 2.0  # 100 concurrent temporal queries within 2 seconds

async def test_temporal_memory_usage(connection_pool):
    """Benchmark memory usage during temporal operations."""
    process = psutil.Process(os.getpid())
    initial_mem = process.memory_info().rss
    
    # Generate temporal data
    async with connection_pool.get_connection() as conn:
        for i in range(1000):
            await conn.execute(
                """
                INSERT INTO entity_changes (
                    entity_name, changed_at, change_type, entity_state
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    f"mem_test_{i % 10}",
                    datetime.now().isoformat(),
                    "update",
                    json.dumps({"data": "x" * 1000})
                )
            )
    
    # Measure memory during queries
    peak_mem = initial_mem
    temporal_ops = connection_pool.temporal_ops
    
    for i in range(10):
        await temporal_ops.get_entity_changes(f"mem_test_{i}")
        current_mem = process.memory_info().rss
        peak_mem = max(peak_mem, current_mem)
    
    mem_increase = (peak_mem - initial_mem) / 1024 / 1024  # MB
    assert mem_increase < 50  # Peak memory increase under 50MB
