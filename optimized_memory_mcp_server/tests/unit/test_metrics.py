"""Unit tests for metrics collection functionality."""
import pytest
from datetime import datetime, timedelta
from ....monitoring.metrics import MetricsCollector, HealthCheck

@pytest.fixture
def metrics_collector():
    """Create a fresh metrics collector for each test."""
    return MetricsCollector()

@pytest.fixture
def health_check(connection_pool):
    """Create a health check instance with connection pool."""
    return HealthCheck(connection_pool)

async def test_query_metrics_recording(metrics_collector):
    """Test recording and retrieving query metrics."""
    # Record some sample queries
    metrics_collector.record_query("select", 0.1, cache_hit=True)
    metrics_collector.record_query("select", 0.2, cache_hit=False)
    metrics_collector.record_query("insert", 0.3, cache_hit=False)
    
    # Check query counts
    assert metrics_collector.get_query_count("select") == 2
    assert metrics_collector.get_query_count("insert") == 1
    
    # Check average durations
    assert 0.15 == pytest.approx(metrics_collector.get_avg_duration("select"))
    assert 0.3 == pytest.approx(metrics_collector.get_avg_duration("insert"))
    
    # Check cache hit rates
    assert 0.5 == pytest.approx(metrics_collector.get_cache_hit_rate("select"))
    assert 0.0 == pytest.approx(metrics_collector.get_cache_hit_rate("insert"))

async def test_connection_metrics(metrics_collector):
    """Test connection pool metrics tracking."""
    metrics_collector.update_connection_metrics(active=3, total=5)
    
    assert metrics_collector.get_active_connections() == 3
    assert metrics_collector.get_total_connections() == 5
    assert 0.6 == pytest.approx(metrics_collector.get_connection_utilization())

async def test_percentile_calculations(metrics_collector):
    """Test calculation of duration percentiles."""
    durations = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    for d in durations:
        metrics_collector.record_query("select", d, cache_hit=False)
        
    assert 0.5 == pytest.approx(metrics_collector.get_percentile_duration("select", 50))
    assert 0.9 == pytest.approx(metrics_collector.get_percentile_duration("select", 90))
    assert 1.0 == pytest.approx(metrics_collector.get_percentile_duration("select", 100))

async def test_metrics_cleanup(metrics_collector):
    """Test cleanup of old metrics data."""
    # Record metrics with old timestamps
    old_time = datetime.now() - timedelta(hours=2)
    metrics_collector.query_metrics["select"] = {
        "durations": [0.1, 0.2],
        "cache_hits": 1,
        "total_queries": 2,
        "timestamps": [old_time, old_time]
    }
    
    # Record a new metric
    metrics_collector.record_query("select", 0.3, cache_hit=True)
    
    # Cleanup metrics older than 1 hour
    metrics_collector.cleanup(max_age=timedelta(hours=1))
    
    # Check that old metrics were removed
    assert len(metrics_collector.query_metrics["select"]["durations"]) == 1
    assert metrics_collector.query_metrics["select"]["durations"][0] == 0.3
    assert metrics_collector.query_metrics["select"]["cache_hits"] == 1
    assert metrics_collector.query_metrics["select"]["total_queries"] == 1

async def test_health_check(health_check, connection_pool):
    """Test database health check functionality."""
    # Test basic connectivity
    is_healthy = await health_check.check_health()
    assert is_healthy is True
    
    # Test health metrics
    metrics = await health_check.get_health_metrics()
    assert "database_size" in metrics
    assert "connection_pool_status" in metrics
    assert "last_successful_query" in metrics
    
    # Test connection pool status
    pool_status = await health_check.check_connection_pool()
    assert pool_status["total_connections"] > 0
    assert pool_status["active_connections"] >= 0
    assert "pool_utilization" in pool_status

async def test_query_timing_distribution(metrics_collector):
    """Test query timing distribution metrics."""
    durations = [0.1, 0.2, 0.3, 0.4, 0.5]
    for d in durations:
        metrics_collector.record_query("select", d, cache_hit=False)
    
    distribution = metrics_collector.get_query_timing_distribution("select")
    assert len(distribution) > 0
    assert sum(distribution.values()) == len(durations)
    
    # Test distribution boundaries
    assert min(distribution.keys()) <= 0.1
    assert max(distribution.keys()) >= 0.5

async def test_cache_efficiency_metrics(metrics_collector):
    """Test cache efficiency metrics calculation."""
    # Record mix of cache hits and misses
    for _ in range(3):
        metrics_collector.record_query("select", 0.1, cache_hit=True)
    for _ in range(2):
        metrics_collector.record_query("select", 0.2, cache_hit=False)
        
    efficiency = metrics_collector.get_cache_efficiency_metrics("select")
    assert efficiency["hit_rate"] == pytest.approx(0.6)
    assert efficiency["avg_hit_duration"] == pytest.approx(0.1)
    assert efficiency["avg_miss_duration"] == pytest.approx(0.2)
