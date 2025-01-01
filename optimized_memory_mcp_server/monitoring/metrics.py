import time
from dataclasses import dataclass
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import asyncio
import logging
from collections import deque

logger = logging.getLogger(__name__)

@dataclass
class QueryMetrics:
    query_count: int = 0
    total_duration: float = 0.0
    cache_hits: int = 0
    cache_misses: int = 0
    
    @property
    def avg_duration(self) -> float:
        return self.total_duration / self.query_count if self.query_count > 0 else 0.0
    
    @property
    def cache_hit_rate(self) -> float:
        total = self.cache_hits + self.cache_misses
        return self.cache_hits / total if total > 0 else 0.0

class MetricsCollector:
    def __init__(self, window_size: int = 3600):  # 1 hour default
        self.window_size = window_size
        self.query_metrics: Dict[str, QueryMetrics] = {}
        self.query_times: Dict[str, deque] = {}  # Rolling window of query times
        self.connection_metrics = {
            'active_connections': 0,
            'total_connections': 0,
            'connection_timeouts': 0,
            'peak_connections': 0
        }
        self.last_cleanup = time.time()
        
    def record_query(self, query_type: str, duration: float, cache_hit: bool = False):
        """Record metrics for a query."""
        if query_type not in self.query_metrics:
            self.query_metrics[query_type] = QueryMetrics()
            self.query_times[query_type] = deque(maxlen=1000)
            
        metrics = self.query_metrics[query_type]
        metrics.query_count += 1
        metrics.total_duration += duration
        
        if cache_hit:
            metrics.cache_hits += 1
        else:
            metrics.cache_misses += 1
            
        self.query_times[query_type].append((time.time(), duration))
        
    def update_connection_metrics(self, active: int, total: int = None):
        """Update connection pool metrics."""
        self.connection_metrics['active_connections'] = active
        if total is not None:
            self.connection_metrics['total_connections'] = total
        self.connection_metrics['peak_connections'] = max(
            self.connection_metrics['peak_connections'],
            active
        )
        
    def record_connection_timeout(self):
        """Record a connection timeout event."""
        self.connection_metrics['connection_timeouts'] += 1
        
    def get_metrics(self) -> Dict:
        """Get current metrics."""
        self._cleanup_old_metrics()
        
        return {
            'queries': {
                qtype: {
                    'count': metrics.query_count,
                    'avg_duration': metrics.avg_duration,
                    'cache_hit_rate': metrics.cache_hit_rate,
                    'p95_duration': self._calculate_percentile(qtype, 95),
                    'p99_duration': self._calculate_percentile(qtype, 99)
                }
                for qtype, metrics in self.query_metrics.items()
            },
            'connections': self.connection_metrics
        }
        
    def _calculate_percentile(self, query_type: str, percentile: float) -> float:
        """Calculate duration percentile for a query type."""
        if query_type not in self.query_times:
            return 0.0
            
        times = sorted(t[1] for t in self.query_times[query_type])
        if not times:
            return 0.0
            
        idx = int(len(times) * (percentile / 100))
        return times[idx]
        
    def _cleanup_old_metrics(self):
        """Remove metrics outside the window."""
        current_time = time.time()
        if current_time - self.last_cleanup < 60:  # Only cleanup every minute
            return
            
        cutoff = current_time - self.window_size
        for query_type in self.query_times:
            while (self.query_times[query_type] and 
                   self.query_times[query_type][0][0] < cutoff):
                self.query_times[query_type].popleft()
                
        self.last_cleanup = current_time

class HealthCheck:
    def __init__(self, pool):
        self.pool = pool
        
    async def check_database(self) -> Dict[str, any]:
        """Perform database health check."""
        try:
            start_time = time.time()
            async with self.pool.get_connection() as conn:
                await conn.execute("SELECT 1")
                response_time = time.time() - start_time
                
                # Check WAL file size
                cursor = await conn.execute("PRAGMA wal_checkpoint")
                wal_info = await cursor.fetchone()
                
                # Check database size
                cursor = await conn.execute("PRAGMA page_count")
                page_count = (await cursor.fetchone())[0]
                cursor = await conn.execute("PRAGMA page_size")
                page_size = (await cursor.fetchone())[0]
                db_size = page_count * page_size / (1024 * 1024)  # Size in MB
                
                return {
                    'status': 'healthy',
                    'response_time': response_time,
                    'database_size_mb': db_size,
                    'wal_info': dict(zip(['busy', 'log', 'checkpointed'], wal_info)),
                    'connections': {
                        'active': self.pool._pool_semaphore._value,
                        'total': self.pool._pool_size
                    }
                }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                'status': 'unhealthy',
                'error': str(e)
            }
