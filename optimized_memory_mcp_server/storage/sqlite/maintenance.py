"""Maintenance operations for SQLite backend."""
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Optional
import aiosqlite

logger = logging.getLogger(__name__)

class MaintenanceManager:
    """Handles database maintenance tasks."""
    
    def __init__(self, pool, refresh_interval: int = 3600):
        self.pool = pool
        self.refresh_interval = refresh_interval
        self._last_refresh: Optional[datetime] = None
        self._maintenance_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start periodic maintenance."""
        self._maintenance_task = asyncio.create_task(self._maintenance_loop())
        
    async def stop(self):
        """Stop periodic maintenance."""
        if self._maintenance_task:
            self._maintenance_task.cancel()
            try:
                await self._maintenance_task
            except asyncio.CancelledError:
                pass
            self._maintenance_task = None
            
    async def _maintenance_loop(self):
        """Periodic maintenance loop."""
        while True:
            try:
                await self.perform_maintenance()
                await asyncio.sleep(self.refresh_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Maintenance error: {e}")
                await asyncio.sleep(60)  # Wait before retry
                
    async def perform_maintenance(self):
        """Perform all maintenance tasks."""
        async with self.pool.get_connection() as conn:
            async with self.pool.transaction(conn):
                await self._refresh_materialized_views(conn)
                await self._maintain_partitions(conn)
                await self._optimize_database(conn)
                
    async def _refresh_materialized_views(self, conn: aiosqlite.Connection):
        """Refresh materialized views."""
        logger.info("Refreshing materialized views")
        await conn.execute("DELETE FROM mv_entity_stats")
        await conn.execute("""
            INSERT INTO mv_entity_stats
            SELECT 
                entity_type,
                COUNT(*) as count,
                AVG(confidence_score) as avg_confidence,
                MIN(created_at) as oldest_entry,
                MAX(created_at) as newest_entry
            FROM (
                SELECT * FROM entities_recent
                UNION ALL
                SELECT * FROM entities_intermediate
                UNION ALL
                SELECT * FROM entities_archive
            )
            GROUP BY entity_type
        """)
        
        await conn.execute("DELETE FROM mv_relation_summary")
        await conn.execute("""
            INSERT INTO mv_relation_summary
            SELECT 
                relation_type,
                COUNT(*) as count,
                AVG(confidence_score) as avg_confidence,
                COUNT(DISTINCT from_entity) as unique_sources,
                COUNT(DISTINCT to_entity) as unique_targets
            FROM relations
            GROUP BY relation_type
        """)
        
    async def _maintain_partitions(self, conn: aiosqlite.Connection):
        """Maintain table partitions."""
        logger.info("Maintaining partitions")
        # Move old records to appropriate partitions
        await conn.execute("""
            INSERT INTO entities_intermediate
            SELECT * FROM entities_recent 
            WHERE created_at < date('now', '-30 days')
        """)
        await conn.execute("""
            DELETE FROM entities_recent 
            WHERE created_at < date('now', '-30 days')
        """)
        
        await conn.execute("""
            INSERT INTO entities_archive
            SELECT * FROM entities_intermediate 
            WHERE created_at < date('now', '-180 days')
        """)
        await conn.execute("""
            DELETE FROM entities_intermediate 
            WHERE created_at < date('now', '-180 days')
        """)
        
    async def _optimize_database(self, conn: aiosqlite.Connection):
        """Perform database optimization."""
        logger.info("Optimizing database")
        await conn.execute("PRAGMA optimize")
        await conn.execute("ANALYZE")
