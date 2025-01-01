"""Table partitioning definitions for SQLite."""

# Partitioned tables for entities based on time ranges
PARTITIONED_TABLES = [
    """
    CREATE TABLE IF NOT EXISTS entities_recent (
        name TEXT PRIMARY KEY,
        entity_type TEXT NOT NULL,
        observations TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        confidence_score FLOAT DEFAULT 1.0,
        context_source TEXT,
        metadata JSON,
        category_id INTEGER,
        CHECK (created_at >= date('now', '-30 days')),
        FOREIGN KEY (category_id) REFERENCES knowledge_categories(id)
    )
    """,
    
    """
    CREATE TABLE IF NOT EXISTS entities_intermediate (
        name TEXT PRIMARY KEY,
        entity_type TEXT NOT NULL,
        observations TEXT,
        created_at TIMESTAMP NOT NULL,
        last_updated TIMESTAMP NOT NULL,
        confidence_score FLOAT DEFAULT 1.0,
        context_source TEXT,
        metadata JSON,
        category_id INTEGER,
        CHECK (created_at >= date('now', '-180 days') AND created_at < date('now', '-30 days')),
        FOREIGN KEY (category_id) REFERENCES knowledge_categories(id)
    )
    """,
    
    """
    CREATE TABLE IF NOT EXISTS entities_archive (
        name TEXT PRIMARY KEY,
        entity_type TEXT NOT NULL,
        observations TEXT,
        created_at TIMESTAMP NOT NULL,
        last_updated TIMESTAMP NOT NULL,
        confidence_score FLOAT DEFAULT 1.0,
        context_source TEXT,
        metadata JSON,
        category_id INTEGER,
        CHECK (created_at < date('now', '-180 days')),
        FOREIGN KEY (category_id) REFERENCES knowledge_categories(id)
    )
    """
]

# Materialized views for common queries
MATERIALIZED_VIEWS = [
    """
    CREATE VIEW IF NOT EXISTS mv_entity_stats AS
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
    """,
    
    """
    CREATE VIEW IF NOT EXISTS mv_relation_summary AS
    SELECT 
        relation_type,
        COUNT(*) as count,
        AVG(confidence_score) as avg_confidence,
        COUNT(DISTINCT from_entity) as unique_sources,
        COUNT(DISTINCT to_entity) as unique_targets
    FROM relations
    GROUP BY relation_type
    """
]

# Triggers for partition maintenance
PARTITION_TRIGGERS = [
    """
    CREATE TRIGGER IF NOT EXISTS move_to_intermediate
    AFTER UPDATE ON entities_recent
    WHEN OLD.created_at < date('now', '-30 days')
    BEGIN
        INSERT INTO entities_intermediate 
        SELECT * FROM entities_recent WHERE name = OLD.name;
        DELETE FROM entities_recent WHERE name = OLD.name;
    END
    """,
    
    """
    CREATE TRIGGER IF NOT EXISTS move_to_archive
    AFTER UPDATE ON entities_intermediate
    WHEN OLD.created_at < date('now', '-180 days')
    BEGIN
        INSERT INTO entities_archive 
        SELECT * FROM entities_intermediate WHERE name = OLD.name;
        DELETE FROM entities_intermediate WHERE name = OLD.name;
    END
    """
]

# Indices for partitioned tables
PARTITION_INDICES = [
    "CREATE INDEX IF NOT EXISTS idx_recent_type ON entities_recent(entity_type)",
    "CREATE INDEX IF NOT EXISTS idx_recent_created ON entities_recent(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_intermediate_type ON entities_intermediate(entity_type)",
    "CREATE INDEX IF NOT EXISTS idx_intermediate_created ON entities_intermediate(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_archive_type ON entities_archive(entity_type)",
    "CREATE INDEX IF NOT EXISTS idx_archive_created ON entities_archive(created_at)"
]
