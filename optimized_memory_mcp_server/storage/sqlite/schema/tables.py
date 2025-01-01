"""SQLite database schema definitions."""

SCHEMA_VERSION = 1

# Core table definitions
ENTITIES_TABLE = """
CREATE TABLE IF NOT EXISTS entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    entity_type TEXT NOT NULL,
    observations TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    confidence_score FLOAT DEFAULT 1.0 CHECK (confidence_score >= 0.0 AND confidence_score <= 1.0),
    context_source TEXT,
    metadata JSON,
    category_id INTEGER,
    FOREIGN KEY (category_id) REFERENCES knowledge_categories(id)
)
"""

RELATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS relations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_entity TEXT NOT NULL,
    to_entity TEXT NOT NULL,
    relation_type TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_until TIMESTAMP,
    confidence_score FLOAT DEFAULT 1.0 CHECK (confidence_score >= 0.0 AND confidence_score <= 1.0),
    context_source TEXT,
    FOREIGN KEY (from_entity) REFERENCES entities(name) ON DELETE CASCADE,
    FOREIGN KEY (to_entity) REFERENCES entities(name) ON DELETE CASCADE
)
"""

KNOWLEDGE_CATEGORIES_TABLE = """
CREATE TABLE IF NOT EXISTS knowledge_categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    priority INTEGER CHECK (priority >= 1 AND priority <= 5),
    retention_period INTEGER  -- in days
)
"""

CLOUD_RESOURCES_TABLE = """
CREATE TABLE IF NOT EXISTS cloud_resources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    resource_id TEXT UNIQUE NOT NULL,
    resource_type TEXT NOT NULL,
    region TEXT,
    account_id TEXT,
    metadata JSON,
    entity_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (entity_id) REFERENCES entities(id)
)
"""

# Index definitions
INDICES = [
    "CREATE INDEX IF NOT EXISTS idx_entities_type_created ON entities(entity_type, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_entities_type_confidence ON entities(entity_type, confidence_score)",
    "CREATE INDEX IF NOT EXISTS idx_relations_from_type ON relations(from_entity, relation_type)",
    "CREATE INDEX IF NOT EXISTS idx_cloud_resources_type ON cloud_resources(resource_type)",
    "CREATE INDEX IF NOT EXISTS idx_entities_category ON entities(category_id)",
]

# Trigger for last_updated
TRIGGERS = [
    """
    CREATE TRIGGER IF NOT EXISTS update_entities_timestamp 
    AFTER UPDATE ON entities
    BEGIN
        UPDATE entities SET last_updated = CURRENT_TIMESTAMP
        WHERE id = NEW.id;
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS update_cloud_resources_timestamp
    AFTER UPDATE ON cloud_resources
    BEGIN
        UPDATE cloud_resources SET last_updated = CURRENT_TIMESTAMP
        WHERE id = NEW.id;
    END
    """
]
