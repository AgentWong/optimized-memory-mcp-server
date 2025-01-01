"""Temporal schema definitions for SQLite backend."""

# Version tracking table for entities
ENTITY_VERSIONS_TABLE = """
CREATE TABLE IF NOT EXISTS entity_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_name TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    observations TEXT,
    confidence_score FLOAT,
    context_source TEXT,
    metadata JSON,
    version_number INTEGER NOT NULL,
    valid_from TIMESTAMP NOT NULL,
    valid_until TIMESTAMP,
    change_type TEXT NOT NULL,  -- 'create', 'update', 'delete'
    change_reason TEXT,
    FOREIGN KEY (entity_name) REFERENCES entities(name) ON DELETE CASCADE
)
"""

# Version tracking for relations
RELATION_VERSIONS_TABLE = """
CREATE TABLE IF NOT EXISTS relation_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_entity TEXT NOT NULL,
    to_entity TEXT NOT NULL,
    relation_type TEXT NOT NULL,
    confidence_score FLOAT,
    context_source TEXT,
    version_number INTEGER NOT NULL,
    valid_from TIMESTAMP NOT NULL,
    valid_until TIMESTAMP,
    change_type TEXT NOT NULL,  -- 'create', 'update', 'delete'
    change_reason TEXT,
    FOREIGN KEY (from_entity) REFERENCES entities(name) ON DELETE CASCADE,
    FOREIGN KEY (to_entity) REFERENCES entities(name) ON DELETE CASCADE
)
"""

# Temporal indices
TEMPORAL_INDICES = [
    "CREATE INDEX IF NOT EXISTS idx_entity_versions_name ON entity_versions(entity_name)",
    "CREATE INDEX IF NOT EXISTS idx_entity_versions_time ON entity_versions(valid_from, valid_until)",
    "CREATE INDEX IF NOT EXISTS idx_relation_versions_entities ON relation_versions(from_entity, to_entity)",
    "CREATE INDEX IF NOT EXISTS idx_relation_versions_time ON relation_versions(valid_from, valid_until)"
]

# Triggers for version tracking
TEMPORAL_TRIGGERS = [
    # Entity version tracking
    """
    CREATE TRIGGER IF NOT EXISTS track_entity_versions_insert
    AFTER INSERT ON entities
    BEGIN
        INSERT INTO entity_versions (
            entity_name, entity_type, observations, confidence_score,
            context_source, metadata, version_number, valid_from,
            change_type, change_reason
        )
        VALUES (
            NEW.name, NEW.entity_type, NEW.observations, NEW.confidence_score,
            NEW.context_source, NEW.metadata, 1, CURRENT_TIMESTAMP,
            'create', 'Initial creation'
        );
    END
    """,
    
    """
    CREATE TRIGGER IF NOT EXISTS track_entity_versions_update
    AFTER UPDATE ON entities
    BEGIN
        UPDATE entity_versions 
        SET valid_until = CURRENT_TIMESTAMP
        WHERE entity_name = NEW.name AND valid_until IS NULL;
        
        INSERT INTO entity_versions (
            entity_name, entity_type, observations, confidence_score,
            context_source, metadata, version_number, valid_from,
            change_type, change_reason
        )
        SELECT 
            NEW.name, NEW.entity_type, NEW.observations, NEW.confidence_score,
            NEW.context_source, NEW.metadata,
            COALESCE(MAX(version_number), 0) + 1,
            CURRENT_TIMESTAMP,
            'update',
            'Entity updated'
        FROM entity_versions
        WHERE entity_name = NEW.name;
    END
    """,
    
    """
    CREATE TRIGGER IF NOT EXISTS track_entity_versions_delete
    AFTER DELETE ON entities
    BEGIN
        UPDATE entity_versions 
        SET valid_until = CURRENT_TIMESTAMP
        WHERE entity_name = OLD.name AND valid_until IS NULL;
        
        INSERT INTO entity_versions (
            entity_name, entity_type, observations, confidence_score,
            context_source, metadata, version_number, valid_from,
            change_type, change_reason
        )
        SELECT 
            OLD.name, OLD.entity_type, OLD.observations, OLD.confidence_score,
            OLD.context_source, OLD.metadata,
            COALESCE(MAX(version_number), 0) + 1,
            CURRENT_TIMESTAMP,
            'delete',
            'Entity deleted'
        FROM entity_versions
        WHERE entity_name = OLD.name;
    END
    """,
    
    # Relation version tracking
    """
    CREATE TRIGGER IF NOT EXISTS track_relation_versions_insert
    AFTER INSERT ON relations
    BEGIN
        INSERT INTO relation_versions (
            from_entity, to_entity, relation_type, confidence_score,
            context_source, version_number, valid_from,
            change_type, change_reason
        )
        VALUES (
            NEW.from_entity, NEW.to_entity, NEW.relation_type, NEW.confidence_score,
            NEW.context_source, 1, CURRENT_TIMESTAMP,
            'create', 'Initial creation'
        );
    END
    """,
    
    """
    CREATE TRIGGER IF NOT EXISTS track_relation_versions_delete
    AFTER DELETE ON relations
    BEGIN
        UPDATE relation_versions 
        SET valid_until = CURRENT_TIMESTAMP
        WHERE from_entity = OLD.from_entity 
        AND to_entity = OLD.to_entity
        AND relation_type = OLD.relation_type
        AND valid_until IS NULL;
        
        INSERT INTO relation_versions (
            from_entity, to_entity, relation_type, confidence_score,
            context_source, version_number, valid_from,
            change_type, change_reason
        )
        SELECT 
            OLD.from_entity, OLD.to_entity, OLD.relation_type, OLD.confidence_score,
            OLD.context_source,
            COALESCE(MAX(version_number), 0) + 1,
            CURRENT_TIMESTAMP,
            'delete',
            'Relation deleted'
        FROM relation_versions
        WHERE from_entity = OLD.from_entity
        AND to_entity = OLD.to_entity
        AND relation_type = OLD.relation_type;
    END
    """
]
