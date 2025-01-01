import aiosqlite
from typing import List
from datetime import datetime
from .schema.partitions import PARTITIONED_TABLES, MATERIALIZED_VIEWS, PARTITION_TRIGGERS, PARTITION_INDICES

SCHEMA_STATEMENTS = [
    # Enable SQLite features
    "PRAGMA journal_mode=WAL",
    "PRAGMA synchronous=NORMAL",
    "PRAGMA temp_store=MEMORY",
    # Enable SQLite Window Functions
    "PRAGMA enable_window_functions = ON",
    
    # Knowledge Categories table
    """
    CREATE TABLE IF NOT EXISTS knowledge_categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        priority INTEGER CHECK (priority >= 1 AND priority <= 5),
        retention_period INTEGER  -- in days
    )
    """,
    
    # Updated Entities table
    """
    CREATE TABLE IF NOT EXISTS entities (
        name TEXT PRIMARY KEY,
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
    """,
    
    # Updated Relations table
    """
    CREATE TABLE IF NOT EXISTS relations (
        from_entity TEXT NOT NULL,
        to_entity TEXT NOT NULL,
        relation_type TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        valid_until TIMESTAMP,
        confidence_score FLOAT DEFAULT 1.0 CHECK (confidence_score >= 0.0 AND confidence_score <= 1.0),
        context_source TEXT,
        PRIMARY KEY (from_entity, to_entity, relation_type),
        FOREIGN KEY (from_entity) REFERENCES entities(name) ON DELETE CASCADE,
        FOREIGN KEY (to_entity) REFERENCES entities(name) ON DELETE CASCADE
    )
    """,
    
    # Partitioned Entities table by entity_type
    """
    CREATE TABLE IF NOT EXISTS entities_by_type (
        name TEXT,
        entity_type TEXT NOT NULL,
        observations TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        confidence_score FLOAT DEFAULT 1.0 CHECK (confidence_score >= 0.0 AND confidence_score <= 1.0),
        context_source TEXT,
        metadata JSON,
        category_id INTEGER,
        PRIMARY KEY (entity_type, name),
        FOREIGN KEY (category_id) REFERENCES knowledge_categories(id)
    ) WITHOUT ROWID
    """,

    # Materialized view for high confidence entities
    """
    CREATE VIEW IF NOT EXISTS high_confidence_entities AS
    SELECT * FROM entities 
    WHERE confidence_score >= 0.8
    """,

    # Materialized view for recent relations
    """
    CREATE VIEW IF NOT EXISTS recent_relations AS
    SELECT r.*, e1.entity_type as from_type, e2.entity_type as to_type
    FROM relations r
    JOIN entities e1 ON r.from_entity = e1.name
    JOIN entities e2 ON r.to_entity = e2.name
    WHERE r.created_at >= date('now', '-30 days')
    """,

    # Cloud Resources table
    """
    CREATE TABLE IF NOT EXISTS cloud_resources (
        resource_id TEXT PRIMARY KEY,
        resource_type TEXT NOT NULL,
        region TEXT,
        account_id TEXT,
        metadata JSON,
        entity_name TEXT,
        tags JSON,
        last_synced TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (entity_name) REFERENCES entities(name) ON DELETE CASCADE
    )
    """,

    # Terraform State table
    """
    CREATE TABLE IF NOT EXISTS terraform_states (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        workspace TEXT NOT NULL,
        state_file TEXT NOT NULL,
        last_synced TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        metadata JSON,
        UNIQUE(workspace, state_file)
    )
    """,

    # Terraform Resources table
    """
    CREATE TABLE IF NOT EXISTS terraform_resources (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        resource_id TEXT NOT NULL,
        resource_type TEXT NOT NULL,
        workspace TEXT NOT NULL,
        state_file TEXT NOT NULL,
        state JSON NOT NULL,
        cloud_resource_id TEXT,
        last_synced TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (cloud_resource_id) REFERENCES cloud_resources(resource_id),
        FOREIGN KEY (workspace, state_file) REFERENCES terraform_states(workspace, state_file),
        UNIQUE(resource_id, workspace, state_file)
    )
    """,
    
    # Indices
    "CREATE INDEX IF NOT EXISTS idx_entity_type ON entities(entity_type)",
    "CREATE INDEX IF NOT EXISTS idx_entities_type_created ON entities(entity_type, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_entities_type_confidence ON entities(entity_type, confidence_score)",
    "CREATE INDEX IF NOT EXISTS idx_entities_category ON entities(category_id)",
    "CREATE INDEX IF NOT EXISTS idx_from_entity ON relations(from_entity)",
    "CREATE INDEX IF NOT EXISTS idx_to_entity ON relations(to_entity)",
    "CREATE INDEX IF NOT EXISTS idx_relations_from_type ON relations(from_entity, relation_type)",
    "CREATE INDEX IF NOT EXISTS idx_cloud_resources_type ON cloud_resources(resource_type)",
    "CREATE INDEX IF NOT EXISTS idx_terraform_resources_type ON terraform_resources(resource_type)",
    "CREATE INDEX IF NOT EXISTS idx_terraform_resources_cloud ON terraform_resources(cloud_resource_id)",
    
    # Triggers for last_updated timestamps
    """
    CREATE TRIGGER IF NOT EXISTS update_entities_timestamp 
    AFTER UPDATE ON entities
    BEGIN
        UPDATE entities SET last_updated = CURRENT_TIMESTAMP
        WHERE name = NEW.name;
    END
    """,
    
    """
    CREATE TRIGGER IF NOT EXISTS update_cloud_resources_timestamp
    AFTER UPDATE ON cloud_resources
    BEGIN
        UPDATE cloud_resources SET last_updated = CURRENT_TIMESTAMP
        WHERE resource_id = NEW.resource_id;
    END
    """,

    # Ansible Playbooks table
    """
    CREATE TABLE IF NOT EXISTS ansible_playbooks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        playbook_path TEXT NOT NULL,
        inventory_path TEXT,
        last_run TIMESTAMP,
        metadata JSON,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(playbook_path, inventory_path)
    )
    """,

    # Ansible Runs table
    """
    CREATE TABLE IF NOT EXISTS ansible_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        playbook_id INTEGER NOT NULL,
        start_time TIMESTAMP NOT NULL,
        end_time TIMESTAMP,
        status TEXT NOT NULL,  -- 'running', 'completed', 'failed'
        host_count INTEGER,
        metadata JSON,
        FOREIGN KEY (playbook_id) REFERENCES ansible_playbooks(id)
    )
    """,

    # Ansible Tasks table
    """
    CREATE TABLE IF NOT EXISTS ansible_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL,
        task_name TEXT NOT NULL,
        host TEXT NOT NULL,
        status TEXT NOT NULL,  -- 'ok', 'changed', 'failed', 'skipped'
        start_time TIMESTAMP NOT NULL,
        end_time TIMESTAMP,
        result JSON,
        cloud_resource_id TEXT,
        FOREIGN KEY (run_id) REFERENCES ansible_runs(id),
        FOREIGN KEY (cloud_resource_id) REFERENCES cloud_resources(resource_id)
    )
    """,

    # Add indices
    "CREATE INDEX IF NOT EXISTS idx_ansible_runs_playbook ON ansible_runs(playbook_id)",
    "CREATE INDEX IF NOT EXISTS idx_ansible_tasks_run ON ansible_tasks(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_ansible_tasks_cloud ON ansible_tasks(cloud_resource_id)",

    # Add trigger for last_updated
    """
    CREATE TRIGGER IF NOT EXISTS update_ansible_playbooks_timestamp
    AFTER UPDATE ON ansible_playbooks
    BEGIN
        UPDATE ansible_playbooks SET last_updated = CURRENT_TIMESTAMP
        WHERE id = NEW.id;
    END
    """,

    # Conversations table
    """
    CREATE TABLE IF NOT EXISTS conversations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_message_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        metadata JSON,
        context_entities TEXT,  -- Comma-separated list of entity names
        relevance_score FLOAT DEFAULT 1.0 CHECK (relevance_score >= 0.0 AND relevance_score <= 1.0)
    )
    """,

    # Messages table
    """
    CREATE TABLE IF NOT EXISTS conversation_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        conversation_id INTEGER NOT NULL,
        message_type TEXT NOT NULL,  -- 'user', 'assistant', 'system'
        content TEXT NOT NULL,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        metadata JSON,
        entities_mentioned TEXT,  -- Comma-separated list of entity names
        relevance_score FLOAT DEFAULT 1.0 CHECK (relevance_score >= 0.0 AND relevance_score <= 1.0),
        FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
    )
    """,

    # Add indices
    "CREATE INDEX IF NOT EXISTS idx_conversations_time ON conversations(last_message_time)",
    "CREATE INDEX IF NOT EXISTS idx_messages_conversation ON conversation_messages(conversation_id)",
    "CREATE INDEX IF NOT EXISTS idx_messages_type ON conversation_messages(message_type)",

    # Add trigger for last_message_time
    """
    CREATE TRIGGER IF NOT EXISTS update_conversation_last_message
    AFTER INSERT ON conversation_messages
    BEGIN
        UPDATE conversations 
        SET last_message_time = NEW.timestamp
        WHERE id = NEW.conversation_id;
    END
    """,

    # Code Snippets table
    """
    CREATE TABLE IF NOT EXISTS code_snippets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        language TEXT NOT NULL,
        content TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        metadata JSON,
        tags TEXT,  -- Comma-separated tags
        entity_name TEXT,  -- Associated entity
        similarity_hash TEXT,  -- For pattern matching
        FOREIGN KEY (entity_name) REFERENCES entities(name) ON DELETE SET NULL
    )
    """,

    # Snippet Versions table for tracking changes
    """
    CREATE TABLE IF NOT EXISTS snippet_versions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        snippet_id INTEGER NOT NULL,
        content TEXT NOT NULL,
        version_number INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        commit_message TEXT,
        diff TEXT,  -- Stored diff from previous version
        FOREIGN KEY (snippet_id) REFERENCES code_snippets(id) ON DELETE CASCADE,
        UNIQUE(snippet_id, version_number)
    )
    """,

    # Snippet Patterns table for common patterns
    """
    CREATE TABLE IF NOT EXISTS snippet_patterns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pattern_name TEXT NOT NULL,
        pattern_type TEXT NOT NULL,  -- 'syntax', 'semantic', 'usage'
        pattern_hash TEXT NOT NULL,  -- Hash of the pattern
        description TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        occurrence_count INTEGER DEFAULT 1,
        UNIQUE(pattern_hash)
    )
    """,

    # Snippet Pattern Matches table
    """
    CREATE TABLE IF NOT EXISTS snippet_pattern_matches (
        snippet_id INTEGER NOT NULL,
        pattern_id INTEGER NOT NULL,
        match_location TEXT,  -- JSON array of line numbers/positions
        confidence_score FLOAT DEFAULT 1.0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (snippet_id, pattern_id),
        FOREIGN KEY (snippet_id) REFERENCES code_snippets(id) ON DELETE CASCADE,
        FOREIGN KEY (pattern_id) REFERENCES snippet_patterns(id) ON DELETE CASCADE
    )
    """,

    # Add indices
    "CREATE INDEX IF NOT EXISTS idx_snippets_language ON code_snippets(language)",
    "CREATE INDEX IF NOT EXISTS idx_snippets_entity ON code_snippets(entity_name)",
    "CREATE INDEX IF NOT EXISTS idx_snippets_similarity ON code_snippets(similarity_hash)",
    "CREATE INDEX IF NOT EXISTS idx_snippet_versions ON snippet_versions(snippet_id, version_number)",
    "CREATE INDEX IF NOT EXISTS idx_patterns_hash ON snippet_patterns(pattern_hash)",
    "CREATE INDEX IF NOT EXISTS idx_pattern_matches_pattern ON snippet_pattern_matches(pattern_id)",

    # Add trigger for last_updated
    """
    CREATE TRIGGER IF NOT EXISTS update_code_snippets_timestamp
    AFTER UPDATE ON code_snippets
    BEGIN
        UPDATE code_snippets SET last_updated = CURRENT_TIMESTAMP
        WHERE id = NEW.id;
    END
    """,

    # Add trigger for pattern last_seen
    """
    CREATE TRIGGER IF NOT EXISTS update_pattern_last_seen
    AFTER INSERT ON snippet_pattern_matches
    BEGIN
        UPDATE snippet_patterns 
        SET last_seen = CURRENT_TIMESTAMP,
            occurrence_count = occurrence_count + 1
        WHERE id = NEW.pattern_id;
    END
    """,

    # Entity Changes table for temporal tracking
    """
    CREATE TABLE IF NOT EXISTS entity_changes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        entity_name TEXT NOT NULL,
        changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        change_type TEXT NOT NULL,  -- 'create', 'update', 'delete'
        entity_state JSON NOT NULL,  -- Complete entity state at this point
        changed_by TEXT,  -- Optional user/process that made the change
        FOREIGN KEY (entity_name) REFERENCES entities(name) ON DELETE CASCADE
    )
    """,

    # Relation Changes table for temporal tracking
    """
    CREATE TABLE IF NOT EXISTS relation_changes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_entity TEXT NOT NULL,
        to_entity TEXT NOT NULL,
        relation_type TEXT NOT NULL,
        changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        change_type TEXT NOT NULL,  -- 'create', 'update', 'delete'
        relation_state JSON NOT NULL,  -- Complete relation state at this point
        changed_by TEXT,  -- Optional user/process that made the change
        FOREIGN KEY (from_entity) REFERENCES entities(name) ON DELETE CASCADE,
        FOREIGN KEY (to_entity) REFERENCES entities(name) ON DELETE CASCADE
    )
    """,

    # Temporal indices
    "CREATE INDEX IF NOT EXISTS idx_entity_changes_name ON entity_changes(entity_name)",
    "CREATE INDEX IF NOT EXISTS idx_entity_changes_time ON entity_changes(changed_at)",
    "CREATE INDEX IF NOT EXISTS idx_entity_changes_type ON entity_changes(change_type)",
    "CREATE INDEX IF NOT EXISTS idx_relation_changes_entities ON relation_changes(from_entity, to_entity)",
    "CREATE INDEX IF NOT EXISTS idx_relation_changes_time ON relation_changes(changed_at)",
    "CREATE INDEX IF NOT EXISTS idx_relation_changes_type ON relation_changes(change_type)",

    # Temporal change tracking triggers
    """
    CREATE TRIGGER IF NOT EXISTS track_entity_changes
    AFTER UPDATE ON entities
    BEGIN
        INSERT INTO entity_changes (
            entity_name, change_type, entity_state
        )
        VALUES (
            NEW.name,
            'update',
            json_object(
                'name', NEW.name,
                'entity_type', NEW.entity_type,
                'observations', NEW.observations,
                'confidence_score', NEW.confidence_score,
                'context_source', NEW.context_source,
                'metadata', NEW.metadata
            )
        );
    END
    """,

    """
    CREATE TRIGGER IF NOT EXISTS track_entity_creation
    AFTER INSERT ON entities
    BEGIN
        INSERT INTO entity_changes (
            entity_name, change_type, entity_state
        )
        VALUES (
            NEW.name,
            'create',
            json_object(
                'name', NEW.name,
                'entity_type', NEW.entity_type,
                'observations', NEW.observations,
                'confidence_score', NEW.confidence_score,
                'context_source', NEW.context_source,
                'metadata', NEW.metadata
            )
        );
    END
    """,

    """
    CREATE TRIGGER IF NOT EXISTS track_entity_deletion
    BEFORE DELETE ON entities
    BEGIN
        INSERT INTO entity_changes (
            entity_name, change_type, entity_state
        )
        VALUES (
            OLD.name,
            'delete',
            json_object(
                'name', OLD.name,
                'entity_type', OLD.entity_type,
                'observations', OLD.observations,
                'confidence_score', OLD.confidence_score,
                'context_source', OLD.context_source,
                'metadata', OLD.metadata
            )
        );
    END
    """,

    """
    CREATE TRIGGER IF NOT EXISTS track_relation_changes
    AFTER UPDATE ON relations
    BEGIN
        INSERT INTO relation_changes (
            from_entity, to_entity, relation_type,
            change_type, relation_state
        )
        VALUES (
            NEW.from_entity,
            NEW.to_entity,
            NEW.relation_type,
            'update',
            json_object(
                'from_entity', NEW.from_entity,
                'to_entity', NEW.to_entity,
                'relation_type', NEW.relation_type,
                'valid_from', NEW.valid_from,
                'valid_until', NEW.valid_until,
                'confidence_score', NEW.confidence_score,
                'context_source', NEW.context_source
            )
        );
    END
    """,

    """
    CREATE TRIGGER IF NOT EXISTS track_relation_creation
    AFTER INSERT ON relations
    BEGIN
        INSERT INTO relation_changes (
            from_entity, to_entity, relation_type,
            change_type, relation_state
        )
        VALUES (
            NEW.from_entity,
            NEW.to_entity,
            NEW.relation_type,
            'create',
            json_object(
                'from_entity', NEW.from_entity,
                'to_entity', NEW.to_entity,
                'relation_type', NEW.relation_type,
                'valid_from', NEW.valid_from,
                'valid_until', NEW.valid_until,
                'confidence_score', NEW.confidence_score,
                'context_source', NEW.context_source
            )
        );
    END
    """,

    """
    CREATE TRIGGER IF NOT EXISTS track_relation_deletion
    BEFORE DELETE ON relations
    BEGIN
        INSERT INTO relation_changes (
            from_entity, to_entity, relation_type,
            change_type, relation_state
        )
        VALUES (
            OLD.from_entity,
            OLD.to_entity,
            OLD.relation_type,
            'delete',
            json_object(
                'from_entity', OLD.from_entity,
                'to_entity', OLD.to_entity,
                'relation_type', OLD.relation_type,
                'valid_from', OLD.valid_from,
                'valid_until', OLD.valid_until,
                'confidence_score', OLD.confidence_score,
                'context_source', OLD.context_source
            )
        );
    END
    """
]

# Add partitioning-related statements
SCHEMA_STATEMENTS.extend(PARTITIONED_TABLES)
SCHEMA_STATEMENTS.extend(MATERIALIZED_VIEWS)
SCHEMA_STATEMENTS.extend(PARTITION_TRIGGERS)
SCHEMA_STATEMENTS.extend(PARTITION_INDICES)

async def initialize_schema(conn: aiosqlite.Connection) -> None:
    """Initialize database schema and indices."""
    for statement in SCHEMA_STATEMENTS:
        await conn.execute(statement)
    await conn.commit()
