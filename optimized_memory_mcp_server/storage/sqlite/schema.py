import aiosqlite
from typing import List
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
    """
]

async def initialize_schema(conn: aiosqlite.Connection) -> None:
    """Initialize database schema and indices."""
    for statement in SCHEMA_STATEMENTS:
        await conn.execute(statement)
    await conn.commit()
