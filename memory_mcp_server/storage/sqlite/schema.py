import aiosqlite
from typing import List

SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS entities (
        name TEXT PRIMARY KEY,
        entity_type TEXT NOT NULL,
        observations TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS relations (
        from_entity TEXT NOT NULL,
        to_entity TEXT NOT NULL,
        relation_type TEXT NOT NULL,
        PRIMARY KEY (from_entity, to_entity, relation_type),
        FOREIGN KEY (from_entity) REFERENCES entities(name),
        FOREIGN KEY (to_entity) REFERENCES entities(name)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_entity_type ON entities(entity_type)",
    "CREATE INDEX IF NOT EXISTS idx_from_entity ON relations(from_entity)",
    "CREATE INDEX IF NOT EXISTS idx_to_entity ON relations(to_entity)"
]

async def initialize_schema(conn: aiosqlite.Connection) -> None:
    """Initialize database schema and indices."""
    for statement in SCHEMA_STATEMENTS:
        await conn.execute(statement)
