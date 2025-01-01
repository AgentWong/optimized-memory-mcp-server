#!/usr/bin/env python3
import asyncio
import logging
from typing import List
import argparse
import os

import mcp.types as types
from mcp.server import Server
from mcp.server.stdio import stdio_server

from ..storage import create_storage_backend
from ..core.exceptions import KnowledgeGraphError
from .tools import create_tool_handlers

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("knowledge-graph-server")

def parse_database_config() -> dict:
    """Parse database configuration from environment variables."""
    return {
        "database_url": os.environ.get("DATABASE_URL", "sqlite:///memory.db"),
        "echo": os.environ.get("SQL_ECHO", "").lower() == "true"
    }

async def async_main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--database-url",
        type=str,
        help="SQLite database URL (e.g., sqlite:///path/to/db)"
    )
    args = parser.parse_args()

    # Get database configuration
    config = parse_database_config()
    if args.database_url:
        config["database_url"] = args.database_url

    # Initialize storage backend
    storage = await create_storage_backend(
        database_url=config["database_url"],
        echo=config["echo"]
    )
    await storage.initialize()

    app = Server("knowledge-graph-server")
    
    # Register tool handlers
    tool_handlers = create_tool_handlers(storage)
    for name, handler in tool_handlers.items():
        app.register_tool(name, handler)

    async with stdio_server() as (read_stream, write_stream):
        logger.info(
            f"Knowledge Graph MCP Server running on stdio "
            f"(database: {config['database_url']})"
        )
        try:
            await app.run(
                read_stream,
                write_stream,
                app.create_initialization_options()
            )
        finally:
            await storage.cleanup()

def main():
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        logger.info("Server shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        exit(1)
