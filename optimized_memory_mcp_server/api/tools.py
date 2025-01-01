from typing import Dict, Any, Callable, Awaitable, List
import mcp.types as types

from ..storage.base import StorageBackend

def create_tool_handlers(storage: StorageBackend) -> Dict[str, Callable[[Dict[str, Any]], Awaitable[List[types.TextContent]]]]:
    """Create tool handlers using the provided storage backend."""
    
    async def handle_tool(name: str, arguments: Dict[str, Any]) -> List[types.TextContent]:
        try:
            result = await getattr(storage, name)(**arguments)
            return [types.TextContent(
                type="text",
                text=str(result) if result is not None else "Operation completed successfully"
            )]
        except Exception as e:
            error_message = f"Error in {name}: {str(e)}"
            return [types.TextContent(type="text", text=error_message)]

    return {
        "create_entities": lambda args: handle_tool("create_entities", args),
        "create_relations": lambda args: handle_tool("create_relations", args),
        "read_graph": lambda args: handle_tool("read_graph", args),
        "search_nodes": lambda args: handle_tool("search_nodes", args),
        "add_observations": lambda args: handle_tool("add_observations", args),
        "delete_entities": lambda args: handle_tool("delete_entities", args),
        "delete_observations": lambda args: handle_tool("delete_observations", args),
        "delete_relations": lambda args: handle_tool("delete_relations", args),
        "open_nodes": lambda args: handle_tool("open_nodes", args),
    }
