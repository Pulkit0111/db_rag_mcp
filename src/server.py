"""
Main FastMCP server for Natural Language SQL interactions.

This module contains the main FastMCP server instance and imports all
the available tools. It serves as the entry point for the application.
"""

from fastmcp import FastMCP
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.config import config
from src.tools.connection import connect_database, disconnect_database, get_connection_status
from src.tools.schema import list_tables, describe_table, get_database_summary
from src.tools.query import query_data, add_data, update_data, delete_data


# Initialize the FastMCP server with configuration
mcp = FastMCP(name=config.server.name)


@mcp.tool
def hello(name: str = "world") -> str:
    """
    A simple hello world tool to test the server functionality.
    
    Args:
        name: The name to greet (default: "world")
        
    Returns:
        A greeting message
    """
    return f"Hello, {name}! Welcome to the Natural Language SQL MCP Server."


@mcp.tool
def server_info() -> dict:
    """
    Get information about the server configuration.
    
    Returns:
        Dictionary containing server information
    """
    return {
        "server_name": config.server.name,
        "transport": config.server.transport,
        "host": config.server.host,
        "port": config.server.port,
        "database_type": config.database.db_type,
        "llm_model": config.llm.model
    }


# Register database connection tools
mcp.tool(connect_database)
mcp.tool(disconnect_database) 
mcp.tool(get_connection_status)

# Register schema discovery tools
mcp.tool(list_tables)
mcp.tool(describe_table)
mcp.tool(get_database_summary)

# Register data query and manipulation tools
mcp.tool(query_data)
mcp.tool(add_data)
mcp.tool(update_data)
mcp.tool(delete_data)


if __name__ == "__main__":
    """
    Run the server with the configured transport method.
    """
    if config.server.transport == "http":
        print(f"Starting {config.server.name} on http://{config.server.host}:{config.server.port}")
        mcp.run(
            transport="http",
            host=config.server.host,
            port=config.server.port
        )
    else:
        print(f"Starting {config.server.name} with STDIO transport")
        mcp.run()  # Default to STDIO transport
