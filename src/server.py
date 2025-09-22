"""
Main FastMCP server for Natural Language SQL interactions.

This module contains the main FastMCP server instance and imports all
the available tools. It serves as the entry point for the application.

Features:
- Multi-database support (PostgreSQL, MySQL, SQLite)
- AI-powered query intelligence and optimization
- User authentication and role-based access control
- Data visualization with Plotly
- Multiple export formats (CSV, JSON, Excel)
- Query history and session management
- Advanced caching with Redis
"""

from fastmcp import FastMCP
import sys
import os
import warnings
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.config import config

# Core database and connection tools
from src.tools.connection import connect_database, disconnect_database, get_connection_status
from src.tools.schema import list_tables, describe_table, get_database_summary
from src.tools.query import query_data, add_data, update_data, delete_data

# Advanced query features
from src.tools.advanced_query import explain_query, query_with_suggestions, aggregate_data
from src.tools.history import get_query_history, repeat_query

# AI-powered features
from src.tools.ai_features import (
    explain_results, suggest_related_queries, optimize_query,
    improve_query_language, analyze_query_intent
)

# Authentication and user management
from src.tools.auth import (
    authenticate_user, logout_user, get_current_user, create_user,
    list_users, update_user_role, deactivate_user, check_permission
)

# Data visualization
from src.tools.visualization import (
    create_visualization, recommend_visualizations, 
    create_dashboard, export_visualization
)

# Data export
from src.tools.export import (
    export_csv, export_json, export_excel, export_multiple_formats
)


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
    try:
        supported_databases = ["PostgreSQL", "MySQL", "SQLite"]
        features = [
            "Natural Language to SQL Translation",
            "Multi-Database Support",
            "AI-Powered Query Intelligence",
            "Data Visualization",
            "Export Capabilities",
            "Query History & Session Management",
            "User Authentication & RBAC",
            "Advanced Caching"
        ]
        
        return {
            "server_name": config.server.name if config else "Natural Language SQL Server",
            "version": "2.0.0",
            "transport": config.server.transport if config else "stdio",
            "host": config.server.host if config else "127.0.0.1",
            "port": config.server.port if config else 8000,
            "database_type": config.database.db_type if config else "postgresql",
            "supported_databases": supported_databases,
            "llm_model": config.llm.model if config else "gpt-4o-mini",
            "features": features,
            "authentication_enabled": config.enable_authentication if config else False,
            "caching_enabled": config.enable_query_caching if config else False,
            "visualization_enabled": config.enable_visualization if config else True,
            "ai_features_enabled": config.enable_smart_suggestions if config else True
        }
    except Exception as e:
        warnings.warn(f"Error getting server info: {e}")
        return {
            "server_name": "Natural Language SQL Server",
            "version": "2.0.0",
            "status": "Configuration error",
            "error": str(e)
        }


# ================================
# CORE DATABASE TOOLS
# ================================

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


# ================================
# ADVANCED QUERY FEATURES
# ================================

# Register advanced query tools
mcp.tool(explain_query)
mcp.tool(query_with_suggestions)
mcp.tool(aggregate_data)

# Register query history tools
mcp.tool(get_query_history)
mcp.tool(repeat_query)


# ================================
# AI-POWERED FEATURES
# ================================

# Register AI-powered tools
mcp.tool(explain_results)
mcp.tool(suggest_related_queries)
mcp.tool(optimize_query)
mcp.tool(improve_query_language)
mcp.tool(analyze_query_intent)


# ================================
# AUTHENTICATION & USER MANAGEMENT
# ================================

# Register authentication tools
mcp.tool(authenticate_user)
mcp.tool(logout_user)
mcp.tool(get_current_user)
mcp.tool(check_permission)

# Register user management tools (Admin only)
mcp.tool(create_user)
mcp.tool(list_users)
mcp.tool(update_user_role)
mcp.tool(deactivate_user)


# ================================
# DATA VISUALIZATION
# ================================

# Register visualization tools
mcp.tool(create_visualization)
mcp.tool(recommend_visualizations)
mcp.tool(create_dashboard)
mcp.tool(export_visualization)


# ================================
# DATA EXPORT
# ================================

# Register export tools
mcp.tool(export_csv)
mcp.tool(export_json)
mcp.tool(export_excel)
mcp.tool(export_multiple_formats)


def startup_checks():
    """Perform startup checks and display configuration."""
    print("=" * 60)
    print("🚀 NATURAL LANGUAGE SQL MCP SERVER v2.0.0")
    print("=" * 60)
    
    # Configuration validation
    if config:
        print(f"✅ Configuration loaded successfully")
        print(f"   Database: {config.database.db_type} at {config.database.host}:{config.database.port}")
        print(f"   LLM Model: {config.llm.model}")
        
        # Feature status
        features = {
            "Authentication": config.enable_authentication,
            "Query Caching": config.enable_query_caching,
            "Query History": config.enable_query_history,
            "AI Suggestions": config.enable_smart_suggestions,
            "Visualizations": config.enable_visualization
        }
        
        print("\n🔧 Feature Status:")
        for feature, enabled in features.items():
            status = "✅ Enabled" if enabled else "❌ Disabled"
            print(f"   {feature}: {status}")
        
        # Configuration warnings
        validation_issues = config.validate_all()
        if validation_issues:
            print("\n⚠️  Configuration Warnings:")
            for issue in validation_issues:
                print(f"   • {issue}")
    else:
        print("❌ Configuration failed to load - using defaults")
    
    # Tool registration summary
    tool_count = len([
        # Core tools
        "connect_database", "disconnect_database", "get_connection_status",
        "list_tables", "describe_table", "get_database_summary", 
        "query_data", "add_data", "update_data", "delete_data",
        # Advanced features
        "explain_query", "query_with_suggestions", "aggregate_data",
        "get_query_history", "repeat_query",
        # AI features
        "explain_results", "suggest_related_queries", "optimize_query",
        "improve_query_language", "analyze_query_intent",
        # Auth features
        "authenticate_user", "logout_user", "get_current_user", "check_permission",
        "create_user", "list_users", "update_user_role", "deactivate_user",
        # Visualization
        "create_visualization", "recommend_visualizations", 
        "create_dashboard", "export_visualization",
        # Export
        "export_csv", "export_json", "export_excel", "export_multiple_formats",
        # Utility
        "hello", "server_info"
    ])
    
    print(f"\n🔨 Tools Registered: {tool_count} tools available")
    print("\n📊 Supported Databases: PostgreSQL, MySQL, SQLite")
    print("🤖 AI Features: OpenAI GPT-4o-mini (default)")
    print("📈 Visualization: Plotly-based interactive charts")
    print("💾 Export Formats: CSV, JSON, Excel")
    print("=" * 60)


if __name__ == "__main__":
    """
    Run the server with the configured transport method.
    """
    try:
        # Perform startup checks
        startup_checks()
        
        # Determine transport and start server
        if config and config.server.transport == "http":
            host = config.server.host
            port = config.server.port
            print(f"\n🌐 Starting HTTP server at http://{host}:{port}")
            print("   Use Ctrl+C to stop the server")
            print("=" * 60)
            
            mcp.run(
                transport="http",
                host=host,
                port=port
            )
        else:
            server_name = config.server.name if config else "Natural Language SQL Server"
            print(f"\n📡 Starting {server_name} with STDIO transport")
            print("   Ready for MCP client connections")
            print("   Use Ctrl+C to stop the server")
            print("=" * 60)
            
            mcp.run()  # Default to STDIO transport
            
    except KeyboardInterrupt:
        print("\n\n👋 Server stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Server startup failed: {e}")
        if config and config.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)
