"""
Database schema discovery tools for the MCP server.

This module provides tools for exploring database structure, including
listing tables and describing table schemas.
"""

from typing import Dict, Any, List
from fastmcp import Context

from .connection import get_database_manager


async def list_tables(ctx: Context) -> Dict[str, Any]:
    """
    List all tables in the connected database.
    
    This tool retrieves a list of all tables in the currently connected database.
    Useful for exploring the database structure and understanding what data is available.
    
    Returns:
        Dictionary containing list of table names and metadata
    """
    try:
        # Get the database manager for this session
        db_manager = get_database_manager(ctx)
        
        if not db_manager:
            await ctx.error("No database connection found. Please connect to a database first.")
            return {
                "success": False,
                "message": "No database connection found",
                "tables": []
            }
        
        # Check if connection is still active
        if not await db_manager.test_connection():
            await ctx.error("Database connection is not active")
            return {
                "success": False,
                "message": "Database connection is not active",
                "tables": []
            }
        
        await ctx.info("Retrieving list of tables from database")
        
        # Get tables from database
        tables = await db_manager.get_tables()
        
        await ctx.info(f"Found {len(tables)} tables in database")
        
        return {
            "success": True,
            "message": f"Successfully retrieved {len(tables)} tables",
            "table_count": len(tables),
            "tables": tables
        }
        
    except Exception as e:
        await ctx.error(f"Error listing tables: {str(e)}")
        return {
            "success": False,
            "message": "Failed to retrieve table list",
            "error": str(e),
            "tables": []
        }


async def describe_table(ctx: Context, table_name: str) -> Dict[str, Any]:
    """
    Get detailed schema information for a specific table.
    
    This tool retrieves comprehensive schema information including columns,
    data types, constraints, primary keys, and foreign key relationships.
    
    Args:
        table_name: Name of the table to describe
        
    Returns:
        Dictionary containing detailed table schema information
    """
    try:
        # Get the database manager for this session
        db_manager = get_database_manager(ctx)
        
        if not db_manager:
            await ctx.error("No database connection found. Please connect to a database first.")
            return {
                "success": False,
                "message": "No database connection found",
                "table_schema": {}
            }
        
        # Check if connection is still active
        if not await db_manager.test_connection():
            await ctx.error("Database connection is not active")
            return {
                "success": False,
                "message": "Database connection is not active", 
                "table_schema": {}
            }
        
        await ctx.info(f"Retrieving schema information for table: {table_name}")
        
        # Get table schema
        schema = await db_manager.get_table_schema(table_name)
        
        if not schema.columns:
            await ctx.warning(f"No schema information found for table: {table_name}")
            return {
                "success": False,
                "message": f"Table '{table_name}' not found or has no accessible schema",
                "table_schema": {}
            }
        
        await ctx.info(f"Retrieved schema for table '{table_name}' with {len(schema.columns)} columns")
        
        # Format the schema information for easy reading
        formatted_schema = {
            "table_name": schema.table_name,
            "column_count": len(schema.columns),
            "columns": schema.columns,
            "primary_keys": schema.primary_keys,
            "foreign_keys": schema.foreign_keys,
            "summary": _format_table_summary(schema)
        }
        
        return {
            "success": True,
            "message": f"Successfully retrieved schema for table '{table_name}'",
            "table_schema": formatted_schema
        }
        
    except Exception as e:
        await ctx.error(f"Error describing table {table_name}: {str(e)}")
        return {
            "success": False,
            "message": f"Failed to retrieve schema for table '{table_name}'",
            "error": str(e),
            "table_schema": {}
        }


async def get_database_summary(ctx: Context) -> Dict[str, Any]:
    """
    Get a high-level summary of the database structure.
    
    This tool provides an overview of the database including table count,
    and a brief description of each table's purpose based on its schema.
    
    Returns:
        Dictionary containing database structure summary
    """
    try:
        # Get the database manager for this session
        db_manager = get_database_manager(ctx)
        
        if not db_manager:
            await ctx.error("No database connection found. Please connect to a database first.")
            return {
                "success": False,
                "message": "No database connection found",
                "summary": {}
            }
        
        await ctx.info("Generating database structure summary")
        
        # Get all tables
        tables = await db_manager.get_tables()
        
        if not tables:
            return {
                "success": True,
                "message": "Database is empty (no tables found)",
                "summary": {
                    "table_count": 0,
                    "tables": []
                }
            }
        
        # Get summary for each table
        table_summaries = []
        for table_name in tables[:10]:  # Limit to first 10 tables for performance
            try:
                schema = await db_manager.get_table_schema(table_name)
                summary = _format_table_summary(schema)
                table_summaries.append({
                    "table_name": table_name,
                    "column_count": len(schema.columns),
                    "primary_keys": schema.primary_keys,
                    "foreign_key_count": len(schema.foreign_keys),
                    "summary": summary
                })
            except Exception as e:
                # Skip tables that can't be accessed
                await ctx.warning(f"Could not access schema for table {table_name}: {str(e)}")
        
        await ctx.info(f"Generated summary for {len(table_summaries)} out of {len(tables)} tables")
        
        return {
            "success": True,
            "message": f"Database summary generated for {len(table_summaries)} tables",
            "summary": {
                "total_table_count": len(tables),
                "summarized_table_count": len(table_summaries),
                "tables": table_summaries,
                "note": "Limited to first 10 tables for performance" if len(tables) > 10 else None
            }
        }
        
    except Exception as e:
        await ctx.error(f"Error generating database summary: {str(e)}")
        return {
            "success": False,
            "message": "Failed to generate database summary",
            "error": str(e),
            "summary": {}
        }


def _format_table_summary(schema) -> str:
    """
    Generate a human-readable summary of a table schema.
    
    Args:
        schema: TableSchema object
        
    Returns:
        String summary of the table structure
    """
    if not schema.columns:
        return "No column information available"
    
    summary_parts = []
    
    # Basic info
    summary_parts.append(f"{len(schema.columns)} columns")
    
    # Primary keys
    if schema.primary_keys:
        pk_str = ", ".join(schema.primary_keys)
        summary_parts.append(f"Primary key: {pk_str}")
    
    # Foreign keys
    if schema.foreign_keys:
        fk_count = len(schema.foreign_keys)
        summary_parts.append(f"{fk_count} foreign key{'s' if fk_count > 1 else ''}")
    
    # Column types summary
    type_counts = {}
    for col in schema.columns:
        col_type = col.get('data_type', 'unknown')
        type_counts[col_type] = type_counts.get(col_type, 0) + 1
    
    if type_counts:
        type_summary = ", ".join([f"{count} {type_name}" for type_name, count in type_counts.items()])
        summary_parts.append(f"Types: {type_summary}")
    
    return "; ".join(summary_parts)
