"""
Query execution tools for the MCP server.

This module provides tools for executing natural language queries
and data modification commands against the database.
"""

import json
from typing import Dict, Any, List
from fastmcp import Context

from .connection import get_database_manager
from ..nlp.translator import get_translator


async def query_data(ctx: Context, natural_language_query: str) -> Dict[str, Any]:
    """
    Execute a natural language query against the database.
    
    This tool translates a natural language question into a SQL SELECT statement
    and executes it against the connected database, returning the results.
    
    Args:
        natural_language_query: The natural language query to execute
        
    Returns:
        Dictionary containing query results or error information
    """
    try:
        # Get the database manager for this session
        db_manager = get_database_manager(ctx)
        
        if not db_manager:
            await ctx.error("No database connection found. Please connect to a database first.")
            return {
                "success": False,
                "message": "No database connection found",
                "results": []
            }
        
        # Check if connection is still active
        if not await db_manager.test_connection():
            await ctx.error("Database connection is not active")
            return {
                "success": False,
                "message": "Database connection is not active",
                "results": []
            }
        
        await ctx.info(f"Processing natural language query: {natural_language_query}")
        
        # Get database schema for context
        await ctx.info("Retrieving database schema for query context")
        tables = await db_manager.get_tables()
        
        if not tables:
            await ctx.warning("No tables found in database")
            return {
                "success": False,
                "message": "Database appears to be empty (no tables found)",
                "results": []
            }
        
        # Get schema for all tables (or limit to reasonable number for performance)
        schemas = []
        table_limit = min(len(tables), 10)  # Limit to first 10 tables for performance
        
        for table_name in tables[:table_limit]:
            try:
                schema = await db_manager.get_table_schema(table_name)
                if schema.columns:  # Only include tables we can access
                    schemas.append(schema)
            except Exception as e:
                await ctx.warning(f"Could not access schema for table {table_name}: {str(e)}")
        
        if not schemas:
            return {
                "success": False,
                "message": "Could not access any table schemas",
                "results": []
            }
        
        await ctx.info(f"Using schema from {len(schemas)} tables for query translation")
        
        # Translate natural language to SQL
        await ctx.info("Translating natural language to SQL")
        translator = get_translator()
        
        translation_result = await translator.translate_to_select(
            natural_language_query,
            schemas,
            database_type="postgresql"  # TODO: Get from config
        )
        
        if not translation_result["success"]:
            await ctx.error(f"Failed to translate query: {translation_result.get('error', 'Unknown error')}")
            return {
                "success": False,
                "message": "Failed to translate natural language to SQL",
                "error": translation_result.get('error'),
                "results": []
            }
        
        sql_query = translation_result["sql_query"]
        await ctx.info(f"Generated SQL: {sql_query}")
        
        # Execute the SQL query
        await ctx.info("Executing SQL query against database")
        query_result = await db_manager.execute_query(sql_query)
        
        if not query_result.success:
            await ctx.error(f"Query execution failed: {query_result.error_message}")
            return {
                "success": False,
                "message": "SQL query execution failed",
                "error": query_result.error_message,
                "generated_sql": sql_query,
                "results": []
            }
        
        await ctx.info(f"Query executed successfully, returned {query_result.row_count} rows")
        
        # Format results for return
        return {
            "success": True,
            "message": f"Query executed successfully, returned {query_result.row_count} rows",
            "original_query": natural_language_query,
            "generated_sql": sql_query,
            "row_count": query_result.row_count,
            "results": query_result.data
        }
        
    except Exception as e:
        await ctx.error(f"Error processing query: {str(e)}")
        return {
            "success": False,
            "message": "Error processing natural language query",
            "error": str(e),
            "results": []
        }


async def add_data(ctx: Context, natural_language_command: str) -> Dict[str, Any]:
    """
    Add data to the database using natural language commands.
    
    This tool translates a natural language command into a SQL INSERT statement
    and executes it against the connected database.
    
    Args:
        natural_language_command: The natural language command for adding data
        
    Returns:
        Dictionary containing operation results
    """
    try:
        # Get the database manager for this session
        db_manager = get_database_manager(ctx)
        
        if not db_manager:
            await ctx.error("No database connection found. Please connect to a database first.")
            return {
                "success": False,
                "message": "No database connection found"
            }
        
        if not await db_manager.test_connection():
            await ctx.error("Database connection is not active")
            return {
                "success": False,
                "message": "Database connection is not active"
            }
        
        await ctx.info(f"Processing data insertion command: {natural_language_command}")
        
        # Get database schema for context
        tables = await db_manager.get_tables()
        schemas = []
        
        for table_name in tables[:10]:  # Limit for performance
            try:
                schema = await db_manager.get_table_schema(table_name)
                if schema.columns:
                    schemas.append(schema)
            except Exception as e:
                await ctx.warning(f"Could not access schema for table {table_name}: {str(e)}")
        
        if not schemas:
            return {
                "success": False,
                "message": "Could not access any table schemas"
            }
        
        # Translate to INSERT SQL
        await ctx.info("Translating natural language command to INSERT SQL")
        translator = get_translator()
        
        translation_result = await translator.translate_to_insert(
            natural_language_command,
            schemas,
            database_type="postgresql"
        )
        
        if not translation_result["success"]:
            await ctx.error(f"Failed to translate command: {translation_result.get('error', 'Unknown error')}")
            return {
                "success": False,
                "message": "Failed to translate natural language to SQL",
                "error": translation_result.get('error')
            }
        
        sql_query = translation_result["sql_query"]
        await ctx.info(f"Generated SQL: {sql_query}")
        
        # Execute the INSERT query
        await ctx.info("Executing INSERT statement")
        query_result = await db_manager.execute_query(sql_query)
        
        if not query_result.success:
            await ctx.error(f"INSERT execution failed: {query_result.error_message}")
            return {
                "success": False,
                "message": "INSERT statement execution failed",
                "error": query_result.error_message,
                "generated_sql": sql_query
            }
        
        await ctx.info("Data inserted successfully")
        
        return {
            "success": True,
            "message": "Data inserted successfully",
            "original_command": natural_language_command,
            "generated_sql": sql_query,
            "affected_rows": query_result.row_count
        }
        
    except Exception as e:
        await ctx.error(f"Error processing data insertion: {str(e)}")
        return {
            "success": False,
            "message": "Error processing data insertion command",
            "error": str(e)
        }


async def update_data(ctx: Context, natural_language_command: str) -> Dict[str, Any]:
    """
    Update data in the database using natural language commands.
    
    This tool translates a natural language command into a SQL UPDATE statement
    and executes it against the connected database.
    
    Args:
        natural_language_command: The natural language command for updating data
        
    Returns:
        Dictionary containing operation results
    """
    try:
        db_manager = get_database_manager(ctx)
        
        if not db_manager:
            await ctx.error("No database connection found. Please connect to a database first.")
            return {
                "success": False,
                "message": "No database connection found"
            }
        
        if not await db_manager.test_connection():
            await ctx.error("Database connection is not active")
            return {
                "success": False,
                "message": "Database connection is not active"
            }
        
        await ctx.info(f"Processing data update command: {natural_language_command}")
        
        # Get database schema
        tables = await db_manager.get_tables()
        schemas = []
        
        for table_name in tables[:10]:
            try:
                schema = await db_manager.get_table_schema(table_name)
                if schema.columns:
                    schemas.append(schema)
            except Exception as e:
                await ctx.warning(f"Could not access schema for table {table_name}: {str(e)}")
        
        if not schemas:
            return {
                "success": False,
                "message": "Could not access any table schemas"
            }
        
        # Translate to UPDATE SQL
        await ctx.info("Translating natural language command to UPDATE SQL")
        translator = get_translator()
        
        translation_result = await translator.translate_to_update(
            natural_language_command,
            schemas,
            database_type="postgresql"
        )
        
        if not translation_result["success"]:
            await ctx.error(f"Failed to translate command: {translation_result.get('error', 'Unknown error')}")
            return {
                "success": False,
                "message": "Failed to translate natural language to SQL",
                "error": translation_result.get('error')
            }
        
        sql_query = translation_result["sql_query"]
        await ctx.info(f"Generated SQL: {sql_query}")
        
        # Safety confirmation for UPDATE
        await ctx.warning(f"About to execute UPDATE statement: {sql_query}")
        
        # Execute the UPDATE query
        query_result = await db_manager.execute_query(sql_query)
        
        if not query_result.success:
            await ctx.error(f"UPDATE execution failed: {query_result.error_message}")
            return {
                "success": False,
                "message": "UPDATE statement execution failed",
                "error": query_result.error_message,
                "generated_sql": sql_query
            }
        
        await ctx.info(f"Data updated successfully, {query_result.row_count} rows affected")
        
        return {
            "success": True,
            "message": f"Data updated successfully, {query_result.row_count} rows affected",
            "original_command": natural_language_command,
            "generated_sql": sql_query,
            "affected_rows": query_result.row_count
        }
        
    except Exception as e:
        await ctx.error(f"Error processing data update: {str(e)}")
        return {
            "success": False,
            "message": "Error processing data update command",
            "error": str(e)
        }


async def delete_data(ctx: Context, natural_language_command: str) -> Dict[str, Any]:
    """
    Delete data from the database using natural language commands.
    
    This tool translates a natural language command into a SQL DELETE statement
    and executes it against the connected database.
    
    WARNING: This operation is irreversible. The command must clearly specify
    which records to delete to prevent accidental bulk deletions.
    
    Args:
        natural_language_command: The natural language command for deleting data
        
    Returns:
        Dictionary containing operation results
    """
    try:
        db_manager = get_database_manager(ctx)
        
        if not db_manager:
            await ctx.error("No database connection found. Please connect to a database first.")
            return {
                "success": False,
                "message": "No database connection found"
            }
        
        if not await db_manager.test_connection():
            await ctx.error("Database connection is not active")
            return {
                "success": False,
                "message": "Database connection is not active"
            }
        
        await ctx.warning(f"Processing DELETION command: {natural_language_command}")
        
        # Get database schema
        tables = await db_manager.get_tables()
        schemas = []
        
        for table_name in tables[:10]:
            try:
                schema = await db_manager.get_table_schema(table_name)
                if schema.columns:
                    schemas.append(schema)
            except Exception as e:
                await ctx.warning(f"Could not access schema for table {table_name}: {str(e)}")
        
        if not schemas:
            return {
                "success": False,
                "message": "Could not access any table schemas"
            }
        
        # Translate to DELETE SQL
        await ctx.warning("Translating natural language command to DELETE SQL")
        translator = get_translator()
        
        translation_result = await translator.translate_to_delete(
            natural_language_command,
            schemas,
            database_type="postgresql"
        )
        
        if not translation_result["success"]:
            await ctx.error(f"Failed to translate command: {translation_result.get('error', 'Unknown error')}")
            return {
                "success": False,
                "message": "Failed to translate natural language to SQL",
                "error": translation_result.get('error')
            }
        
        sql_query = translation_result["sql_query"]
        await ctx.warning(f"Generated DELETE SQL: {sql_query}")
        
        # Critical safety warning for DELETE
        await ctx.warning("WARNING: About to execute DELETE statement. This operation cannot be undone!")
        
        # Execute the DELETE query
        query_result = await db_manager.execute_query(sql_query)
        
        if not query_result.success:
            await ctx.error(f"DELETE execution failed: {query_result.error_message}")
            return {
                "success": False,
                "message": "DELETE statement execution failed",
                "error": query_result.error_message,
                "generated_sql": sql_query
            }
        
        await ctx.warning(f"Data deleted successfully, {query_result.row_count} rows affected")
        
        return {
            "success": True,
            "message": f"Data deleted successfully, {query_result.row_count} rows affected",
            "original_command": natural_language_command,
            "generated_sql": sql_query,
            "affected_rows": query_result.row_count,
            "warning": "Deletion completed - this operation cannot be undone"
        }
        
    except Exception as e:
        await ctx.error(f"Error processing data deletion: {str(e)}")
        return {
            "success": False,
            "message": "Error processing data deletion command",
            "error": str(e)
        }
