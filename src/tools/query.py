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
from ..core.exceptions import (
    DatabaseConnectionError, 
    QueryTranslationError, 
    QueryExecutionError,
    ValidationError
)
from ..core.config import config
from ..core.session_manager import session_manager
from ..core.cache import cache_query_result, query_cache, schema_cache
import time


def _get_session_id(ctx: Context) -> str:
    """Get session ID from context."""
    return getattr(ctx, 'session_id', 'default_session')


@cache_query_result(ttl=600)  # Cache for 10 minutes
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
    start_time = time.time()
    session_id = _get_session_id(ctx)
    sql_query = ""
    db_type = "unknown"
    
    try:
        # Validate input
        if not natural_language_query or not natural_language_query.strip():
            raise ValidationError(
                "natural_language_query",
                "empty string",
                "Query must be a non-empty string"
            )
        
        # Get the database manager for this session
        db_manager = get_database_manager(ctx)
        
        if not db_manager:
            raise DatabaseConnectionError(
                db_type="unknown",
                technical_details="No database manager found in session"
            )
        
        # Check if connection is still active
        if not await db_manager.test_connection():
            raise DatabaseConnectionError(
                db_type="unknown",
                technical_details="Connection test failed"
            )
        
        await ctx.info(f"Processing natural language query: {natural_language_query}")
        
        # Get database schema for context
        await ctx.info("Retrieving database schema for query context")
        tables = await db_manager.get_tables()
        
        if not tables:
            raise QueryTranslationError(
                query=natural_language_query,
                reason="Database appears to be empty (no tables found)",
                technical_details="get_tables() returned empty list"
            )
        
        # Get schema for all tables (limit for performance)
        schemas = []
        max_tables = config.max_result_rows // 100 if config else 10  # Dynamic limit based on config
        table_limit = min(len(tables), max_tables)
        
        for table_name in tables[:table_limit]:
            try:
                schema = await db_manager.get_table_schema(table_name)
                if schema.columns:  # Only include tables we can access
                    schemas.append(schema)
            except Exception as e:
                await ctx.warning(f"Could not access schema for table {table_name}: {str(e)}")
        
        if not schemas:
            raise QueryTranslationError(
                query=natural_language_query,
                reason="Could not access any table schemas",
                technical_details="All table schema requests failed"
            )
        
        await ctx.info(f"Using schema from {len(schemas)} tables for query translation")
        
        # Translate natural language to SQL
        await ctx.info("Translating natural language to SQL")
        translator = get_translator()
        
        # Get database type from config or manager
        db_type = config.database.db_type if config else "postgresql"
        
        translation_result = await translator.translate_to_select(
            natural_language_query,
            schemas,
            database_type=db_type
        )
        
        if not translation_result["success"]:
            raise QueryTranslationError(
                query=natural_language_query,
                reason=translation_result.get('error', 'Translation failed'),
                technical_details=str(translation_result)
            )
        
        sql_query = translation_result["sql_query"]
        await ctx.info(f"Generated SQL: {sql_query}")
        
        # Execute the SQL query
        await ctx.info("Executing SQL query against database")
        query_result = await db_manager.execute_query(sql_query)
        
        if not query_result.success:
            raise QueryExecutionError(
                sql_query=sql_query,
                db_error=query_result.error_message,
                technical_details=f"Row count: {query_result.row_count}"
            )
        
        await ctx.info(f"Query executed successfully, returned {query_result.row_count} rows")
        
        # Check if result set is too large
        max_rows = config.max_result_rows if config else 1000
        truncated = query_result.row_count > max_rows
        if truncated:
            await ctx.warning(f"Result set ({query_result.row_count} rows) exceeds limit ({max_rows}). Truncating results.")
            query_result.data = query_result.data[:max_rows]
        
        # Record successful query in history
        execution_time = time.time() - start_time
        if config and config.enable_query_history:
            try:
                await session_manager.add_query(
                    session_id=session_id,
                    natural_query=natural_language_query,
                    sql_query=sql_query,
                    execution_time=execution_time,
                    results_count=query_result.row_count,
                    success=True,
                    database_type=db_type
                )
            except Exception as e:
                await ctx.warning(f"Failed to record query in history: {str(e)}")
        
        # Format results for return
        return {
            "success": True,
            "message": f"Query executed successfully, returned {len(query_result.data)} rows",
            "original_query": natural_language_query,
            "generated_sql": sql_query,
            "row_count": query_result.row_count,
            "results": query_result.data,
            "truncated": truncated,
            "execution_time": round(execution_time, 3)
        }
        
    except (DatabaseConnectionError, QueryTranslationError, QueryExecutionError, ValidationError) as e:
        await ctx.error(f"Query processing failed: {e.user_message}")
        
        # Record failed query in history
        execution_time = time.time() - start_time
        if config and config.enable_query_history:
            try:
                await session_manager.add_query(
                    session_id=session_id,
                    natural_query=natural_language_query,
                    sql_query=sql_query,
                    execution_time=execution_time,
                    results_count=0,
                    success=False,
                    database_type=db_type,
                    error_message=e.user_message
                )
            except Exception as history_error:
                await ctx.warning(f"Failed to record failed query in history: {str(history_error)}")
        
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False),
            "results": [],
            "execution_time": round(execution_time, 3)
        }
    except Exception as e:
        # Unexpected error - convert to appropriate exception
        unexpected_error = QueryExecutionError(
            sql_query=sql_query or "unknown",
            db_error="Unexpected error during query processing",
            technical_details=str(e)
        )
        
        await ctx.error(f"Unexpected error during query: {unexpected_error.user_message}")
        
        # Record unexpected error in history
        execution_time = time.time() - start_time
        if config and config.enable_query_history:
            try:
                await session_manager.add_query(
                    session_id=session_id,
                    natural_query=natural_language_query,
                    sql_query=sql_query,
                    execution_time=execution_time,
                    results_count=0,
                    success=False,
                    database_type=db_type,
                    error_message=unexpected_error.user_message
                )
            except Exception as history_error:
                await ctx.warning(f"Failed to record unexpected error in history: {str(history_error)}")
        
        return {
            "success": False,
            "error": unexpected_error.to_dict(include_technical=config.debug if config else False),
            "results": [],
            "execution_time": round(execution_time, 3)
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
