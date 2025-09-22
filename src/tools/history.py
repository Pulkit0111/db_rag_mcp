"""
Query history and context management tools for the MCP server.

This module provides tools for managing query history, retrieving past queries,
and getting context-aware suggestions based on user's query patterns.
"""

import json
from typing import Dict, Any, List
from datetime import datetime
from fastmcp import Context

from ..core.session_manager import session_manager, QueryHistory
from ..core.exceptions import ValidationError, DatabaseConnectionError
from ..core.config import config
from .connection import get_database_manager


def _get_session_id(ctx: Context) -> str:
    """Get session ID from context."""
    return getattr(ctx, 'session_id', 'default_session')


async def get_query_history(
    ctx: Context, 
    limit: int = 10,
    successful_only: bool = False
) -> Dict[str, Any]:
    """
    Get recent query history for the current session.
    
    This tool retrieves the user's recent queries, showing both successful
    and failed attempts along with their results and execution times.
    
    Args:
        limit: Maximum number of queries to return (default: 10, max: 50)
        successful_only: Only return successful queries (default: False)
        
    Returns:
        Dictionary containing query history and session statistics
    """
    try:
        session_id = _get_session_id(ctx)
        
        # Validate limit
        if limit < 1 or limit > 50:
            raise ValidationError(
                "limit",
                str(limit),
                "Limit must be between 1 and 50"
            )
        
        await ctx.info(f"Retrieving last {limit} queries for session")
        
        # Get query history
        history = await session_manager.get_context(
            session_id, 
            last_n=limit,
            successful_only=successful_only
        )
        
        # Get session statistics
        stats = await session_manager.get_session_stats(session_id)
        
        # Format history for response
        formatted_history = []
        for query in history:
            formatted_query = {
                "id": query.id,
                "query": query.query,
                "sql": query.sql if query.success else None,
                "timestamp": query.timestamp.isoformat(),
                "execution_time": round(query.execution_time, 3),
                "results_count": query.results_count,
                "success": query.success,
                "database_type": query.database_type
            }
            
            if not query.success and query.error_message:
                formatted_query["error"] = query.error_message
            
            formatted_history.append(formatted_query)
        
        await ctx.info(f"Retrieved {len(formatted_history)} queries from history")
        
        return {
            "success": True,
            "session_id": session_id,
            "history": formatted_history,
            "total_queries": len(formatted_history),
            "statistics": {
                "total_queries": stats.total_queries,
                "successful_queries": stats.successful_queries,
                "failed_queries": stats.failed_queries,
                "success_rate": round(stats.success_rate, 1),
                "average_execution_time": round(stats.average_execution_time, 3)
            }
        }
        
    except ValidationError as e:
        await ctx.error(f"Invalid input: {e.user_message}")
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False)
        }
    except Exception as e:
        await ctx.error(f"Error retrieving query history: {str(e)}")
        return {
            "success": False,
            "error": {
                "message": "Failed to retrieve query history",
                "technical_details": str(e) if config and config.debug else None
            }
        }


async def repeat_query(ctx: Context, query_id: str) -> Dict[str, Any]:
    """
    Re-execute a previous query by its ID.
    
    This tool allows users to repeat a query from their history without
    having to retype it. The query will be executed with current database state.
    
    Args:
        query_id: The ID of the query to repeat
        
    Returns:
        Dictionary containing the re-executed query results
    """
    try:
        session_id = _get_session_id(ctx)
        
        # Validate query_id
        if not query_id or not query_id.strip():
            raise ValidationError(
                "query_id",
                "empty string",
                "Query ID cannot be empty"
            )
        
        await ctx.info(f"Looking up query {query_id} for re-execution")
        
        # Find the query in history
        historical_query = await session_manager.get_query_by_id(session_id, query_id)
        
        if not historical_query:
            return {
                "success": False,
                "message": f"Query with ID {query_id} not found in session history",
                "suggestions": [
                    "Use get_query_history to see available queries",
                    "Check if you're using the correct session",
                    "The query may have been removed due to history limits"
                ]
            }
        
        await ctx.info(f"Found query: '{historical_query.query[:50]}...'")
        
        # Import query_data here to avoid circular imports
        from .query import query_data
        
        # Re-execute the original natural language query
        result = await query_data(ctx, historical_query.query)
        
        if result["success"]:
            # Add metadata about this being a repeated query
            result["repeated_from"] = {
                "original_query_id": query_id,
                "original_timestamp": historical_query.timestamp.isoformat(),
                "original_success": historical_query.success,
                "original_results_count": historical_query.results_count
            }
            
            await ctx.info(f"Successfully re-executed query, returned {result.get('row_count', 0)} rows")
        else:
            await ctx.warning("Query re-execution failed")
        
        return result
        
    except ValidationError as e:
        await ctx.error(f"Invalid input: {e.user_message}")
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False)
        }
    except Exception as e:
        await ctx.error(f"Error repeating query: {str(e)}")
        return {
            "success": False,
            "error": {
                "message": "Failed to repeat query",
                "technical_details": str(e) if config and config.debug else None
            }
        }


async def get_query_suggestions(ctx: Context, current_query: str = None) -> Dict[str, Any]:
    """
    Get AI-powered query suggestions based on session history and context.
    
    This tool analyzes the user's query patterns and database structure
    to suggest relevant follow-up queries that might be useful.
    
    Args:
        current_query: Current query context for more relevant suggestions (optional)
        
    Returns:
        Dictionary containing suggested queries and explanations
    """
    try:
        session_id = _get_session_id(ctx)
        
        await ctx.info("Generating query suggestions based on session context")
        
        # Get follow-up suggestions from session manager
        suggestions = await session_manager.suggest_followup(session_id, current_query)
        
        # Get similar queries if current_query is provided
        similar_queries = []
        if current_query and current_query.strip():
            similar_queries = await session_manager.get_similar_queries(
                session_id, 
                current_query, 
                limit=3
            )
        
        # Get database context for enhanced suggestions
        db_manager = get_database_manager(ctx)
        enhanced_suggestions = []
        
        if db_manager and await db_manager.test_connection():
            try:
                # Get available tables for context
                tables = await db_manager.get_tables()
                if tables:
                    # Add table-specific suggestions
                    for table in tables[:3]:  # Limit to first 3 tables
                        enhanced_suggestions.extend([
                            f"Show me the structure of the {table} table",
                            f"How many records are in {table}?",
                            f"What are the most recent entries in {table}?"
                        ])
            except Exception as e:
                await ctx.warning(f"Could not get database context: {str(e)}")
        
        # Combine and deduplicate suggestions
        all_suggestions = suggestions + enhanced_suggestions
        unique_suggestions = list(dict.fromkeys(all_suggestions))[:10]  # Remove duplicates, limit to 10
        
        # Format similar queries
        formatted_similar = []
        for similar in similar_queries:
            formatted_similar.append({
                "query": similar.query,
                "timestamp": similar.timestamp.isoformat(),
                "results_count": similar.results_count,
                "execution_time": round(similar.execution_time, 3)
            })
        
        await ctx.info(f"Generated {len(unique_suggestions)} suggestions")
        
        return {
            "success": True,
            "session_id": session_id,
            "suggestions": unique_suggestions,
            "similar_queries": formatted_similar,
            "current_query": current_query,
            "suggestion_count": len(unique_suggestions)
        }
        
    except Exception as e:
        await ctx.error(f"Error generating suggestions: {str(e)}")
        return {
            "success": False,
            "error": {
                "message": "Failed to generate query suggestions",
                "technical_details": str(e) if config and config.debug else None
            }
        }


async def clear_query_history(ctx: Context, confirm: bool = False) -> Dict[str, Any]:
    """
    Clear all query history for the current session.
    
    This tool removes all stored queries and resets session statistics.
    Use with caution as this action cannot be undone.
    
    Args:
        confirm: Must be True to actually clear the history
        
    Returns:
        Dictionary indicating success or failure of the operation
    """
    try:
        session_id = _get_session_id(ctx)
        
        if not confirm:
            return {
                "success": False,
                "message": "History clearing requires explicit confirmation",
                "warning": "This will permanently delete all query history for this session",
                "instructions": "Call this tool again with confirm=True to proceed"
            }
        
        # Get current stats before clearing
        stats = await session_manager.get_session_stats(session_id)
        queries_cleared = stats.total_queries
        
        # Clear the session history
        if session_id in session_manager._sessions:
            del session_manager._sessions[session_id]
        if session_id in session_manager._session_stats:
            del session_manager._session_stats[session_id]
        if session_id in session_manager._session_last_activity:
            del session_manager._session_last_activity[session_id]
        
        await ctx.info(f"Cleared {queries_cleared} queries from session history")
        
        return {
            "success": True,
            "message": f"Successfully cleared {queries_cleared} queries from session history",
            "session_id": session_id,
            "queries_cleared": queries_cleared
        }
        
    except Exception as e:
        await ctx.error(f"Error clearing history: {str(e)}")
        return {
            "success": False,
            "error": {
                "message": "Failed to clear query history",
                "technical_details": str(e) if config and config.debug else None
            }
        }


async def export_session_data(ctx: Context, format: str = "json") -> Dict[str, Any]:
    """
    Export session history and statistics for backup or analysis.
    
    This tool exports the complete session data including all queries,
    statistics, and metadata in the specified format.
    
    Args:
        format: Export format - "json" or "csv" (default: "json")
        
    Returns:
        Dictionary containing exported data or download information
    """
    try:
        session_id = _get_session_id(ctx)
        
        # Validate format
        if format.lower() not in ["json", "csv"]:
            raise ValidationError(
                "format",
                format,
                "Format must be 'json' or 'csv'"
            )
        
        await ctx.info(f"Exporting session data in {format.upper()} format")
        
        # Export session data
        export_data = await session_manager.export_session_history(session_id)
        
        if format.lower() == "json":
            return {
                "success": True,
                "format": "json",
                "data": export_data,
                "export_timestamp": datetime.now().isoformat(),
                "queries_exported": len(export_data.get("history", []))
            }
        
        elif format.lower() == "csv":
            # Convert to CSV format
            import csv
            import io
            
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Write headers
            headers = [
                "Query ID", "Timestamp", "Natural Language Query", "SQL Query",
                "Success", "Results Count", "Execution Time", "Database Type", "Error Message"
            ]
            writer.writerow(headers)
            
            # Write data rows
            for query_data in export_data.get("history", []):
                row = [
                    query_data.get("id", ""),
                    query_data.get("timestamp", ""),
                    query_data.get("query", ""),
                    query_data.get("sql", ""),
                    query_data.get("success", ""),
                    query_data.get("results_count", ""),
                    query_data.get("execution_time", ""),
                    query_data.get("database_type", ""),
                    query_data.get("error_message", "")
                ]
                writer.writerow(row)
            
            csv_data = output.getvalue()
            output.close()
            
            return {
                "success": True,
                "format": "csv",
                "data": csv_data,
                "filename": f"session_{session_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                "export_timestamp": datetime.now().isoformat(),
                "queries_exported": len(export_data.get("history", []))
            }
        
    except ValidationError as e:
        await ctx.error(f"Invalid input: {e.user_message}")
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False)
        }
    except Exception as e:
        await ctx.error(f"Error exporting session data: {str(e)}")
        return {
            "success": False,
            "error": {
                "message": "Failed to export session data",
                "technical_details": str(e) if config and config.debug else None
            }
        }
