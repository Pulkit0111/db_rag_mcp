"""
Database connection tools for the MCP server.

This module provides tools for establishing and managing database connections.
"""

import asyncio
from typing import Dict, Any, Optional
from fastmcp import Context

from ..database import create_database_manager, BaseManager
from ..core.config import config
from ..core.exceptions import DatabaseConnectionError, ConfigurationError


# Global dictionary to store database managers per session
_database_managers: Dict[str, BaseManager] = {}


def _get_session_id(ctx: Context) -> str:
    """Get a unique session identifier from the context."""
    # For now, use a simple approach - in production, you might want more sophisticated session management
    return getattr(ctx, 'session_id', 'default_session')


async def connect_database(
    ctx: Context,
    host: str = None,
    port: int = None,
    username: str = None,
    password: str = None,
    database_name: str = None,
    db_type: str = None
) -> Dict[str, Any]:
    """
    Establish a connection to a SQL database.
    
    This tool connects to a database using the provided parameters or defaults from configuration.
    The connection is stored for the current session and can be used by other tools.
    
    Args:
        host: Database host (optional, uses config default if not provided)
        port: Database port (optional, uses config default if not provided)  
        username: Database username (optional, uses config default if not provided)
        password: Database password (optional, uses config default if not provided)
        database_name: Database name (optional, uses config default if not provided)
        db_type: Database type like 'postgresql' (optional, uses config default if not provided)
        
    Returns:
        Dictionary with connection status and information
    """
    session_id = _get_session_id(ctx)
    
    try:
        # Check if config is available
        if config is None:
            raise ConfigurationError(
                "global_config",
                "Configuration could not be loaded",
                technical_details="Config object is None"
            )
        
        # Use provided parameters or fall back to config defaults
        connection_config = {
            'host': host or config.database.host,
            'port': port or config.database.port,
            'username': username or config.database.username,
            'password': password or config.database.password,
            'database': database_name or config.database.database,
        }
        
        db_type = db_type or config.database.db_type
        
        await ctx.info(f"Attempting to connect to {db_type} database at {connection_config['host']}:{connection_config['port']}")
        
        # Create database manager
        try:
            db_manager = create_database_manager(db_type, connection_config)
        except ValueError as e:
            raise DatabaseConnectionError(
                db_type=db_type,
                host=connection_config['host'],
                port=connection_config['port'],
                technical_details=str(e)
            )
        
        # Attempt connection
        success = await db_manager.connect()
        
        if success:
            # Store the manager for this session
            _database_managers[session_id] = db_manager
            
            await ctx.info("Database connection established successfully")
            
            return {
                "success": True,
                "message": "Successfully connected to database",
                "database_type": db_type,
                "host": connection_config['host'],
                "port": connection_config['port'],
                "database": connection_config['database'],
                "session_id": session_id
            }
        else:
            # Connection failed, raise appropriate exception
            raise DatabaseConnectionError(
                db_type=db_type,
                host=connection_config['host'],
                port=connection_config['port'],
                technical_details="Connection attempt returned False"
            )
            
    except DatabaseConnectionError as e:
        await ctx.error(f"Database connection failed: {e.user_message}")
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False)
        }
    except ConfigurationError as e:
        await ctx.error(f"Configuration error: {e.user_message}")
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False)
        }
    except Exception as e:
        # Unexpected error - convert to DatabaseConnectionError
        db_error = DatabaseConnectionError(
            db_type=db_type or "unknown",
            technical_details=str(e)
        )
        await ctx.error(f"Unexpected error during connection: {db_error.user_message}")
        return {
            "success": False,
            "error": db_error.to_dict(include_technical=config.debug if config else False)
        }


async def disconnect_database(ctx: Context) -> Dict[str, Any]:
    """
    Disconnect from the current database session.
    
    Returns:
        Dictionary with disconnection status
    """
    session_id = _get_session_id(ctx)
    
    try:
        if session_id in _database_managers:
            db_manager = _database_managers[session_id]
            await db_manager.disconnect()
            del _database_managers[session_id]
            
            await ctx.info("Database disconnected successfully")
            return {
                "success": True,
                "message": "Database disconnected successfully"
            }
        else:
            return {
                "success": False,
                "message": "No active database connection found",
                "suggestions": [
                    "Use connect_database tool to establish a connection first",
                    "Check if you're using the same session as when you connected"
                ]
            }
            
    except Exception as e:
        db_error = DatabaseConnectionError(
            db_type="unknown",
            technical_details=str(e)
        )
        db_error.user_message = "Failed to disconnect from database properly"
        db_error.suggestions = [
            "Connection may have been lost already",
            "Try reconnecting if you need to use the database again",
            "Check server logs for more details"
        ]
        
        await ctx.error(f"Disconnection error: {db_error.user_message}")
        return {
            "success": False,
            "error": db_error.to_dict(include_technical=config.debug if config else False)
        }


async def get_connection_status(ctx: Context) -> Dict[str, Any]:
    """
    Get the current database connection status.
    
    Returns:
        Dictionary with connection status information
    """
    session_id = _get_session_id(ctx)
    
    try:
        if session_id in _database_managers:
            db_manager = _database_managers[session_id]
            is_connected = await db_manager.test_connection()
            
            if is_connected:
                connection_info = db_manager.get_connection_info()
                return {
                    "connected": True,
                    "message": "Database connection is active",
                    "connection_info": connection_info
                }
            else:
                return {
                    "connected": False,
                    "message": "Database connection is not working",
                    "suggestions": [
                        "Try disconnecting and reconnecting to the database",
                        "Check if the database server is still running",
                        "Verify network connectivity"
                    ]
                }
        else:
            return {
                "connected": False,
                "message": "No database connection established",
                "suggestions": [
                    "Use connect_database tool to establish a connection first",
                    "Provide valid database credentials and configuration"
                ]
            }
            
    except Exception as e:
        db_error = DatabaseConnectionError(
            db_type="unknown",
            technical_details=str(e)
        )
        db_error.user_message = "Error checking database connection status"
        db_error.suggestions = [
            "Database connection may have been lost",
            "Try reconnecting to the database",
            "Check database server availability"
        ]
        
        await ctx.error(f"Error checking connection status: {db_error.user_message}")
        return {
            "connected": False,
            "error": db_error.to_dict(include_technical=config.debug if config else False)
        }


def get_database_manager(ctx: Context) -> Optional[BaseManager]:
    """
    Get the database manager for the current session.
    
    This is a utility function used by other tools.
    
    Returns:
        Database manager instance or None if not connected
    """
    session_id = _get_session_id(ctx)
    return _database_managers.get(session_id)
