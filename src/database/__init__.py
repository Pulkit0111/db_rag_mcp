"""
Database package initialization and factory functions.

This module provides factory functions to create appropriate database managers
based on the database type specified in configuration.
"""

from typing import Dict, Any, List
from .base_manager import BaseManager, TableSchema, QueryResult
from .postgres_manager import PostgresManager
from .mysql_manager import MySQLManager
from .sqlite_manager import SQLiteManager


class DatabaseManagerFactory:
    """Factory class for creating database managers."""
    
    @staticmethod
    def create_manager(db_type: str, connection_config: Dict[str, Any]) -> BaseManager:
        """
        Create and return appropriate database manager based on database type.
        
        Args:
            db_type: Type of database ('postgresql', 'mysql', 'sqlite', etc.)
            connection_config: Connection configuration dictionary
            
        Returns:
            Database manager instance
            
        Raises:
            ValueError: If unsupported database type is specified
        """
        db_type_lower = db_type.lower()
        
        if db_type_lower in ['postgresql', 'postgres']:
            return PostgresManager(connection_config)
        elif db_type_lower == 'mysql':
            return MySQLManager(connection_config)
        elif db_type_lower == 'sqlite':
            return SQLiteManager(connection_config)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    @staticmethod
    def get_supported_databases() -> List[str]:
        """
        Get list of supported database types.
        
        Returns:
            List of supported database type strings
        """
        return ['postgresql', 'postgres', 'mysql', 'sqlite']


# Convenience function for creating database managers
def create_database_manager(db_type: str, connection_config: Dict[str, Any]) -> BaseManager:
    """
    Convenience function to create a database manager.
    
    Args:
        db_type: Type of database
        connection_config: Connection configuration
        
    Returns:
        Database manager instance
    """
    return DatabaseManagerFactory.create_manager(db_type, connection_config)


# Export commonly used classes and functions
__all__ = [
    'BaseManager',
    'TableSchema', 
    'QueryResult',
    'PostgresManager',
    'MySQLManager',
    'SQLiteManager',
    'DatabaseManagerFactory',
    'create_database_manager'
]
