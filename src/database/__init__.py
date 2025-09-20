"""
Database package initialization and factory functions.

This module provides factory functions to create appropriate database managers
based on the database type specified in configuration.
"""

from typing import Dict, Any, List
from .base_manager import BaseManager, TableSchema, QueryResult
from .postgres_manager import PostgresManager


class DatabaseManagerFactory:
    """Factory class for creating database managers."""
    
    @staticmethod
    def create_manager(db_type: str, connection_config: Dict[str, Any]) -> BaseManager:
        """
        Create and return appropriate database manager based on database type.
        
        Args:
            db_type: Type of database ('postgresql', 'mysql', etc.)
            connection_config: Connection configuration dictionary
            
        Returns:
            Database manager instance
            
        Raises:
            ValueError: If unsupported database type is specified
        """
        db_type_lower = db_type.lower()
        
        if db_type_lower == 'postgresql' or db_type_lower == 'postgres':
            return PostgresManager(connection_config)
        elif db_type_lower == 'mysql':
            # TODO: Implement MySQL manager in future
            raise ValueError(f"MySQL support not yet implemented")
        elif db_type_lower == 'sqlite':
            # TODO: Implement SQLite manager in future  
            raise ValueError(f"SQLite support not yet implemented")
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    @staticmethod
    def get_supported_databases() -> List[str]:
        """
        Get list of supported database types.
        
        Returns:
            List of supported database type strings
        """
        return ['postgresql', 'postgres']


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
    'DatabaseManagerFactory',
    'create_database_manager'
]
