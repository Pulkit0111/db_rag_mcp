"""
Base database manager abstract class.

This module defines the abstract interface that all database managers must implement.
It provides a consistent API for interacting with different SQL databases.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class TableSchema:
    """Represents the schema information for a database table."""
    table_name: str
    columns: List[Dict[str, Any]]  # List of column info dicts
    primary_keys: List[str]
    foreign_keys: List[Dict[str, str]]  # List of foreign key relationships


@dataclass
class QueryResult:
    """Represents the result of a database query."""
    success: bool
    data: List[Dict[str, Any]]
    row_count: int
    error_message: Optional[str] = None


class BaseManager(ABC):
    """Abstract base class for all database managers."""
    
    def __init__(self, connection_config: Dict[str, Any]):
        """
        Initialize the database manager with connection configuration.
        
        Args:
            connection_config: Dictionary containing connection parameters
        """
        self.connection_config = connection_config
        self.connection = None
        self.is_connected = False
    
    @abstractmethod
    async def connect(self) -> bool:
        """
        Establish a connection to the database.
        
        Returns:
            True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Close the database connection."""
        pass
    
    @abstractmethod
    async def execute_query(self, query: str, parameters: Optional[List[Any]] = None) -> QueryResult:
        """
        Execute a SQL query against the database.
        
        Args:
            query: The SQL query string
            parameters: Optional list of parameters for the query
            
        Returns:
            QueryResult containing the results or error information
        """
        pass
    
    @abstractmethod
    async def get_tables(self) -> List[str]:
        """
        Get a list of all table names in the database.
        
        Returns:
            List of table names
        """
        pass
    
    @abstractmethod
    async def get_table_schema(self, table_name: str) -> TableSchema:
        """
        Get the schema information for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            TableSchema object containing table structure information
        """
        pass
    
    @abstractmethod
    async def test_connection(self) -> bool:
        """
        Test if the database connection is working.
        
        Returns:
            True if connection is working, False otherwise
        """
        pass
    
    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get sanitized connection information (without password).
        
        Returns:
            Dictionary with connection info
        """
        info = self.connection_config.copy()
        if 'password' in info:
            info['password'] = '***'
        return info
