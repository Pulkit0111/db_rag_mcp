"""
SQLite database manager implementation.

This module provides a concrete implementation of the BaseManager for SQLite databases
using the aiosqlite library for async database operations.
"""

import aiosqlite
import logging
from typing import List, Dict, Any, Optional
from .base_manager import BaseManager, TableSchema, QueryResult


logger = logging.getLogger(__name__)


class SQLiteManager(BaseManager):
    """SQLite database manager using aiosqlite."""
    
    def __init__(self, connection_config: Dict[str, Any]):
        """
        Initialize SQLite manager.
        
        Args:
            connection_config: Dict with keys: database (file path)
        """
        super().__init__(connection_config)
        self.connection = None
    
    async def connect(self) -> bool:
        """
        Establish connection to SQLite database.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            database_path = self.connection_config.get('database', ':memory:')
            self.connection = await aiosqlite.connect(database_path)
            
            # Enable foreign key support
            await self.connection.execute("PRAGMA foreign_keys = ON")
            
            # Test the connection
            async with self.connection.execute('SELECT 1') as cursor:
                result = await cursor.fetchone()
                
            self.is_connected = True
            logger.info(f"Successfully connected to SQLite database: {database_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to SQLite: {str(e)}")
            self.is_connected = False
            return False
    
    async def disconnect(self) -> None:
        """Close the database connection."""
        try:
            if self.connection:
                await self.connection.close()
                self.connection = None
            self.is_connected = False
            logger.info("Disconnected from SQLite database")
        except Exception as e:
            logger.error(f"Error disconnecting from SQLite: {str(e)}")
    
    async def execute_query(self, query: str, parameters: Optional[List[Any]] = None) -> QueryResult:
        """
        Execute a SQL query against SQLite.
        
        Args:
            query: The SQL query string
            parameters: Optional list of parameters for the query
            
        Returns:
            QueryResult containing the results or error information
        """
        if not self.is_connected or not self.connection:
            return QueryResult(
                success=False,
                data=[],
                row_count=0,
                error_message="Database not connected"
            )
        
        try:
            # Set row factory to return dictionaries
            self.connection.row_factory = aiosqlite.Row
            
            if parameters:
                async with self.connection.execute(query, parameters) as cursor:
                    if query.strip().upper().startswith('SELECT'):
                        rows = await cursor.fetchall()
                        data = [dict(row) for row in rows] if rows else []
                        row_count = len(data)
                    else:
                        data = []
                        row_count = cursor.rowcount
            else:
                async with self.connection.execute(query) as cursor:
                    if query.strip().upper().startswith('SELECT'):
                        rows = await cursor.fetchall()
                        data = [dict(row) for row in rows] if rows else []
                        row_count = len(data)
                    else:
                        data = []
                        row_count = cursor.rowcount
            
            # Commit changes for non-SELECT queries
            if not query.strip().upper().startswith('SELECT'):
                await self.connection.commit()
            
            return QueryResult(
                success=True,
                data=data,
                row_count=row_count
            )
                
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            return QueryResult(
                success=False,
                data=[],
                row_count=0,
                error_message=str(e)
            )
    
    async def get_tables(self) -> List[str]:
        """
        Get a list of all table names in the database.
        
        Returns:
            List of table names
        """
        query = """
        SELECT name 
        FROM sqlite_master 
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name;
        """
        
        result = await self.execute_query(query)
        if result.success:
            return [row['name'] for row in result.data]
        else:
            logger.error(f"Failed to get tables: {result.error_message}")
            return []
    
    async def get_table_schema(self, table_name: str) -> TableSchema:
        """
        Get the schema information for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            TableSchema object containing table structure information
        """
        try:
            # Get column information using PRAGMA table_info
            columns_query = f"PRAGMA table_info({table_name})"
            columns_result = await self.execute_query(columns_query)
            
            # Transform SQLite column info to match our format
            columns = []
            primary_keys = []
            
            if columns_result.success:
                for row in columns_result.data:
                    column_info = {
                        'column_name': row['name'],
                        'data_type': row['type'],
                        'is_nullable': 'NO' if row['notnull'] else 'YES',
                        'column_default': row['dflt_value'],
                        'character_maximum_length': None,
                        'numeric_precision': None,
                        'numeric_scale': None
                    }
                    columns.append(column_info)
                    
                    # Check if this is a primary key
                    if row['pk']:
                        primary_keys.append(row['name'])
            
            # Get foreign key information using PRAGMA foreign_key_list
            fk_query = f"PRAGMA foreign_key_list({table_name})"
            fk_result = await self.execute_query(fk_query)
            
            foreign_keys = []
            if fk_result.success:
                for row in fk_result.data:
                    fk_info = {
                        'column': row['from'],
                        'foreign_table': row['table'],
                        'foreign_column': row['to']
                    }
                    foreign_keys.append(fk_info)
            
            return TableSchema(
                table_name=table_name,
                columns=columns,
                primary_keys=primary_keys,
                foreign_keys=foreign_keys
            )
            
        except Exception as e:
            logger.error(f"Failed to get schema for table {table_name}: {str(e)}")
            return TableSchema(
                table_name=table_name,
                columns=[],
                primary_keys=[],
                foreign_keys=[]
            )
    
    async def test_connection(self) -> bool:
        """
        Test if the database connection is working.
        
        Returns:
            True if connection is working, False otherwise
        """
        if not self.is_connected or not self.connection:
            return False
        
        try:
            async with self.connection.execute('SELECT 1') as cursor:
                result = await cursor.fetchone()
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False
