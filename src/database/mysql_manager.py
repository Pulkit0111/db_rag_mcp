"""
MySQL database manager implementation.

This module provides a concrete implementation of the BaseManager for MySQL databases
using the aiomysql library for async database operations.
"""

import aiomysql
import logging
from typing import List, Dict, Any, Optional
from .base_manager import BaseManager, TableSchema, QueryResult


logger = logging.getLogger(__name__)


class MySQLManager(BaseManager):
    """MySQL database manager using aiomysql."""
    
    def __init__(self, connection_config: Dict[str, Any]):
        """
        Initialize MySQL manager.
        
        Args:
            connection_config: Dict with keys: host, port, username, password, database
        """
        super().__init__(connection_config)
        self.connection_pool = None
    
    async def connect(self) -> bool:
        """
        Establish connection to MySQL database.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Create connection pool for better performance
            self.connection_pool = await aiomysql.create_pool(
                host=self.connection_config['host'],
                port=self.connection_config['port'],
                user=self.connection_config['username'],
                password=self.connection_config['password'],
                db=self.connection_config['database'],
                minsize=1,
                maxsize=5,
                autocommit=True  # Enable autocommit for MySQL
            )
            
            # Test the connection
            async with self.connection_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute('SELECT 1')
                    result = await cursor.fetchone()
                    
            self.is_connected = True
            logger.info(f"Successfully connected to MySQL database: {self.connection_config['database']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to MySQL: {str(e)}")
            self.is_connected = False
            return False
    
    async def disconnect(self) -> None:
        """Close the database connection pool."""
        try:
            if self.connection_pool:
                self.connection_pool.close()
                await self.connection_pool.wait_closed()
                self.connection_pool = None
            self.is_connected = False
            logger.info("Disconnected from MySQL database")
        except Exception as e:
            logger.error(f"Error disconnecting from MySQL: {str(e)}")
    
    async def execute_query(self, query: str, parameters: Optional[List[Any]] = None) -> QueryResult:
        """
        Execute a SQL query against MySQL.
        
        Args:
            query: The SQL query string
            parameters: Optional list of parameters for the query
            
        Returns:
            QueryResult containing the results or error information
        """
        if not self.is_connected or not self.connection_pool:
            return QueryResult(
                success=False,
                data=[],
                row_count=0,
                error_message="Database not connected"
            )
        
        try:
            async with self.connection_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    if parameters:
                        await cursor.execute(query, parameters)
                    else:
                        await cursor.execute(query)
                    
                    # Fetch all results for SELECT queries
                    if query.strip().upper().startswith('SELECT'):
                        result = await cursor.fetchall()
                        data = list(result) if result else []
                    else:
                        # For INSERT/UPDATE/DELETE, return affected rows
                        data = []
                        
                    return QueryResult(
                        success=True,
                        data=data,
                        row_count=len(data) if data else cursor.rowcount
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
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name;
        """
        
        result = await self.execute_query(query, [self.connection_config['database']])
        if result.success:
            return [row['table_name'] for row in result.data]
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
        # Query for column information
        columns_query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            column_key,
            extra
        FROM information_schema.columns 
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position;
        """
        
        # Query for primary keys
        pk_query = """
        SELECT column_name
        FROM information_schema.key_column_usage
        WHERE table_schema = %s 
        AND table_name = %s 
        AND constraint_name = 'PRIMARY';
        """
        
        # Query for foreign keys
        fk_query = """
        SELECT 
            kcu.column_name,
            kcu.referenced_table_name AS foreign_table_name,
            kcu.referenced_column_name AS foreign_column_name
        FROM information_schema.key_column_usage kcu
        WHERE kcu.table_schema = %s 
        AND kcu.table_name = %s 
        AND kcu.referenced_table_name IS NOT NULL;
        """
        
        try:
            db_name = self.connection_config['database']
            
            # Get columns info
            columns_result = await self.execute_query(columns_query, [db_name, table_name])
            columns = columns_result.data if columns_result.success else []
            
            # Get primary keys
            pk_result = await self.execute_query(pk_query, [db_name, table_name])
            primary_keys = [row['column_name'] for row in pk_result.data] if pk_result.success else []
            
            # Get foreign keys
            fk_result = await self.execute_query(fk_query, [db_name, table_name])
            foreign_keys = []
            if fk_result.success:
                foreign_keys = [
                    {
                        'column': row['column_name'],
                        'foreign_table': row['foreign_table_name'],
                        'foreign_column': row['foreign_column_name']
                    }
                    for row in fk_result.data
                ]
            
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
        if not self.is_connected or not self.connection_pool:
            return False
        
        try:
            async with self.connection_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute('SELECT 1')
                    await cursor.fetchone()
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False
