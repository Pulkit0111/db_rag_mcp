"""
Tests for database managers.

This module contains unit and integration tests for the database manager
implementations including PostgreSQL, MySQL, and SQLite.
"""

import pytest
import os
from unittest.mock import AsyncMock, patch

from src.database import create_database_manager, DatabaseManagerFactory
from src.database.postgres_manager import PostgresManager
from src.database.mysql_manager import MySQLManager
from src.database.sqlite_manager import SQLiteManager
from src.database.base_manager import QueryResult, TableSchema


class TestDatabaseManagerFactory:
    """Test the database manager factory."""
    
    def test_create_postgresql_manager(self):
        """Test creating PostgreSQL manager."""
        config = {
            'host': 'localhost',
            'port': 5432,
            'username': 'test',
            'password': 'test',
            'database': 'test'
        }
        
        manager = create_database_manager('postgresql', config)
        assert isinstance(manager, PostgresManager)
        
        manager = create_database_manager('postgres', config)
        assert isinstance(manager, PostgresManager)
    
    def test_create_mysql_manager(self):
        """Test creating MySQL manager."""
        config = {
            'host': 'localhost',
            'port': 3306,
            'username': 'test',
            'password': 'test',
            'database': 'test'
        }
        
        manager = create_database_manager('mysql', config)
        assert isinstance(manager, MySQLManager)
    
    def test_create_sqlite_manager(self):
        """Test creating SQLite manager."""
        config = {
            'database': '/tmp/test.db'
        }
        
        manager = create_database_manager('sqlite', config)
        assert isinstance(manager, SQLiteManager)
    
    def test_unsupported_database_type(self):
        """Test error for unsupported database type."""
        config = {'host': 'localhost'}
        
        with pytest.raises(ValueError, match="Unsupported database type"):
            create_database_manager('mongodb', config)
    
    def test_get_supported_databases(self):
        """Test getting list of supported databases."""
        supported = DatabaseManagerFactory.get_supported_databases()
        expected = ['postgresql', 'postgres', 'mysql', 'sqlite']
        assert supported == expected


@pytest.mark.unit
class TestPostgresManager:
    """Test PostgreSQL database manager."""
    
    def test_init(self):
        """Test PostgreSQL manager initialization."""
        config = {
            'host': 'localhost',
            'port': 5432,
            'username': 'test',
            'password': 'test',
            'database': 'test'
        }
        
        manager = PostgresManager(config)
        assert manager.connection_config == config
        assert not manager.is_connected
        assert manager.connection_pool is None
    
    def test_get_connection_info(self):
        """Test getting sanitized connection info."""
        config = {
            'host': 'localhost',
            'port': 5432,
            'username': 'test',
            'password': 'secret',
            'database': 'test'
        }
        
        manager = PostgresManager(config)
        info = manager.get_connection_info()
        
        assert info['host'] == 'localhost'
        assert info['port'] == 5432
        assert info['username'] == 'test'
        assert info['password'] == '***'  # Sanitized
        assert info['database'] == 'test'


@pytest.mark.unit
class TestMySQLManager:
    """Test MySQL database manager."""
    
    def test_init(self):
        """Test MySQL manager initialization."""
        config = {
            'host': 'localhost',
            'port': 3306,
            'username': 'test',
            'password': 'test',
            'database': 'test'
        }
        
        manager = MySQLManager(config)
        assert manager.connection_config == config
        assert not manager.is_connected
        assert manager.connection_pool is None


@pytest.mark.unit 
class TestSQLiteManager:
    """Test SQLite database manager."""
    
    def test_init(self):
        """Test SQLite manager initialization."""
        config = {
            'database': '/tmp/test.db'
        }
        
        manager = SQLiteManager(config)
        assert manager.connection_config == config
        assert not manager.is_connected
        assert manager.connection is None


@pytest.mark.integration
class TestPostgresIntegration:
    """Integration tests for PostgreSQL manager."""
    
    async def test_connection(self, postgres_db):
        """Test PostgreSQL connection."""
        assert postgres_db.is_connected
        assert await postgres_db.test_connection()
    
    async def test_get_tables(self, postgres_db):
        """Test getting table list."""
        tables = await postgres_db.get_tables()
        assert isinstance(tables, list)
        assert 'users' in tables
        assert 'orders' in tables
    
    async def test_get_table_schema(self, postgres_db):
        """Test getting table schema."""
        schema = await postgres_db.get_table_schema('users')
        
        assert isinstance(schema, TableSchema)
        assert schema.table_name == 'users'
        assert len(schema.columns) > 0
        assert 'id' in schema.primary_keys
    
    async def test_execute_query_select(self, postgres_db):
        """Test executing SELECT query."""
        result = await postgres_db.execute_query("SELECT * FROM users LIMIT 5")
        
        assert isinstance(result, QueryResult)
        assert result.success
        assert len(result.data) <= 5
        assert result.row_count == len(result.data)
    
    async def test_execute_query_invalid(self, postgres_db):
        """Test executing invalid query."""
        result = await postgres_db.execute_query("SELECT * FROM nonexistent_table")
        
        assert isinstance(result, QueryResult)
        assert not result.success
        assert result.error_message is not None
        assert result.row_count == 0


@pytest.mark.integration
class TestMySQLIntegration:
    """Integration tests for MySQL manager."""
    
    async def test_connection(self, mysql_db):
        """Test MySQL connection."""
        assert mysql_db.is_connected
        assert await mysql_db.test_connection()
    
    async def test_get_tables(self, mysql_db):
        """Test getting table list."""
        tables = await mysql_db.get_tables()
        assert isinstance(tables, list)
        assert 'users' in tables
    
    async def test_execute_query(self, mysql_db):
        """Test executing query."""
        result = await mysql_db.execute_query("SELECT COUNT(*) as count FROM users")
        
        assert result.success
        assert len(result.data) == 1
        assert 'count' in result.data[0]


@pytest.mark.integration
class TestSQLiteIntegration:
    """Integration tests for SQLite manager."""
    
    async def test_connection(self, sqlite_db):
        """Test SQLite connection."""
        assert sqlite_db.is_connected
        assert await sqlite_db.test_connection()
    
    async def test_get_tables(self, sqlite_db):
        """Test getting table list."""
        tables = await sqlite_db.get_tables()
        assert isinstance(tables, list)
        assert 'users' in tables
    
    async def test_crud_operations(self, sqlite_db):
        """Test CRUD operations."""
        # Insert
        insert_result = await sqlite_db.execute_query(
            "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
            ['Test User', 'test@example.com', 25]
        )
        assert insert_result.success
        
        # Select
        select_result = await sqlite_db.execute_query(
            "SELECT * FROM users WHERE email = ?",
            ['test@example.com']
        )
        assert select_result.success
        assert len(select_result.data) == 1
        assert select_result.data[0]['name'] == 'Test User'
        
        # Update
        user_id = select_result.data[0]['id']
        update_result = await sqlite_db.execute_query(
            "UPDATE users SET age = ? WHERE id = ?",
            [30, user_id]
        )
        assert update_result.success
        
        # Verify update
        verify_result = await sqlite_db.execute_query(
            "SELECT age FROM users WHERE id = ?",
            [user_id]
        )
        assert verify_result.success
        assert verify_result.data[0]['age'] == 30
        
        # Delete
        delete_result = await sqlite_db.execute_query(
            "DELETE FROM users WHERE id = ?",
            [user_id]
        )
        assert delete_result.success


@pytest.mark.unit
class TestQueryResult:
    """Test QueryResult data class."""
    
    def test_successful_result(self):
        """Test successful query result."""
        data = [{'id': 1, 'name': 'Test'}]
        result = QueryResult(
            success=True,
            data=data,
            row_count=1
        )
        
        assert result.success
        assert result.data == data
        assert result.row_count == 1
        assert result.error_message is None
    
    def test_failed_result(self):
        """Test failed query result."""
        result = QueryResult(
            success=False,
            data=[],
            row_count=0,
            error_message="Table does not exist"
        )
        
        assert not result.success
        assert result.data == []
        assert result.row_count == 0
        assert result.error_message == "Table does not exist"


@pytest.mark.unit
class TestTableSchema:
    """Test TableSchema data class."""
    
    def test_complete_schema(self):
        """Test complete table schema."""
        columns = [
            {
                'column_name': 'id',
                'data_type': 'integer',
                'is_nullable': 'NO'
            },
            {
                'column_name': 'name',
                'data_type': 'varchar',
                'is_nullable': 'YES'
            }
        ]
        
        foreign_keys = [
            {
                'column': 'user_id',
                'foreign_table': 'users',
                'foreign_column': 'id'
            }
        ]
        
        schema = TableSchema(
            table_name='orders',
            columns=columns,
            primary_keys=['id'],
            foreign_keys=foreign_keys
        )
        
        assert schema.table_name == 'orders'
        assert len(schema.columns) == 2
        assert schema.primary_keys == ['id']
        assert len(schema.foreign_keys) == 1
        assert schema.foreign_keys[0]['column'] == 'user_id'
