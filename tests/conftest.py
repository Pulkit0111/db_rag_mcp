"""
Test configuration and fixtures for the Natural Language SQL MCP Server.

This module provides pytest fixtures for database connections, sample data,
and other test utilities across the entire test suite.
"""

import pytest
import asyncio
import os
import tempfile
from typing import Dict, Any, List
from unittest.mock import AsyncMock, MagicMock

# Import the components we need to test
from src.database.postgres_manager import PostgresManager
from src.database.mysql_manager import MySQLManager
from src.database.sqlite_manager import SQLiteManager
from src.database import create_database_manager
from src.core.config import Config, DatabaseConfig, LLMConfig, ServerConfig
from src.core.exceptions import *


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the test session."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture
def test_db_config():
    """Test database configuration for PostgreSQL."""
    return {
        'host': os.getenv('TEST_DB_HOST', 'localhost'),
        'port': int(os.getenv('TEST_DB_PORT', '5432')),
        'username': os.getenv('TEST_DB_USERNAME', 'postgres'),
        'password': os.getenv('TEST_DB_PASSWORD', 'password'),
        'database': os.getenv('TEST_DB_NAME', 'test_nlp_sql_db')
    }


@pytest.fixture
def test_mysql_config():
    """Test database configuration for MySQL."""
    return {
        'host': os.getenv('TEST_MYSQL_HOST', 'localhost'),
        'port': int(os.getenv('TEST_MYSQL_PORT', '3306')),
        'username': os.getenv('TEST_MYSQL_USERNAME', 'root'),
        'password': os.getenv('TEST_MYSQL_PASSWORD', 'password'),
        'database': os.getenv('TEST_MYSQL_NAME', 'test_nlp_sql_db')
    }


@pytest.fixture
def test_sqlite_config():
    """Test database configuration for SQLite."""
    temp_dir = tempfile.gettempdir()
    return {
        'database': os.path.join(temp_dir, 'test_nlp_sql.db')
    }


@pytest.fixture
async def postgres_db(test_db_config):
    """
    Create a test PostgreSQL database connection.
    
    Note: This requires a running PostgreSQL instance.
    Skip tests if not available.
    """
    try:
        db_manager = PostgresManager(test_db_config)
        success = await db_manager.connect()
        
        if not success:
            pytest.skip("PostgreSQL test database not available")
        
        # Create test tables
        await _create_test_tables(db_manager)
        
        yield db_manager
        
        # Cleanup
        await _cleanup_test_tables(db_manager)
        await db_manager.disconnect()
    except Exception as e:
        pytest.skip(f"PostgreSQL setup failed: {str(e)}")


@pytest.fixture
async def mysql_db(test_mysql_config):
    """
    Create a test MySQL database connection.
    
    Note: This requires a running MySQL instance.
    Skip tests if not available.
    """
    try:
        db_manager = MySQLManager(test_mysql_config)
        success = await db_manager.connect()
        
        if not success:
            pytest.skip("MySQL test database not available")
        
        # Create test tables
        await _create_test_tables(db_manager)
        
        yield db_manager
        
        # Cleanup
        await _cleanup_test_tables(db_manager)
        await db_manager.disconnect()
    except Exception as e:
        pytest.skip(f"MySQL setup failed: {str(e)}")


@pytest.fixture
async def sqlite_db(test_sqlite_config):
    """Create a test SQLite database connection."""
    try:
        # Remove existing test database
        if os.path.exists(test_sqlite_config['database']):
            os.remove(test_sqlite_config['database'])
        
        db_manager = SQLiteManager(test_sqlite_config)
        success = await db_manager.connect()
        
        if not success:
            pytest.skip("SQLite database setup failed")
        
        # Create test tables
        await _create_test_tables(db_manager)
        
        yield db_manager
        
        # Cleanup
        await db_manager.disconnect()
        
        # Remove test database file
        if os.path.exists(test_sqlite_config['database']):
            os.remove(test_sqlite_config['database'])
    except Exception as e:
        pytest.skip(f"SQLite setup failed: {str(e)}")


@pytest.fixture
def mock_db_manager():
    """Create a mock database manager for unit tests."""
    manager = AsyncMock()
    manager.is_connected = True
    manager.test_connection.return_value = True
    manager.get_tables.return_value = ['users', 'orders', 'products']
    
    # Mock schema data
    from src.database.base_manager import TableSchema
    manager.get_table_schema.return_value = TableSchema(
        table_name='users',
        columns=[
            {
                'column_name': 'id',
                'data_type': 'integer',
                'is_nullable': 'NO'
            },
            {
                'column_name': 'name',
                'data_type': 'varchar',
                'is_nullable': 'YES'
            },
            {
                'column_name': 'email',
                'data_type': 'varchar',
                'is_nullable': 'YES'
            }
        ],
        primary_keys=['id'],
        foreign_keys=[]
    )
    
    return manager


@pytest.fixture
def sample_data():
    """Sample test data for database operations."""
    return [
        {"id": 1, "name": "Alice Johnson", "email": "alice@example.com", "age": 28},
        {"id": 2, "name": "Bob Smith", "email": "bob@example.com", "age": 34},
        {"id": 3, "name": "Carol Wilson", "email": "carol@example.com", "age": 22},
        {"id": 4, "name": "David Brown", "email": "david@example.com", "age": 45},
        {"id": 5, "name": "Eve Davis", "email": "eve@example.com", "age": 31}
    ]


@pytest.fixture
def test_config():
    """Create test configuration with safe defaults."""
    return Config(
        environment="test",
        debug=True,
        enable_query_caching=False,  # Disable for tests
        enable_query_history=True,
        enable_smart_suggestions=False,  # Disable AI features for tests
        enable_visualization=True,
        enable_authentication=False,
        cache_redis_url="redis://localhost:6379/1",  # Use test database
        cache_ttl=60,  # Short TTL for tests
        query_timeout=10,  # Short timeout for tests
        max_result_rows=100  # Small limit for tests
    )


@pytest.fixture
def mock_context():
    """Create a mock FastMCP context for testing tools."""
    context = MagicMock()
    context.session_id = "test_session"
    
    # Mock async methods
    context.info = AsyncMock()
    context.warning = AsyncMock()
    context.error = AsyncMock()
    
    return context


@pytest.fixture
def mock_translator():
    """Create a mock translator for NLP testing."""
    translator = AsyncMock()
    
    # Mock successful translation
    translator.translate_to_select.return_value = {
        "success": True,
        "sql_query": "SELECT * FROM users WHERE name LIKE '%test%'",
        "explanation": "Find users with 'test' in their name"
    }
    
    # Mock insert translation
    translator.translate_to_insert.return_value = {
        "success": True,
        "sql_query": "INSERT INTO users (name, email) VALUES ('Test User', 'test@example.com')",
        "explanation": "Insert a new user"
    }
    
    # Mock update translation
    translator.translate_to_update.return_value = {
        "success": True,
        "sql_query": "UPDATE users SET email = 'newemail@example.com' WHERE id = 1",
        "explanation": "Update user email"
    }
    
    # Mock delete translation
    translator.translate_to_delete.return_value = {
        "success": True,
        "sql_query": "DELETE FROM users WHERE id = 1",
        "explanation": "Delete user by ID"
    }
    
    return translator


async def _create_test_tables(db_manager):
    """Create test tables in the database."""
    # Users table
    users_sql = """
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(150) UNIQUE,
        age INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    # Orders table
    orders_sql = """
    CREATE TABLE IF NOT EXISTS orders (
        id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES users(id),
        product_name VARCHAR(200) NOT NULL,
        quantity INTEGER DEFAULT 1,
        price DECIMAL(10, 2),
        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    # Products table
    products_sql = """
    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        description TEXT,
        price DECIMAL(10, 2) NOT NULL,
        stock INTEGER DEFAULT 0,
        category VARCHAR(100)
    )
    """
    
    # Adjust SQL for different database types
    if isinstance(db_manager, MySQLManager):
        users_sql = users_sql.replace('SERIAL', 'INT AUTO_INCREMENT').replace('TIMESTAMP DEFAULT CURRENT_TIMESTAMP', 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP()')
        orders_sql = orders_sql.replace('SERIAL', 'INT AUTO_INCREMENT').replace('TIMESTAMP DEFAULT CURRENT_TIMESTAMP', 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP()')
        products_sql = products_sql.replace('SERIAL', 'INT AUTO_INCREMENT')
    elif isinstance(db_manager, SQLiteManager):
        users_sql = users_sql.replace('SERIAL', 'INTEGER').replace('VARCHAR', 'TEXT').replace('TIMESTAMP DEFAULT CURRENT_TIMESTAMP', 'DATETIME DEFAULT CURRENT_TIMESTAMP')
        orders_sql = orders_sql.replace('SERIAL', 'INTEGER').replace('VARCHAR', 'TEXT').replace('DECIMAL(10, 2)', 'REAL').replace('TIMESTAMP DEFAULT CURRENT_TIMESTAMP', 'DATETIME DEFAULT CURRENT_TIMESTAMP')
        products_sql = products_sql.replace('SERIAL', 'INTEGER').replace('VARCHAR', 'TEXT').replace('DECIMAL(10, 2)', 'REAL')
    
    # Execute table creation
    await db_manager.execute_query(users_sql)
    await db_manager.execute_query(orders_sql)
    await db_manager.execute_query(products_sql)
    
    # Insert sample data
    sample_users = [
        "INSERT INTO users (name, email, age) VALUES ('Alice Johnson', 'alice@example.com', 28)",
        "INSERT INTO users (name, email, age) VALUES ('Bob Smith', 'bob@example.com', 34)",
        "INSERT INTO users (name, email, age) VALUES ('Carol Wilson', 'carol@example.com', 22)"
    ]
    
    for user_sql in sample_users:
        await db_manager.execute_query(user_sql)


async def _cleanup_test_tables(db_manager):
    """Clean up test tables from the database."""
    cleanup_queries = [
        "DROP TABLE IF EXISTS orders",
        "DROP TABLE IF EXISTS products", 
        "DROP TABLE IF EXISTS users"
    ]
    
    for query in cleanup_queries:
        try:
            await db_manager.execute_query(query)
        except Exception:
            pass  # Ignore cleanup errors


# Pytest markers
def pytest_configure(config):
    """Configure custom pytest markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test (requires database)"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as unit test (no external dependencies)"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "requires_redis: mark test as requiring Redis"
    )
    config.addinivalue_line(
        "markers", "requires_llm: mark test as requiring LLM API access"
    )
