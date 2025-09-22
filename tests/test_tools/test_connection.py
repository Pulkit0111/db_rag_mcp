"""
Tests for connection tools.

This module tests the database connection management tools
including connection, disconnection, and status checking.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from src.tools.connection import (
    connect_database,
    disconnect_database,
    get_connection_status,
    get_database_manager,
    _get_session_id
)
from src.core.exceptions import DatabaseConnectionError, ConfigurationError


@pytest.mark.unit
class TestConnectionUtilities:
    """Test connection utility functions."""
    
    def test_get_session_id_default(self):
        """Test getting default session ID."""
        ctx = MagicMock()
        # No session_id attribute
        session_id = _get_session_id(ctx)
        assert session_id == "default_session"
    
    def test_get_session_id_custom(self):
        """Test getting custom session ID."""
        ctx = MagicMock()
        ctx.session_id = "custom_session_123"
        session_id = _get_session_id(ctx)
        assert session_id == "custom_session_123"
    
    def test_get_database_manager_none(self, mock_context):
        """Test getting database manager when none exists."""
        manager = get_database_manager(mock_context)
        assert manager is None
    
    @patch('src.tools.connection._database_managers')
    def test_get_database_manager_exists(self, mock_managers, mock_context):
        """Test getting existing database manager."""
        mock_db = MagicMock()
        mock_managers.__getitem__ = MagicMock(return_value=mock_db)
        mock_managers.get.return_value = mock_db
        
        manager = get_database_manager(mock_context)
        assert manager == mock_db


@pytest.mark.unit
class TestConnectDatabase:
    """Test database connection functionality."""
    
    @patch('src.tools.connection.create_database_manager')
    @patch('src.tools.connection.config')
    async def test_connect_success(self, mock_config, mock_create_manager, mock_context):
        """Test successful database connection."""
        # Mock configuration
        mock_config.database.host = 'localhost'
        mock_config.database.port = 5432
        mock_config.database.username = 'test'
        mock_config.database.password = 'test'
        mock_config.database.database = 'testdb'
        mock_config.database.db_type = 'postgresql'
        mock_config.debug = False
        
        # Mock database manager
        mock_db_manager = AsyncMock()
        mock_db_manager.connect.return_value = True
        mock_create_manager.return_value = mock_db_manager
        
        result = await connect_database(mock_context)
        
        assert result["success"] is True
        assert result["database_type"] == "postgresql"
        assert result["host"] == "localhost"
        assert result["port"] == 5432
        
        mock_context.info.assert_called()
        mock_db_manager.connect.assert_called_once()
    
    @patch('src.tools.connection.config', None)
    async def test_connect_no_config(self, mock_context):
        """Test connection with no configuration."""
        result = await connect_database(mock_context)
        
        assert result["success"] is False
        assert "error" in result
        assert result["error"]["error_type"] == "ConfigurationError"
        
        mock_context.error.assert_called()
    
    @patch('src.tools.connection.create_database_manager')
    @patch('src.tools.connection.config')
    async def test_connect_invalid_db_type(self, mock_config, mock_create_manager, mock_context):
        """Test connection with invalid database type."""
        # Mock configuration
        mock_config.database.host = 'localhost'
        mock_config.database.port = 5432
        mock_config.database.username = 'test'
        mock_config.database.password = 'test'
        mock_config.database.database = 'testdb'
        mock_config.database.db_type = 'postgresql'
        mock_config.debug = False
        
        # Mock create_database_manager to raise error
        mock_create_manager.side_effect = ValueError("Unsupported database type")
        
        result = await connect_database(mock_context, db_type="unsupported")
        
        assert result["success"] is False
        assert "error" in result
        assert result["error"]["error_type"] == "DatabaseConnectionError"
    
    @patch('src.tools.connection.create_database_manager')
    @patch('src.tools.connection.config')
    async def test_connect_connection_failed(self, mock_config, mock_create_manager, mock_context):
        """Test connection failure."""
        # Mock configuration
        mock_config.database.host = 'localhost'
        mock_config.database.port = 5432
        mock_config.database.username = 'test'
        mock_config.database.password = 'test'
        mock_config.database.database = 'testdb'
        mock_config.database.db_type = 'postgresql'
        mock_config.debug = False
        
        # Mock database manager that fails to connect
        mock_db_manager = AsyncMock()
        mock_db_manager.connect.return_value = False
        mock_create_manager.return_value = mock_db_manager
        
        result = await connect_database(mock_context)
        
        assert result["success"] is False
        assert "error" in result
        assert result["error"]["error_type"] == "DatabaseConnectionError"
    
    @patch('src.tools.connection.create_database_manager')
    @patch('src.tools.connection.config')
    async def test_connect_custom_params(self, mock_config, mock_create_manager, mock_context):
        """Test connection with custom parameters."""
        # Mock configuration (will be overridden)
        mock_config.database.host = 'localhost'
        mock_config.database.port = 5432
        mock_config.database.username = 'test'
        mock_config.database.password = 'test'
        mock_config.database.database = 'testdb'
        mock_config.database.db_type = 'postgresql'
        mock_config.debug = False
        
        # Mock database manager
        mock_db_manager = AsyncMock()
        mock_db_manager.connect.return_value = True
        mock_create_manager.return_value = mock_db_manager
        
        result = await connect_database(
            mock_context,
            host="custom_host",
            port=3306,
            username="custom_user",
            password="custom_pass",
            database_name="custom_db",
            db_type="mysql"
        )
        
        assert result["success"] is True
        assert result["database_type"] == "mysql"
        assert result["host"] == "custom_host"
        assert result["port"] == 3306
        
        # Verify the create_database_manager was called with custom config
        call_args = mock_create_manager.call_args
        assert call_args[0][0] == "mysql"  # db_type
        config_dict = call_args[0][1]
        assert config_dict["host"] == "custom_host"
        assert config_dict["port"] == 3306
        assert config_dict["username"] == "custom_user"


@pytest.mark.unit
class TestDisconnectDatabase:
    """Test database disconnection functionality."""
    
    @patch('src.tools.connection._database_managers')
    @patch('src.tools.connection.config')
    async def test_disconnect_success(self, mock_config, mock_managers, mock_context):
        """Test successful disconnection."""
        mock_config.debug = False
        
        # Mock existing database manager
        mock_db_manager = AsyncMock()
        mock_managers.__contains__ = MagicMock(return_value=True)
        mock_managers.__getitem__ = MagicMock(return_value=mock_db_manager)
        mock_managers.__delitem__ = MagicMock()
        
        result = await disconnect_database(mock_context)
        
        assert result["success"] is True
        assert "successfully" in result["message"].lower()
        
        mock_db_manager.disconnect.assert_called_once()
        mock_context.info.assert_called()
    
    @patch('src.tools.connection._database_managers')
    async def test_disconnect_no_connection(self, mock_managers, mock_context):
        """Test disconnection when no connection exists."""
        mock_managers.__contains__ = MagicMock(return_value=False)
        
        result = await disconnect_database(mock_context)
        
        assert result["success"] is False
        assert "no active database connection" in result["message"].lower()
        assert "suggestions" in result
    
    @patch('src.tools.connection._database_managers')
    @patch('src.tools.connection.config')
    async def test_disconnect_error(self, mock_config, mock_managers, mock_context):
        """Test disconnection error handling."""
        mock_config.debug = False
        
        # Mock database manager that raises exception
        mock_db_manager = AsyncMock()
        mock_db_manager.disconnect.side_effect = Exception("Connection error")
        mock_managers.__contains__ = MagicMock(return_value=True)
        mock_managers.__getitem__ = MagicMock(return_value=mock_db_manager)
        
        result = await disconnect_database(mock_context)
        
        assert result["success"] is False
        assert "error" in result
        assert result["error"]["error_type"] == "DatabaseConnectionError"


@pytest.mark.unit
class TestConnectionStatus:
    """Test connection status functionality."""
    
    @patch('src.tools.connection._database_managers')
    @patch('src.tools.connection.config')
    async def test_status_connected(self, mock_config, mock_managers, mock_context):
        """Test status when connected."""
        mock_config.debug = False
        
        # Mock connected database manager
        mock_db_manager = AsyncMock()
        mock_db_manager.test_connection.return_value = True
        mock_db_manager.get_connection_info.return_value = {
            "host": "localhost",
            "port": 5432,
            "database": "testdb"
        }
        mock_managers.__contains__ = MagicMock(return_value=True)
        mock_managers.__getitem__ = MagicMock(return_value=mock_db_manager)
        
        result = await get_connection_status(mock_context)
        
        assert result["connected"] is True
        assert "active" in result["message"].lower()
        assert "connection_info" in result
        assert result["connection_info"]["host"] == "localhost"
    
    @patch('src.tools.connection._database_managers')
    async def test_status_not_connected(self, mock_managers, mock_context):
        """Test status when not connected."""
        mock_managers.__contains__ = MagicMock(return_value=True)
        mock_db_manager = AsyncMock()
        mock_db_manager.test_connection.return_value = False
        mock_managers.__getitem__ = MagicMock(return_value=mock_db_manager)
        
        result = await get_connection_status(mock_context)
        
        assert result["connected"] is False
        assert "not working" in result["message"].lower()
        assert "suggestions" in result
    
    @patch('src.tools.connection._database_managers')
    async def test_status_no_manager(self, mock_managers, mock_context):
        """Test status when no manager exists."""
        mock_managers.__contains__ = MagicMock(return_value=False)
        
        result = await get_connection_status(mock_context)
        
        assert result["connected"] is False
        assert "no database connection established" in result["message"].lower()
        assert "suggestions" in result
    
    @patch('src.tools.connection._database_managers')
    @patch('src.tools.connection.config')
    async def test_status_error(self, mock_config, mock_managers, mock_context):
        """Test status check error handling."""
        mock_config.debug = False
        
        # Mock database manager that raises exception
        mock_db_manager = AsyncMock()
        mock_db_manager.test_connection.side_effect = Exception("Connection error")
        mock_managers.__contains__ = MagicMock(return_value=True)
        mock_managers.__getitem__ = MagicMock(return_value=mock_db_manager)
        
        result = await get_connection_status(mock_context)
        
        assert result["connected"] is False
        assert "error" in result
        assert result["error"]["error_type"] == "DatabaseConnectionError"


@pytest.mark.integration
class TestConnectionIntegration:
    """Integration tests for connection tools."""
    
    async def test_full_connection_cycle_sqlite(self, test_sqlite_config, mock_context):
        """Test full connection cycle with SQLite."""
        # Connect
        result = await connect_database(
            mock_context,
            database_name=test_sqlite_config['database'],
            db_type='sqlite'
        )
        
        if result["success"]:
            # Check status
            status = await get_connection_status(mock_context)
            assert status["connected"] is True
            
            # Disconnect
            disconnect_result = await disconnect_database(mock_context)
            assert disconnect_result["success"] is True
            
            # Check status after disconnect
            final_status = await get_connection_status(mock_context)
            assert final_status["connected"] is False
