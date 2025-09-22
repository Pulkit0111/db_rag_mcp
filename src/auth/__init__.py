"""
Authentication and authorization module for the Natural Language SQL MCP Server.

This module provides comprehensive user management, authentication, and role-based
access control functionality.
"""

from .user_manager import (
    UserManager,
    User,
    UserSession,
    UserRole,
    Permission,
    user_manager
)

__all__ = [
    'UserManager',
    'User', 
    'UserSession',
    'UserRole',
    'Permission',
    'user_manager'
]
