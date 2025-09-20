"""
User authentication and authorization management for the Natural Language SQL MCP Server.

This module provides comprehensive user management including authentication, role-based
access control (RBAC), and session management with integration to the existing codebase.
"""

import hashlib
import secrets
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import json

from ..core.config import config
from ..core.exceptions import ValidationError, ConfigurationError


class UserRole(Enum):
    """User roles for role-based access control."""
    
    ADMIN = "admin"           # Full access to all operations
    ANALYST = "analyst"       # Read access + advanced query features
    USER = "user"             # Basic query access
    VIEWER = "viewer"         # Read-only access


class Permission(Enum):
    """System permissions that can be granted to roles."""
    
    # Database operations
    CONNECT_DATABASE = "connect_database"
    DISCONNECT_DATABASE = "disconnect_database"
    
    # Query operations  
    QUERY_DATA = "query_data"
    ADD_DATA = "add_data"
    UPDATE_DATA = "update_data"
    DELETE_DATA = "delete_data"
    
    # Schema operations
    LIST_TABLES = "list_tables"
    DESCRIBE_TABLE = "describe_table"
    GET_DATABASE_SUMMARY = "get_database_summary"
    
    # Advanced features
    EXPLAIN_QUERY = "explain_query"
    QUERY_WITH_SUGGESTIONS = "query_with_suggestions"
    AGGREGATE_DATA = "aggregate_data"
    
    # History and session
    GET_QUERY_HISTORY = "get_query_history"
    REPEAT_QUERY = "repeat_query"
    
    # AI features
    EXPLAIN_RESULTS = "explain_results"
    SUGGEST_RELATED_QUERIES = "suggest_related_queries"
    OPTIMIZE_QUERY = "optimize_query"
    IMPROVE_QUERY_LANGUAGE = "improve_query_language"
    ANALYZE_QUERY_INTENT = "analyze_query_intent"
    
    # Visualization and export
    CREATE_VISUALIZATION = "create_visualization"
    EXPORT_DATA = "export_data"
    
    # User management (admin only)
    CREATE_USER = "create_user"
    UPDATE_USER = "update_user"
    DELETE_USER = "delete_user"
    LIST_USERS = "list_users"
    MANAGE_ROLES = "manage_roles"


@dataclass
class UserSession:
    """Represents an active user session."""
    
    session_id: str
    user_id: str
    username: str
    role: UserRole
    permissions: Set[Permission]
    created_at: datetime
    last_activity: datetime
    expires_at: datetime
    database_connections: Dict[str, Any] = field(default_factory=dict)
    
    def is_expired(self) -> bool:
        """Check if session has expired."""
        return datetime.now() > self.expires_at
    
    def has_permission(self, permission: Permission) -> bool:
        """Check if session has specific permission."""
        return permission in self.permissions
    
    def update_activity(self) -> None:
        """Update last activity timestamp."""
        self.last_activity = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "session_id": self.session_id,
            "user_id": self.user_id,
            "username": self.username,
            "role": self.role.value,
            "permissions": [p.value for p in self.permissions],
            "created_at": self.created_at.isoformat(),
            "last_activity": self.last_activity.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "database_connections": self.database_connections
        }


@dataclass
class User:
    """Represents a user in the system."""
    
    user_id: str
    username: str
    email: str
    password_hash: str
    salt: str
    role: UserRole
    created_at: datetime
    updated_at: datetime
    is_active: bool = True
    failed_login_attempts: int = 0
    last_login: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def verify_password(self, password: str) -> bool:
        """Verify password against stored hash."""
        return self._hash_password(password, self.salt) == self.password_hash
    
    def update_password(self, new_password: str) -> None:
        """Update user password with new hash and salt."""
        self.salt = secrets.token_hex(32)
        self.password_hash = self._hash_password(new_password, self.salt)
        self.updated_at = datetime.now()
    
    def record_login(self, successful: bool = True) -> None:
        """Record login attempt."""
        if successful:
            self.last_login = datetime.now()
            self.failed_login_attempts = 0
        else:
            self.failed_login_attempts += 1
        self.updated_at = datetime.now()
    
    def is_locked(self, max_attempts: int = 5) -> bool:
        """Check if user account is locked due to failed attempts."""
        return self.failed_login_attempts >= max_attempts
    
    def to_dict(self, include_sensitive: bool = False) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        data = {
            "user_id": self.user_id,
            "username": self.username,
            "email": self.email,
            "role": self.role.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "is_active": self.is_active,
            "last_login": self.last_login.isoformat() if self.last_login else None,
            "metadata": self.metadata
        }
        
        if include_sensitive:
            data.update({
                "failed_login_attempts": self.failed_login_attempts,
                "password_hash": self.password_hash,
                "salt": self.salt
            })
        
        return data
    
    @staticmethod
    def _hash_password(password: str, salt: str) -> str:
        """Hash password with salt using SHA-256."""
        return hashlib.sha256((password + salt).encode()).hexdigest()


class UserManager:
    """
    Manages user authentication, authorization, and sessions.
    
    Provides functionality for:
    - User creation and management
    - Authentication and session management
    - Role-based access control
    - Permission checking
    """
    
    # Role-based permissions mapping
    ROLE_PERMISSIONS = {
        UserRole.VIEWER: {
            Permission.CONNECT_DATABASE,
            Permission.LIST_TABLES,
            Permission.DESCRIBE_TABLE,
            Permission.GET_DATABASE_SUMMARY,
            Permission.QUERY_DATA,
            Permission.GET_QUERY_HISTORY,
        },
        UserRole.USER: {
            Permission.CONNECT_DATABASE,
            Permission.DISCONNECT_DATABASE,
            Permission.LIST_TABLES,
            Permission.DESCRIBE_TABLE,
            Permission.GET_DATABASE_SUMMARY,
            Permission.QUERY_DATA,
            Permission.GET_QUERY_HISTORY,
            Permission.REPEAT_QUERY,
            Permission.EXPLAIN_QUERY,
            Permission.AGGREGATE_DATA,
        },
        UserRole.ANALYST: {
            # All USER permissions plus advanced features
            Permission.CONNECT_DATABASE,
            Permission.DISCONNECT_DATABASE,
            Permission.LIST_TABLES,
            Permission.DESCRIBE_TABLE,
            Permission.GET_DATABASE_SUMMARY,
            Permission.QUERY_DATA,
            Permission.ADD_DATA,
            Permission.UPDATE_DATA,
            Permission.GET_QUERY_HISTORY,
            Permission.REPEAT_QUERY,
            Permission.EXPLAIN_QUERY,
            Permission.QUERY_WITH_SUGGESTIONS,
            Permission.AGGREGATE_DATA,
            Permission.EXPLAIN_RESULTS,
            Permission.SUGGEST_RELATED_QUERIES,
            Permission.OPTIMIZE_QUERY,
            Permission.IMPROVE_QUERY_LANGUAGE,
            Permission.ANALYZE_QUERY_INTENT,
            Permission.CREATE_VISUALIZATION,
            Permission.EXPORT_DATA,
        },
        UserRole.ADMIN: set(Permission),  # All permissions
    }
    
    def __init__(self, session_timeout_hours: int = 24):
        """
        Initialize user manager.
        
        Args:
            session_timeout_hours: Session timeout in hours
        """
        self.session_timeout = timedelta(hours=session_timeout_hours)
        self._users: Dict[str, User] = {}
        self._sessions: Dict[str, UserSession] = {}
        self._username_to_id: Dict[str, str] = {}
        
        # Create default admin user if none exists
        self._create_default_admin()
    
    async def create_user(
        self, 
        username: str, 
        email: str, 
        password: str, 
        role: UserRole = UserRole.USER,
        metadata: Dict[str, Any] = None
    ) -> User:
        """
        Create a new user.
        
        Args:
            username: Unique username
            email: User email address
            password: User password
            role: User role (default: USER)
            metadata: Additional user metadata
            
        Returns:
            Created User object
            
        Raises:
            ValidationError: If validation fails
        """
        # Validate inputs
        if not username or len(username.strip()) < 3:
            raise ValidationError("username", username, "Username must be at least 3 characters")
        
        if not email or '@' not in email:
            raise ValidationError("email", email, "Email must be a valid email address")
        
        if not password or len(password) < 6:
            raise ValidationError("password", "<hidden>", "Password must be at least 6 characters")
        
        username = username.strip()
        email = email.strip().lower()
        
        # Check for duplicates
        if username in self._username_to_id:
            raise ValidationError("username", username, "Username already exists")
        
        if any(user.email == email for user in self._users.values()):
            raise ValidationError("email", email, "Email already exists")
        
        # Create user
        user_id = str(uuid.uuid4())
        salt = secrets.token_hex(32)
        password_hash = User._hash_password(password, salt)
        
        user = User(
            user_id=user_id,
            username=username,
            email=email,
            password_hash=password_hash,
            salt=salt,
            role=role,
            created_at=datetime.now(),
            updated_at=datetime.now(),
            metadata=metadata or {}
        )
        
        # Store user
        self._users[user_id] = user
        self._username_to_id[username] = user_id
        
        return user
    
    async def authenticate_user(self, username: str, password: str) -> Optional[UserSession]:
        """
        Authenticate user and create session.
        
        Args:
            username: Username to authenticate
            password: Password to verify
            
        Returns:
            UserSession if authentication successful, None otherwise
        """
        if not username or not password:
            return None
        
        username = username.strip()
        user_id = self._username_to_id.get(username)
        
        if not user_id:
            return None
        
        user = self._users.get(user_id)
        if not user or not user.is_active:
            return None
        
        # Check if account is locked
        if user.is_locked():
            return None
        
        # Verify password
        if not user.verify_password(password):
            user.record_login(successful=False)
            return None
        
        # Successful authentication
        user.record_login(successful=True)
        
        # Create session
        session = await self._create_session(user)
        return session
    
    async def get_session(self, session_id: str) -> Optional[UserSession]:
        """
        Get active session by ID.
        
        Args:
            session_id: Session identifier
            
        Returns:
            UserSession if valid and active, None otherwise
        """
        session = self._sessions.get(session_id)
        
        if not session or session.is_expired():
            if session:
                # Clean up expired session
                await self._remove_session(session_id)
            return None
        
        # Update activity
        session.update_activity()
        return session
    
    async def logout_user(self, session_id: str) -> bool:
        """
        Logout user and invalidate session.
        
        Args:
            session_id: Session to invalidate
            
        Returns:
            True if successful, False if session not found
        """
        return await self._remove_session(session_id)
    
    async def check_permission(self, session_id: str, permission: Permission) -> bool:
        """
        Check if session has specific permission.
        
        Args:
            session_id: Session identifier
            permission: Permission to check
            
        Returns:
            True if permission granted, False otherwise
        """
        if not config or not config.enable_authentication:
            return True  # Authentication disabled - allow all
        
        session = await self.get_session(session_id)
        return session and session.has_permission(permission)
    
    async def get_user_by_id(self, user_id: str) -> Optional[User]:
        """Get user by ID."""
        return self._users.get(user_id)
    
    async def get_user_by_username(self, username: str) -> Optional[User]:
        """Get user by username."""
        user_id = self._username_to_id.get(username)
        return self._users.get(user_id) if user_id else None
    
    async def update_user_role(self, user_id: str, new_role: UserRole) -> bool:
        """
        Update user role.
        
        Args:
            user_id: User identifier
            new_role: New role to assign
            
        Returns:
            True if successful, False if user not found
        """
        user = self._users.get(user_id)
        if not user:
            return False
        
        user.role = new_role
        user.updated_at = datetime.now()
        
        # Update existing sessions for this user
        for session in self._sessions.values():
            if session.user_id == user_id:
                session.role = new_role
                session.permissions = self.ROLE_PERMISSIONS.get(new_role, set())
        
        return True
    
    async def list_users(self) -> List[Dict[str, Any]]:
        """List all users (excluding sensitive data)."""
        return [user.to_dict(include_sensitive=False) for user in self._users.values()]
    
    async def deactivate_user(self, user_id: str) -> bool:
        """
        Deactivate user account.
        
        Args:
            user_id: User identifier
            
        Returns:
            True if successful, False if user not found
        """
        user = self._users.get(user_id)
        if not user:
            return False
        
        user.is_active = False
        user.updated_at = datetime.now()
        
        # Invalidate all sessions for this user
        sessions_to_remove = [
            session.session_id for session in self._sessions.values()
            if session.user_id == user_id
        ]
        
        for session_id in sessions_to_remove:
            await self._remove_session(session_id)
        
        return True
    
    async def cleanup_expired_sessions(self):
        """Clean up expired sessions."""
        expired_sessions = [
            session_id for session_id, session in self._sessions.items()
            if session.is_expired()
        ]
        
        for session_id in expired_sessions:
            await self._remove_session(session_id)
    
    async def get_session_info(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session information."""
        session = await self.get_session(session_id)
        return session.to_dict() if session else None
    
    def is_authentication_enabled(self) -> bool:
        """Check if authentication is enabled."""
        return config and config.enable_authentication
    
    async def _create_session(self, user: User) -> UserSession:
        """Create a new user session."""
        session_id = str(uuid.uuid4())
        now = datetime.now()
        expires_at = now + self.session_timeout
        
        permissions = self.ROLE_PERMISSIONS.get(user.role, set())
        
        session = UserSession(
            session_id=session_id,
            user_id=user.user_id,
            username=user.username,
            role=user.role,
            permissions=permissions,
            created_at=now,
            last_activity=now,
            expires_at=expires_at
        )
        
        self._sessions[session_id] = session
        return session
    
    async def _remove_session(self, session_id: str) -> bool:
        """Remove a session."""
        if session_id in self._sessions:
            del self._sessions[session_id]
            return True
        return False
    
    def _create_default_admin(self):
        """Create default admin user if none exists."""
        if not self._users:
            try:
                # Use secure defaults
                admin_username = "admin"
                admin_password = "admin123"  # Should be changed immediately
                admin_email = "admin@example.com"
                
                user_id = str(uuid.uuid4())
                salt = secrets.token_hex(32)
                password_hash = User._hash_password(admin_password, salt)
                
                admin_user = User(
                    user_id=user_id,
                    username=admin_username,
                    email=admin_email,
                    password_hash=password_hash,
                    salt=salt,
                    role=UserRole.ADMIN,
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                    metadata={"is_default_admin": True}
                )
                
                self._users[user_id] = admin_user
                self._username_to_id[admin_username] = user_id
                
            except Exception as e:
                # Log error but don't fail initialization
                import warnings
                warnings.warn(f"Failed to create default admin user: {e}")


# Global user manager instance
user_manager = UserManager(
    session_timeout_hours=config.cache_ttl // 3600 if config and config.cache_ttl else 24
)