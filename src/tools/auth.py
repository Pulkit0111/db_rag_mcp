"""
Authentication and user management tools for the MCP server.

This module provides MCP tools for user authentication, session management,
and role-based access control integration.
"""

from typing import Dict, Any, List
from fastmcp import Context

from ..auth.user_manager import user_manager, UserRole, Permission
from ..core.exceptions import ValidationError, NaturalSQLException
from ..core.config import config


def _get_session_id(ctx: Context) -> str:
    """Get session ID from context."""
    return getattr(ctx, 'session_id', 'default_session')


async def authenticate_user(ctx: Context, username: str, password: str) -> Dict[str, Any]:
    """
    Authenticate a user and create a new session.
    
    This tool allows users to log in with their credentials and receive
    a session token for accessing protected resources.
    
    Args:
        username: User's username
        password: User's password
        
    Returns:
        Dictionary containing authentication result and session information
    """
    try:
        if not config or not config.enable_authentication:
            return {
                "success": False,
                "message": "Authentication is disabled in the current configuration",
                "suggestions": ["Enable authentication in configuration to use this feature"]
            }
        
        # Validate inputs
        if not username or not username.strip():
            raise ValidationError("username", username, "Username cannot be empty")
        
        if not password or not password.strip():
            raise ValidationError("password", "<hidden>", "Password cannot be empty")
        
        await ctx.info(f"Attempting to authenticate user: {username}")
        
        # Authenticate user
        session = await user_manager.authenticate_user(username.strip(), password)
        
        if not session:
            await ctx.warning(f"Authentication failed for user: {username}")
            return {
                "success": False,
                "message": "Invalid username or password",
                "suggestions": [
                    "Check your username and password",
                    "Ensure your account is not locked",
                    "Contact administrator if you continue having issues"
                ]
            }
        
        await ctx.info(f"Authentication successful for user: {username}")
        
        return {
            "success": True,
            "message": "Authentication successful",
            "session": {
                "session_id": session.session_id,
                "username": session.username,
                "role": session.role.value,
                "permissions": [p.value for p in session.permissions],
                "expires_at": session.expires_at.isoformat()
            },
            "user_info": {
                "user_id": session.user_id,
                "username": session.username,
                "role": session.role.value
            }
        }
        
    except ValidationError as e:
        await ctx.error(f"Authentication validation failed: {e.user_message}")
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False)
        }
    except Exception as e:
        unexpected_error = NaturalSQLException(
            user_message="An unexpected error occurred during authentication",
            technical_details=str(e),
            suggestions=["Try again", "Contact system administrator if problem persists"]
        )
        await ctx.error(f"Authentication error: {unexpected_error.user_message}")
        return {
            "success": False,
            "error": unexpected_error.to_dict(include_technical=config.debug if config else False)
        }


async def logout_user(ctx: Context) -> Dict[str, Any]:
    """
    Logout the current user and invalidate their session.
    
    This tool ends the current user session and requires re-authentication
    for future requests.
    
    Returns:
        Dictionary containing logout result
    """
    try:
        if not config or not config.enable_authentication:
            return {
                "success": False,
                "message": "Authentication is disabled in the current configuration"
            }
        
        session_id = _get_session_id(ctx)
        
        # Check if there's an active session
        session = await user_manager.get_session(session_id)
        if not session:
            return {
                "success": False,
                "message": "No active session found",
                "suggestions": ["You may already be logged out"]
            }
        
        # Logout user
        success = await user_manager.logout_user(session_id)
        
        if success:
            await ctx.info(f"User {session.username} logged out successfully")
            return {
                "success": True,
                "message": "Logout successful",
                "logged_out_user": session.username
            }
        else:
            return {
                "success": False,
                "message": "Failed to logout user"
            }
        
    except Exception as e:
        unexpected_error = NaturalSQLException(
            user_message="An unexpected error occurred during logout",
            technical_details=str(e),
            suggestions=["Try again", "Contact system administrator if problem persists"]
        )
        await ctx.error(f"Logout error: {unexpected_error.user_message}")
        return {
            "success": False,
            "error": unexpected_error.to_dict(include_technical=config.debug if config else False)
        }


async def get_current_user(ctx: Context) -> Dict[str, Any]:
    """
    Get information about the currently authenticated user.
    
    This tool provides details about the current user session including
    role, permissions, and session status.
    
    Returns:
        Dictionary containing current user information
    """
    try:
        if not config or not config.enable_authentication:
            return {
                "success": True,
                "message": "Authentication is disabled - operating in unrestricted mode",
                "user_info": {
                    "username": "anonymous",
                    "role": "unrestricted",
                    "permissions": "all",
                    "session_status": "authentication_disabled"
                }
            }
        
        session_id = _get_session_id(ctx)
        session = await user_manager.get_session(session_id)
        
        if not session:
            return {
                "success": False,
                "message": "No active session found",
                "suggestions": [
                    "Use authenticate_user tool to log in first",
                    "Check if your session has expired"
                ]
            }
        
        user = await user_manager.get_user_by_id(session.user_id)
        
        return {
            "success": True,
            "user_info": {
                "user_id": session.user_id,
                "username": session.username,
                "role": session.role.value,
                "permissions": [p.value for p in session.permissions],
                "session_created": session.created_at.isoformat(),
                "last_activity": session.last_activity.isoformat(),
                "expires_at": session.expires_at.isoformat(),
                "email": user.email if user else None,
                "last_login": user.last_login.isoformat() if user and user.last_login else None
            },
            "session_status": {
                "is_active": True,
                "time_remaining": str(session.expires_at - session.last_activity),
                "database_connections": list(session.database_connections.keys())
            }
        }
        
    except Exception as e:
        unexpected_error = NaturalSQLException(
            user_message="An unexpected error occurred while retrieving user information",
            technical_details=str(e),
            suggestions=["Try again", "Contact system administrator if problem persists"]
        )
        await ctx.error(f"Get current user error: {unexpected_error.user_message}")
        return {
            "success": False,
            "error": unexpected_error.to_dict(include_technical=config.debug if config else False)
        }


async def create_user(ctx: Context, username: str, email: str, password: str, role: str = "user") -> Dict[str, Any]:
    """
    Create a new user account (Admin only).
    
    This tool allows administrators to create new user accounts with
    specified roles and permissions.
    
    Args:
        username: Unique username for the new user
        email: Email address for the new user
        password: Password for the new user
        role: User role (viewer, user, analyst, admin)
        
    Returns:
        Dictionary containing user creation result
    """
    try:
        if not config or not config.enable_authentication:
            return {
                "success": False,
                "message": "Authentication is disabled in the current configuration",
                "suggestions": ["Enable authentication in configuration to use user management"]
            }
        
        # Check permissions
        session_id = _get_session_id(ctx)
        if not await user_manager.check_permission(session_id, Permission.CREATE_USER):
            return {
                "success": False,
                "message": "Insufficient permissions to create users",
                "suggestions": ["Only administrators can create new users"]
            }
        
        # Validate role
        try:
            user_role = UserRole(role.lower())
        except ValueError:
            valid_roles = [r.value for r in UserRole]
            raise ValidationError("role", role, f"Role must be one of: {', '.join(valid_roles)}")
        
        await ctx.info(f"Creating new user: {username} with role: {role}")
        
        # Create user
        user = await user_manager.create_user(
            username=username,
            email=email,
            password=password,
            role=user_role
        )
        
        await ctx.info(f"User created successfully: {username}")
        
        return {
            "success": True,
            "message": f"User '{username}' created successfully",
            "user_info": {
                "user_id": user.user_id,
                "username": user.username,
                "email": user.email,
                "role": user.role.value,
                "created_at": user.created_at.isoformat(),
                "is_active": user.is_active
            }
        }
        
    except ValidationError as e:
        await ctx.error(f"User creation validation failed: {e.user_message}")
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False)
        }
    except Exception as e:
        unexpected_error = NaturalSQLException(
            user_message="An unexpected error occurred while creating the user",
            technical_details=str(e),
            suggestions=["Check input parameters", "Contact system administrator if problem persists"]
        )
        await ctx.error(f"User creation error: {unexpected_error.user_message}")
        return {
            "success": False,
            "error": unexpected_error.to_dict(include_technical=config.debug if config else False)
        }


async def list_users(ctx: Context) -> Dict[str, Any]:
    """
    List all users in the system (Admin only).
    
    This tool allows administrators to view all user accounts and their
    status information.
    
    Returns:
        Dictionary containing list of all users
    """
    try:
        if not config or not config.enable_authentication:
            return {
                "success": False,
                "message": "Authentication is disabled in the current configuration"
            }
        
        # Check permissions
        session_id = _get_session_id(ctx)
        if not await user_manager.check_permission(session_id, Permission.LIST_USERS):
            return {
                "success": False,
                "message": "Insufficient permissions to list users",
                "suggestions": ["Only administrators can view user lists"]
            }
        
        await ctx.info("Retrieving user list")
        
        # Get users
        users = await user_manager.list_users()
        
        return {
            "success": True,
            "message": f"Retrieved {len(users)} users",
            "users": users,
            "user_count": len(users)
        }
        
    except Exception as e:
        unexpected_error = NaturalSQLException(
            user_message="An unexpected error occurred while listing users",
            technical_details=str(e),
            suggestions=["Contact system administrator if problem persists"]
        )
        await ctx.error(f"List users error: {unexpected_error.user_message}")
        return {
            "success": False,
            "error": unexpected_error.to_dict(include_technical=config.debug if config else False)
        }


async def update_user_role(ctx: Context, username: str, new_role: str) -> Dict[str, Any]:
    """
    Update a user's role (Admin only).
    
    This tool allows administrators to change user roles and their
    associated permissions.
    
    Args:
        username: Username of the user to update
        new_role: New role to assign (viewer, user, analyst, admin)
        
    Returns:
        Dictionary containing role update result
    """
    try:
        if not config or not config.enable_authentication:
            return {
                "success": False,
                "message": "Authentication is disabled in the current configuration"
            }
        
        # Check permissions
        session_id = _get_session_id(ctx)
        if not await user_manager.check_permission(session_id, Permission.MANAGE_ROLES):
            return {
                "success": False,
                "message": "Insufficient permissions to manage user roles",
                "suggestions": ["Only administrators can manage user roles"]
            }
        
        # Validate role
        try:
            user_role = UserRole(new_role.lower())
        except ValueError:
            valid_roles = [r.value for r in UserRole]
            raise ValidationError("new_role", new_role, f"Role must be one of: {', '.join(valid_roles)}")
        
        # Find user
        user = await user_manager.get_user_by_username(username)
        if not user:
            return {
                "success": False,
                "message": f"User '{username}' not found",
                "suggestions": ["Check the username spelling", "Use list_users to see available users"]
            }
        
        await ctx.info(f"Updating role for user {username} from {user.role.value} to {new_role}")
        
        # Update role
        success = await user_manager.update_user_role(user.user_id, user_role)
        
        if success:
            await ctx.info(f"Role updated successfully for user: {username}")
            return {
                "success": True,
                "message": f"Role updated for user '{username}' from '{user.role.value}' to '{new_role}'",
                "user_info": {
                    "username": username,
                    "old_role": user.role.value,
                    "new_role": new_role,
                    "updated_at": user.updated_at.isoformat()
                }
            }
        else:
            return {
                "success": False,
                "message": "Failed to update user role"
            }
        
    except ValidationError as e:
        await ctx.error(f"Role update validation failed: {e.user_message}")
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False)
        }
    except Exception as e:
        unexpected_error = NaturalSQLException(
            user_message="An unexpected error occurred while updating user role",
            technical_details=str(e),
            suggestions=["Contact system administrator if problem persists"]
        )
        await ctx.error(f"Role update error: {unexpected_error.user_message}")
        return {
            "success": False,
            "error": unexpected_error.to_dict(include_technical=config.debug if config else False)
        }


async def deactivate_user(ctx: Context, username: str) -> Dict[str, Any]:
    """
    Deactivate a user account (Admin only).
    
    This tool allows administrators to disable user accounts, preventing
    them from logging in while preserving their data.
    
    Args:
        username: Username of the user to deactivate
        
    Returns:
        Dictionary containing deactivation result
    """
    try:
        if not config or not config.enable_authentication:
            return {
                "success": False,
                "message": "Authentication is disabled in the current configuration"
            }
        
        # Check permissions
        session_id = _get_session_id(ctx)
        if not await user_manager.check_permission(session_id, Permission.UPDATE_USER):
            return {
                "success": False,
                "message": "Insufficient permissions to deactivate users",
                "suggestions": ["Only administrators can deactivate users"]
            }
        
        # Find user
        user = await user_manager.get_user_by_username(username)
        if not user:
            return {
                "success": False,
                "message": f"User '{username}' not found",
                "suggestions": ["Check the username spelling", "Use list_users to see available users"]
            }
        
        if not user.is_active:
            return {
                "success": False,
                "message": f"User '{username}' is already deactivated"
            }
        
        await ctx.info(f"Deactivating user: {username}")
        
        # Deactivate user
        success = await user_manager.deactivate_user(user.user_id)
        
        if success:
            await ctx.info(f"User deactivated successfully: {username}")
            return {
                "success": True,
                "message": f"User '{username}' has been deactivated",
                "user_info": {
                    "username": username,
                    "user_id": user.user_id,
                    "deactivated_at": user.updated_at.isoformat(),
                    "active_sessions_terminated": True
                }
            }
        else:
            return {
                "success": False,
                "message": "Failed to deactivate user"
            }
        
    except Exception as e:
        unexpected_error = NaturalSQLException(
            user_message="An unexpected error occurred while deactivating user",
            technical_details=str(e),
            suggestions=["Contact system administrator if problem persists"]
        )
        await ctx.error(f"User deactivation error: {unexpected_error.user_message}")
        return {
            "success": False,
            "error": unexpected_error.to_dict(include_technical=config.debug if config else False)
        }


async def check_permission(ctx: Context, permission_name: str) -> Dict[str, Any]:
    """
    Check if the current user has a specific permission.
    
    This tool allows users to verify their permissions for specific
    operations before attempting them.
    
    Args:
        permission_name: Name of the permission to check
        
    Returns:
        Dictionary containing permission check result
    """
    try:
        if not config or not config.enable_authentication:
            return {
                "success": True,
                "has_permission": True,
                "message": "Authentication disabled - all permissions granted",
                "permission_name": permission_name
            }
        
        # Validate permission
        try:
            permission = Permission(permission_name.lower())
        except ValueError:
            valid_permissions = [p.value for p in Permission]
            return {
                "success": False,
                "message": f"Invalid permission name: {permission_name}",
                "valid_permissions": valid_permissions
            }
        
        session_id = _get_session_id(ctx)
        has_permission = await user_manager.check_permission(session_id, permission)
        
        session = await user_manager.get_session(session_id)
        current_user = session.username if session else "anonymous"
        
        return {
            "success": True,
            "has_permission": has_permission,
            "permission_name": permission_name,
            "user": current_user,
            "message": f"Permission '{permission_name}' {'granted' if has_permission else 'denied'} for user '{current_user}'"
        }
        
    except Exception as e:
        unexpected_error = NaturalSQLException(
            user_message="An unexpected error occurred while checking permissions",
            technical_details=str(e),
            suggestions=["Contact system administrator if problem persists"]
        )
        await ctx.error(f"Permission check error: {unexpected_error.user_message}")
        return {
            "success": False,
            "error": unexpected_error.to_dict(include_technical=config.debug if config else False)
        }