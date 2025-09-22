"""
Custom exception hierarchy for the Natural Language SQL MCP Server.

This module provides user-friendly exceptions with helpful error messages,
suggestions for fixes, and technical details for debugging.
"""

from typing import List, Optional


class NaturalSQLException(Exception):
    """
    Base exception class for all Natural Language SQL related errors.
    
    This exception includes user-friendly messages, actionable suggestions,
    and optional technical details for debugging.
    """
    
    def __init__(
        self, 
        user_message: str, 
        suggestions: Optional[List[str]] = None, 
        technical_details: Optional[str] = None
    ):
        """
        Initialize the exception.
        
        Args:
            user_message: Human-readable error message
            suggestions: List of suggested solutions or next steps
            technical_details: Technical error details for debugging
        """
        self.user_message = user_message
        self.suggestions = suggestions or []
        self.technical_details = technical_details
        super().__init__(user_message)
    
    def to_dict(self, include_technical: bool = False) -> dict:
        """
        Convert exception to dictionary format.
        
        Args:
            include_technical: Whether to include technical details
            
        Returns:
            Dictionary representation of the exception
        """
        result = {
            "error_type": self.__class__.__name__,
            "user_message": self.user_message,
            "suggestions": self.suggestions
        }
        
        if include_technical and self.technical_details:
            result["technical_details"] = self.technical_details
            
        return result


class DatabaseConnectionError(NaturalSQLException):
    """Exception raised when database connection fails."""
    
    def __init__(
        self, 
        db_type: str, 
        host: str = None, 
        port: int = None, 
        technical_details: str = None
    ):
        """
        Initialize database connection error.
        
        Args:
            db_type: Type of database (postgresql, mysql, sqlite)
            host: Database host
            port: Database port
            technical_details: Technical error details
        """
        connection_info = f"{db_type}"
        if host:
            connection_info += f" at {host}"
        if port:
            connection_info += f":{port}"
            
        user_message = f"Failed to connect to {connection_info} database."
        
        suggestions = [
            "Check if the database server is running",
            "Verify your connection credentials (host, port, username, password)",
            "Ensure the database name exists",
            "Check network connectivity and firewall settings"
        ]
        
        # Database-specific suggestions
        if db_type.lower() == 'postgresql':
            suggestions.extend([
                "Try connecting with psql command-line tool to test connectivity",
                "Check if PostgreSQL service is running: 'sudo service postgresql status'"
            ])
        elif db_type.lower() == 'mysql':
            suggestions.extend([
                "Try connecting with mysql command-line tool to test connectivity",
                "Check if MySQL service is running: 'sudo service mysql status'"
            ])
        elif db_type.lower() == 'sqlite':
            suggestions.extend([
                "Check if the SQLite database file path is correct and accessible",
                "Ensure you have read/write permissions to the database file"
            ])
        
        super().__init__(user_message, suggestions, technical_details)


class QueryTranslationError(NaturalSQLException):
    """Exception raised when natural language to SQL translation fails."""
    
    def __init__(
        self, 
        query: str, 
        reason: str = None, 
        technical_details: str = None
    ):
        """
        Initialize query translation error.
        
        Args:
            query: The natural language query that failed to translate
            reason: Specific reason for translation failure
            technical_details: Technical error details
        """
        user_message = f"Could not translate your query to SQL: '{query[:100]}...'"
        if reason:
            user_message += f" Reason: {reason}"
        
        suggestions = [
            "Try rephrasing your query in simpler terms",
            "Be more specific about which tables and columns you want to query",
            "Check if the table and column names in your query exist in the database",
            "Use standard SQL terminology (SELECT, FROM, WHERE, etc.) in your description",
            "Break complex queries into smaller, simpler parts"
        ]
        
        super().__init__(user_message, suggestions, technical_details)


class QueryExecutionError(NaturalSQLException):
    """Exception raised when SQL query execution fails."""
    
    def __init__(
        self, 
        sql_query: str, 
        db_error: str = None, 
        technical_details: str = None
    ):
        """
        Initialize query execution error.
        
        Args:
            sql_query: The SQL query that failed to execute
            db_error: Database-specific error message
            technical_details: Technical error details
        """
        user_message = "Failed to execute the generated SQL query."
        if db_error:
            user_message += f" Database error: {db_error}"
        
        suggestions = [
            "Check if all referenced tables and columns exist in the database",
            "Verify that you have the necessary permissions to access the data",
            "Ensure the query syntax is valid for your database type",
            "Try simplifying the query to identify the problematic part"
        ]
        
        # Add specific suggestions based on common SQL errors
        if db_error:
            error_lower = db_error.lower()
            if "does not exist" in error_lower or "unknown" in error_lower:
                suggestions.insert(0, "The table or column referenced in the query doesn't exist")
            elif "permission" in error_lower or "denied" in error_lower:
                suggestions.insert(0, "You don't have permission to access this data")
            elif "syntax" in error_lower:
                suggestions.insert(0, "There's a syntax error in the generated SQL")
            elif "connection" in error_lower:
                suggestions.insert(0, "Database connection was lost during query execution")
        
        super().__init__(user_message, suggestions, technical_details)


class ConfigurationError(NaturalSQLException):
    """Exception raised when configuration is invalid."""
    
    def __init__(
        self, 
        config_field: str, 
        issue: str, 
        technical_details: str = None
    ):
        """
        Initialize configuration error.
        
        Args:
            config_field: The configuration field that has an issue
            issue: Description of the configuration issue
            technical_details: Technical error details
        """
        user_message = f"Configuration error in {config_field}: {issue}"
        
        suggestions = [
            "Check your .env file for the correct configuration values",
            "Ensure all required environment variables are set",
            "Verify that configuration values are in the correct format",
            "Review the documentation for proper configuration setup"
        ]
        
        # Field-specific suggestions
        if "database" in config_field.lower():
            suggestions.extend([
                "Verify database connection parameters (host, port, username, password)",
                "Ensure the database type is supported (postgresql, mysql, sqlite)"
            ])
        elif "llm" in config_field.lower() or "api" in config_field.lower():
            suggestions.extend([
                "Check if your API key is valid and has not expired",
                "Verify the API endpoint URL is correct"
            ])
        
        super().__init__(user_message, suggestions, technical_details)


class AuthenticationError(NaturalSQLException):
    """Exception raised when user authentication fails."""
    
    def __init__(
        self, 
        username: str = None, 
        reason: str = None, 
        technical_details: str = None
    ):
        """
        Initialize authentication error.
        
        Args:
            username: Username that failed authentication
            reason: Specific reason for authentication failure
            technical_details: Technical error details
        """
        if username:
            user_message = f"Authentication failed for user '{username}'"
        else:
            user_message = "Authentication failed"
            
        if reason:
            user_message += f": {reason}"
        
        suggestions = [
            "Check your username and password",
            "Ensure your account is active and not locked",
            "Contact your administrator if you continue to have issues",
            "Try resetting your password if available"
        ]
        
        super().__init__(user_message, suggestions, technical_details)


class PermissionError(NaturalSQLException):
    """Exception raised when user lacks necessary permissions."""
    
    def __init__(
        self, 
        operation: str, 
        resource: str = None, 
        user: str = None, 
        technical_details: str = None
    ):
        """
        Initialize permission error.
        
        Args:
            operation: The operation that was attempted
            resource: The resource that was accessed
            user: The user who attempted the operation
            technical_details: Technical error details
        """
        user_message = f"Permission denied for {operation}"
        if resource:
            user_message += f" on {resource}"
        if user:
            user_message += f" for user '{user}'"
        
        suggestions = [
            "Contact your administrator to request the necessary permissions",
            "Check if you're logged in with the correct user account",
            "Verify that your role includes the required permissions",
            "Try a different operation that you have permission to perform"
        ]
        
        super().__init__(user_message, suggestions, technical_details)


class CacheError(NaturalSQLException):
    """Exception raised when cache operations fail."""
    
    def __init__(
        self, 
        operation: str, 
        technical_details: str = None
    ):
        """
        Initialize cache error.
        
        Args:
            operation: The cache operation that failed
            technical_details: Technical error details
        """
        user_message = f"Cache operation failed: {operation}"
        
        suggestions = [
            "The system will continue without caching",
            "Check if Redis server is running and accessible",
            "Verify cache configuration settings",
            "Consider restarting the cache service"
        ]
        
        super().__init__(user_message, suggestions, technical_details)


class ValidationError(NaturalSQLException):
    """Exception raised when input validation fails."""
    
    def __init__(
        self, 
        field: str, 
        value: str, 
        expected: str = None, 
        technical_details: str = None
    ):
        """
        Initialize validation error.
        
        Args:
            field: The field that failed validation
            value: The invalid value
            expected: Description of expected value format
            technical_details: Technical error details
        """
        user_message = f"Invalid value for {field}: '{value}'"
        if expected:
            user_message += f". Expected: {expected}"
        
        suggestions = [
            "Check the format of your input",
            "Ensure all required fields are provided",
            "Review the documentation for valid input formats",
            "Try using simpler values to test"
        ]
        
        super().__init__(user_message, suggestions, technical_details)
