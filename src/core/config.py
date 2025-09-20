"""
Configuration management for the Natural Language SQL MCP Server.

This module handles loading and validating environment variables and configuration
settings using Pydantic for robust validation and error handling.
"""

import os
from typing import Optional, List
from pydantic import BaseModel, field_validator, Field, ConfigDict
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
from .exceptions import ConfigurationError

# Load environment variables from .env file
load_dotenv()


class DatabaseConfig(BaseSettings):
    """Database connection configuration with validation."""
    
    host: str = Field(default="localhost", description="Database host")
    port: int = Field(default=5432, description="Database port")
    username: str = Field(default="postgres", description="Database username")
    password: str = Field(default="", description="Database password")
    database: str = Field(default="nlp_sql_db", description="Database name")
    db_type: str = Field(default="postgresql", description="Database type")
    
    @field_validator('db_type')
    @classmethod
    def validate_db_type(cls, v):
        """Validate that database type is supported."""
        allowed = ['postgresql', 'postgres', 'mysql', 'sqlite']
        if v.lower() not in allowed:
            raise ValueError(f'Database type must be one of {allowed}, got: {v}')
        return v.lower()
    
    @field_validator('port')
    @classmethod
    def validate_port(cls, v):
        """Validate that port is in valid range."""
        if not 1 <= v <= 65535:
            raise ValueError(f'Port must be between 1 and 65535, got: {v}')
        return v
    
    @field_validator('host')
    @classmethod
    def validate_host(cls, v):
        """Validate that host is not empty."""
        if not v or not v.strip():
            raise ValueError('Database host cannot be empty')
        return v.strip()
    
    model_config = ConfigDict(
        env_prefix="DB_",
        env_file=".env",
        case_sensitive=False,
        extra="ignore"
    )


class LLMConfig(BaseSettings):
    """Large Language Model configuration with validation."""
    
    api_key: str = Field(default="", description="OpenAI API key")
    model: str = Field(default="gpt-4o-mini", description="Model name")
    base_url: Optional[str] = Field(default=None, description="Custom API base URL")
    max_tokens: int = Field(default=1000, description="Maximum tokens per request")
    temperature: float = Field(default=0.1, description="Model temperature")
    
    @field_validator('api_key')
    @classmethod
    def validate_api_key(cls, v):
        """Validate that API key is provided."""
        if not v or not v.strip():
            raise ValueError('LLM API key is required')
        return v.strip()
    
    @field_validator('model')
    @classmethod
    def validate_model(cls, v):
        """Validate model name."""
        allowed_models = [
            'gpt-4o-mini', 'gpt-4o', 'gpt-4', 'gpt-3.5-turbo',
            'gpt-4-turbo', 'gpt-4-turbo-preview'
        ]
        if v not in allowed_models:
            # Allow custom models but warn
            return v
        return v
    
    @field_validator('max_tokens')
    @classmethod
    def validate_max_tokens(cls, v):
        """Validate max tokens range."""
        if not 1 <= v <= 8000:
            raise ValueError(f'Max tokens must be between 1 and 8000, got: {v}')
        return v
    
    @field_validator('temperature')
    @classmethod
    def validate_temperature(cls, v):
        """Validate temperature range."""
        if not 0.0 <= v <= 2.0:
            raise ValueError(f'Temperature must be between 0.0 and 2.0, got: {v}')
        return v
    
    model_config = ConfigDict(
        env_prefix="LLM_",
        env_file=".env",
        case_sensitive=False,
        extra="ignore"
    )


class ServerConfig(BaseSettings):
    """MCP Server configuration with validation."""
    
    name: str = Field(default="Natural Language SQL Server", description="Server name")
    host: str = Field(default="127.0.0.1", description="Server host")
    port: int = Field(default=8000, description="Server port")
    transport: str = Field(default="stdio", description="Transport method")
    
    @field_validator('transport')
    @classmethod
    def validate_transport(cls, v):
        """Validate transport method."""
        allowed = ['stdio', 'http']
        if v.lower() not in allowed:
            raise ValueError(f'Transport must be one of {allowed}, got: {v}')
        return v.lower()
    
    @field_validator('port')
    @classmethod
    def validate_port(cls, v):
        """Validate port range."""
        if not 1 <= v <= 65535:
            raise ValueError(f'Port must be between 1 and 65535, got: {v}')
        return v
    
    model_config = ConfigDict(
        env_prefix="MCP_",
        env_file=".env",
        case_sensitive=False,
        extra="ignore"
    )


class Config(BaseSettings):
    """Main configuration class with feature flags and validation."""
    
    # Environment settings
    environment: str = Field(default="development", description="Environment")
    debug: bool = Field(default=False, description="Debug mode")
    
    # Feature flags
    enable_query_caching: bool = Field(default=True, description="Enable query caching")
    enable_query_history: bool = Field(default=True, description="Enable query history")
    enable_smart_suggestions: bool = Field(default=True, description="Enable AI suggestions")
    enable_visualization: bool = Field(default=True, description="Enable data visualization")
    enable_authentication: bool = Field(default=False, description="Enable user authentication")
    
    # Cache settings
    cache_redis_url: str = Field(default="redis://localhost:6379", description="Redis URL for caching")
    cache_ttl: int = Field(default=300, description="Default cache TTL in seconds")
    
    # Performance settings
    query_timeout: int = Field(default=30, description="Query timeout in seconds")
    max_result_rows: int = Field(default=1000, description="Maximum rows to return")
    
    # Nested configurations (initialized in __init__)
    database: Optional[DatabaseConfig] = None
    llm: Optional[LLMConfig] = None
    server: Optional[ServerConfig] = None
    
    @field_validator('environment')
    @classmethod
    def validate_environment(cls, v):
        """Validate environment value."""
        allowed = ['development', 'staging', 'production']
        if v.lower() not in allowed:
            raise ValueError(f'Environment must be one of {allowed}, got: {v}')
        return v.lower()
    
    @field_validator('cache_ttl')
    @classmethod
    def validate_cache_ttl(cls, v):
        """Validate cache TTL."""
        if not 1 <= v <= 86400:  # 1 second to 1 day
            raise ValueError(f'Cache TTL must be between 1 and 86400 seconds, got: {v}')
        return v
    
    @field_validator('query_timeout')
    @classmethod
    def validate_query_timeout(cls, v):
        """Validate query timeout."""
        if not 1 <= v <= 300:  # 1 second to 5 minutes
            raise ValueError(f'Query timeout must be between 1 and 300 seconds, got: {v}')
        return v
    
    @field_validator('max_result_rows')
    @classmethod
    def validate_max_result_rows(cls, v):
        """Validate max result rows."""
        if not 1 <= v <= 10000:
            raise ValueError(f'Max result rows must be between 1 and 10000, got: {v}')
        return v
    
    model_config = ConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore"
    )
    
    def __init__(self, **kwargs):
        """Initialize configuration with validation."""
        super().__init__(**kwargs)
        self._load_nested_configs()
    
    def _load_nested_configs(self):
        """Load and validate nested configuration objects."""
        try:
            self.database = DatabaseConfig()
        except Exception as e:
            raise ConfigurationError("database", str(e), technical_details=str(e))
        
        try:
            self.llm = LLMConfig()
        except Exception as e:
            raise ConfigurationError("llm", str(e), technical_details=str(e))
        
        try:
            self.server = ServerConfig()
        except Exception as e:
            raise ConfigurationError("server", str(e), technical_details=str(e))
    
    @property
    def database_url(self) -> str:
        """Generate database connection URL."""
        try:
            if self.database.db_type in ["postgresql", "postgres"]:
                return f"postgresql://{self.database.username}:{self.database.password}@{self.database.host}:{self.database.port}/{self.database.database}"
            elif self.database.db_type == "mysql":
                return f"mysql://{self.database.username}:{self.database.password}@{self.database.host}:{self.database.port}/{self.database.database}"
            elif self.database.db_type == "sqlite":
                return f"sqlite:///{self.database.database}"
            else:
                raise ValueError(f"Unsupported database type: {self.database.db_type}")
        except Exception as e:
            raise ConfigurationError("database_url", str(e), technical_details=str(e))
    
    def validate_all(self) -> List[str]:
        """
        Validate all configuration settings and return list of issues.
        
        Returns:
            List of validation error messages
        """
        issues = []
        
        # Validate database configuration
        try:
            if self.database.db_type == "sqlite":
                # For SQLite, check if file path is accessible
                db_path = self.database.database
                if db_path != ":memory:" and not os.path.exists(os.path.dirname(db_path) or "."):
                    issues.append(f"SQLite database directory does not exist: {db_path}")
        except Exception as e:
            issues.append(f"Database configuration error: {str(e)}")
        
        # Validate LLM configuration
        if not self.llm.api_key:
            issues.append("LLM API key is required but not provided")
        
        return issues


# Global configuration instance
try:
    config = Config()
    validation_issues = config.validate_all()
    if validation_issues:
        import warnings
        for issue in validation_issues:
            warnings.warn(f"Configuration warning: {issue}")
except Exception as e:
    # If configuration fails, create a minimal config for testing
    import warnings
    warnings.warn(f"Failed to load configuration: {str(e)}")
    config = None
