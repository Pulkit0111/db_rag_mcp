"""
Configuration management for the Natural Language SQL MCP Server.

This module handles loading and providing access to environment variables
and configuration settings required by the application.
"""

import os
from typing import Optional
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    host: str
    port: int
    username: str
    password: str
    database: str
    db_type: str  # e.g., 'postgresql', 'mysql'


@dataclass
class LLMConfig:
    """Large Language Model configuration."""
    api_key: str
    model: str
    base_url: Optional[str] = None


@dataclass
class ServerConfig:
    """MCP Server configuration."""
    name: str
    host: str
    port: int
    transport: str  # 'stdio' or 'http'


class Config:
    """Main configuration class that loads all settings from environment variables."""
    
    def __init__(self):
        self._load_config()
    
    def _load_config(self):
        """Load configuration from environment variables."""
        
        # Database configuration
        self.database = DatabaseConfig(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            username=os.getenv("DB_USERNAME", "postgres"),
            password=os.getenv("DB_PASSWORD", ""),
            database=os.getenv("DB_NAME", "nlp_sql_db"),
            db_type=os.getenv("DB_TYPE", "postgresql")
        )
        
        # LLM configuration
        self.llm = LLMConfig(
            api_key=os.getenv("LLM_API_KEY", ""),
            model=os.getenv("LLM_MODEL", "gpt-3.5-turbo"),
            base_url=os.getenv("LLM_BASE_URL")
        )
        
        # Server configuration
        self.server = ServerConfig(
            name=os.getenv("MCP_SERVER_NAME", "Natural Language SQL Server"),
            host=os.getenv("MCP_HOST", "127.0.0.1"),
            port=int(os.getenv("MCP_PORT", "8000")),
            transport=os.getenv("MCP_TRANSPORT", "stdio")
        )
    
    @property
    def database_url(self) -> str:
        """Generate database connection URL."""
        if self.database.db_type == "postgresql":
            return f"postgresql://{self.database.username}:{self.database.password}@{self.database.host}:{self.database.port}/{self.database.database}"
        elif self.database.db_type == "mysql":
            return f"mysql://{self.database.username}:{self.database.password}@{self.database.host}:{self.database.port}/{self.database.database}"
        else:
            raise ValueError(f"Unsupported database type: {self.database.db_type}")


# Global configuration instance
config = Config()
