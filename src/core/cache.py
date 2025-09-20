"""
Caching system for the Natural Language SQL MCP Server.

This module provides Redis-based caching for queries, schemas, and other
frequently accessed data to improve performance and reduce database load.
"""

import json
import hashlib
import asyncio
from functools import wraps
from typing import Optional, Dict, Any, List, Union
from datetime import datetime, timedelta
import logging

try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None

from .config import config
from .exceptions import CacheError


logger = logging.getLogger(__name__)


class QueryCache:
    """
    Redis-based cache for query results and related data.
    
    Provides high-performance caching with automatic TTL management,
    cache key generation, and intelligent invalidation strategies.
    """
    
    def __init__(self, redis_url: str = None, default_ttl: int = 300):
        """
        Initialize the query cache.
        
        Args:
            redis_url: Redis connection URL (uses config default if None)
            default_ttl: Default time-to-live for cached items in seconds
        """
        self.default_ttl = default_ttl
        self.redis_url = redis_url or (config.cache_redis_url if config else "redis://localhost:6379")
        self.redis_client = None
        self._connection_healthy = False
        
        if not REDIS_AVAILABLE:
            logger.warning("Redis not available - caching will be disabled")
    
    async def connect(self) -> bool:
        """
        Establish connection to Redis server.
        
        Returns:
            True if connection successful, False otherwise
        """
        if not REDIS_AVAILABLE:
            return False
        
        try:
            self.redis_client = redis.from_url(
                self.redis_url,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True
            )
            
            # Test the connection
            await self.redis_client.ping()
            self._connection_healthy = True
            logger.info(f"Connected to Redis cache at {self.redis_url}")
            return True
            
        except Exception as e:
            logger.warning(f"Failed to connect to Redis: {str(e)}")
            self._connection_healthy = False
            return False
    
    async def disconnect(self):
        """Close Redis connection."""
        if self.redis_client:
            try:
                await self.redis_client.aclose()
                self._connection_healthy = False
                logger.info("Disconnected from Redis cache")
            except Exception as e:
                logger.warning(f"Error disconnecting from Redis: {str(e)}")
    
    def _generate_cache_key(self, key_type: str, identifier: str, **kwargs) -> str:
        """
        Generate a consistent cache key.
        
        Args:
            key_type: Type of cached data (e.g., 'query', 'schema', 'translation')
            identifier: Unique identifier for the data
            **kwargs: Additional parameters to include in key generation
            
        Returns:
            Generated cache key string
        """
        # Create a deterministic key from all parameters
        key_data = f"{key_type}:{identifier}"
        
        if kwargs:
            # Sort kwargs for consistent key generation
            sorted_kwargs = sorted(kwargs.items())
            params_str = "&".join(f"{k}={v}" for k, v in sorted_kwargs)
            key_data += f":{params_str}"
        
        # Hash long keys to keep them manageable
        if len(key_data) > 100:
            key_hash = hashlib.sha256(key_data.encode()).hexdigest()[:16]
            key_data = f"{key_type}:hash:{key_hash}"
        
        return key_data
    
    async def get_cached_result(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """
        Get cached result by key.
        
        Args:
            cache_key: Cache key to retrieve
            
        Returns:
            Cached data as dictionary, or None if not found
        """
        if not self._connection_healthy or not self.redis_client:
            return None
        
        try:
            cached_data = await self.redis_client.get(cache_key)
            if cached_data:
                # Parse JSON and add cache metadata
                result = json.loads(cached_data)
                result['_cache_hit'] = True
                result['_cache_key'] = cache_key
                return result
            return None
            
        except Exception as e:
            logger.warning(f"Cache retrieval error for key {cache_key}: {str(e)}")
            return None
    
    async def cache_result(
        self, 
        cache_key: str, 
        data: Dict[str, Any], 
        ttl: int = None
    ) -> bool:
        """
        Cache data with specified TTL.
        
        Args:
            cache_key: Key to store data under
            data: Data to cache (must be JSON serializable)
            ttl: Time-to-live in seconds (uses default if None)
            
        Returns:
            True if caching successful, False otherwise
        """
        if not self._connection_healthy or not self.redis_client:
            return False
        
        try:
            # Add cache metadata
            cache_data = data.copy()
            cache_data['_cached_at'] = datetime.now().isoformat()
            cache_data['_cache_ttl'] = ttl or self.default_ttl
            
            # Serialize and store
            serialized = json.dumps(cache_data, default=str)
            ttl = ttl or self.default_ttl
            
            await self.redis_client.setex(cache_key, ttl, serialized)
            logger.debug(f"Cached data with key {cache_key} (TTL: {ttl}s)")
            return True
            
        except Exception as e:
            logger.warning(f"Cache storage error for key {cache_key}: {str(e)}")
            return False
    
    async def invalidate_cache(self, pattern: str = None, cache_key: str = None) -> int:
        """
        Invalidate cached data by pattern or specific key.
        
        Args:
            pattern: Redis pattern to match keys for deletion (e.g., "query:*")
            cache_key: Specific key to delete
            
        Returns:
            Number of keys deleted
        """
        if not self._connection_healthy or not self.redis_client:
            return 0
        
        try:
            if cache_key:
                # Delete specific key
                result = await self.redis_client.delete(cache_key)
                if result:
                    logger.debug(f"Invalidated cache key: {cache_key}")
                return result
            
            elif pattern:
                # Delete by pattern
                keys = await self.redis_client.keys(pattern)
                if keys:
                    result = await self.redis_client.delete(*keys)
                    logger.debug(f"Invalidated {result} cache keys matching pattern: {pattern}")
                    return result
            
            return 0
            
        except Exception as e:
            logger.warning(f"Cache invalidation error: {str(e)}")
            return 0
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics and health information.
        
        Returns:
            Dictionary with cache statistics
        """
        if not self._connection_healthy or not self.redis_client:
            return {
                "status": "disconnected",
                "redis_available": REDIS_AVAILABLE,
                "connection_healthy": False
            }
        
        try:
            info = await self.redis_client.info()
            
            return {
                "status": "connected",
                "redis_available": REDIS_AVAILABLE,
                "connection_healthy": True,
                "redis_version": info.get('redis_version'),
                "used_memory": info.get('used_memory_human'),
                "connected_clients": info.get('connected_clients'),
                "total_commands_processed": info.get('total_commands_processed'),
                "keyspace_hits": info.get('keyspace_hits', 0),
                "keyspace_misses": info.get('keyspace_misses', 0),
                "hit_rate": self._calculate_hit_rate(
                    info.get('keyspace_hits', 0), 
                    info.get('keyspace_misses', 0)
                )
            }
            
        except Exception as e:
            logger.warning(f"Error getting cache stats: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "redis_available": REDIS_AVAILABLE,
                "connection_healthy": False
            }
    
    def _calculate_hit_rate(self, hits: int, misses: int) -> float:
        """Calculate cache hit rate percentage."""
        total = hits + misses
        return round((hits / total) * 100, 2) if total > 0 else 0.0


class SchemaCache:
    """
    Specialized cache for database schema information.
    
    Schema data changes infrequently, so it can be cached for longer periods
    with different invalidation strategies.
    """
    
    def __init__(self, query_cache: QueryCache):
        """
        Initialize schema cache using the main query cache.
        
        Args:
            query_cache: Main QueryCache instance to use for storage
        """
        self.query_cache = query_cache
        self.schema_ttl = 3600  # 1 hour default TTL for schema data
    
    async def get_table_schema(self, table_name: str, database_type: str = "postgresql") -> Optional[Dict[str, Any]]:
        """Get cached table schema."""
        cache_key = self.query_cache._generate_cache_key(
            "schema", 
            table_name, 
            db_type=database_type
        )
        return await self.query_cache.get_cached_result(cache_key)
    
    async def cache_table_schema(
        self, 
        table_name: str, 
        schema_data: Dict[str, Any], 
        database_type: str = "postgresql"
    ) -> bool:
        """Cache table schema data."""
        cache_key = self.query_cache._generate_cache_key(
            "schema", 
            table_name, 
            db_type=database_type
        )
        return await self.query_cache.cache_result(cache_key, schema_data, self.schema_ttl)
    
    async def get_database_tables(self, database_type: str = "postgresql") -> Optional[List[str]]:
        """Get cached list of database tables."""
        cache_key = self.query_cache._generate_cache_key(
            "tables", 
            "all", 
            db_type=database_type
        )
        cached = await self.query_cache.get_cached_result(cache_key)
        return cached.get('tables') if cached else None
    
    async def cache_database_tables(
        self, 
        tables: List[str], 
        database_type: str = "postgresql"
    ) -> bool:
        """Cache list of database tables."""
        cache_key = self.query_cache._generate_cache_key(
            "tables", 
            "all", 
            db_type=database_type
        )
        return await self.query_cache.cache_result(
            cache_key, 
            {"tables": tables}, 
            self.schema_ttl
        )
    
    async def invalidate_schema(self, table_name: str = None) -> int:
        """Invalidate schema cache for specific table or all schemas."""
        if table_name:
            pattern = f"schema:{table_name}:*"
        else:
            pattern = "schema:*"
        
        return await self.query_cache.invalidate_cache(pattern=pattern)


def cache_query_result(ttl: int = 300, cache_key_func=None):
    """
    Decorator for caching query results.
    
    Args:
        ttl: Time-to-live for cached results in seconds
        cache_key_func: Optional function to generate custom cache key
        
    Returns:
        Decorator function
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Skip caching if not enabled or not available
            if not config or not config.enable_query_caching or not query_cache._connection_healthy:
                return await func(*args, **kwargs)
            
            # Generate cache key
            if cache_key_func:
                cache_key = cache_key_func(*args, **kwargs)
            else:
                # Default cache key generation
                func_name = func.__name__
                
                # Include natural language query if present
                natural_query = kwargs.get('natural_language_query') or (
                    args[1] if len(args) > 1 else ""
                )
                
                # Include database type from config
                db_type = config.database.db_type if config else "postgresql"
                
                cache_key = query_cache._generate_cache_key(
                    "query_result",
                    natural_query,
                    function=func_name,
                    db_type=db_type
                )
            
            # Try to get from cache
            cached_result = await query_cache.get_cached_result(cache_key)
            if cached_result:
                logger.debug(f"Cache hit for {func_name}")
                return cached_result
            
            # Execute function
            result = await func(*args, **kwargs)
            
            # Cache successful results only
            if isinstance(result, dict) and result.get('success'):
                await query_cache.cache_result(cache_key, result, ttl)
                logger.debug(f"Cached result for {func_name}")
            
            return result
        return wrapper
    return decorator


# Global cache instances
query_cache = QueryCache(
    redis_url=config.cache_redis_url if config else "redis://localhost:6379",
    default_ttl=config.cache_ttl if config else 300
)

schema_cache = SchemaCache(query_cache)


async def initialize_cache():
    """Initialize cache connections."""
    if config and config.enable_query_caching:
        success = await query_cache.connect()
        if success:
            logger.info("Cache system initialized successfully")
        else:
            logger.warning("Cache system failed to initialize - continuing without cache")
    else:
        logger.info("Caching disabled in configuration")


async def cleanup_cache():
    """Cleanup cache connections."""
    await query_cache.disconnect()
