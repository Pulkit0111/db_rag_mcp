"""
Session and query history management for the Natural Language SQL MCP Server.

This module provides session management, query history tracking, and context-aware
functionality to enhance user experience and provide query suggestions.
"""

import uuid
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from collections import defaultdict
import hashlib

from .config import config
from .exceptions import ValidationError


@dataclass
class QueryHistory:
    """Represents a single query in the user's history."""
    
    id: str
    query: str
    sql: str
    timestamp: datetime
    results_count: int
    execution_time: float
    success: bool
    database_type: str
    session_id: str
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'QueryHistory':
        """Create from dictionary (for deserialization)."""
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return cls(**data)
    
    def get_cache_key(self) -> str:
        """Generate cache key for this query."""
        query_hash = hashlib.md5(f"{self.query}{self.database_type}".encode()).hexdigest()
        return f"query_cache:{query_hash}"


@dataclass 
class SessionStats:
    """Session statistics for analytics."""
    
    total_queries: int = 0
    successful_queries: int = 0
    failed_queries: int = 0
    average_execution_time: float = 0.0
    most_used_tables: List[str] = None
    query_patterns: List[str] = None
    
    def __post_init__(self):
        if self.most_used_tables is None:
            self.most_used_tables = []
        if self.query_patterns is None:
            self.query_patterns = []
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage."""
        if self.total_queries == 0:
            return 0.0
        return (self.successful_queries / self.total_queries) * 100


class SessionManager:
    """
    Manages user sessions and query history.
    
    Provides functionality for:
    - Query history storage and retrieval
    - Session context management
    - Query suggestions based on history
    - Performance analytics
    """
    
    def __init__(self, max_history_per_session: int = 50):
        """
        Initialize session manager.
        
        Args:
            max_history_per_session: Maximum number of queries to keep per session
        """
        self.max_history_per_session = max_history_per_session
        self._sessions: Dict[str, List[QueryHistory]] = defaultdict(list)
        self._session_stats: Dict[str, SessionStats] = defaultdict(SessionStats)
        self._session_last_activity: Dict[str, datetime] = {}
    
    async def add_query(
        self, 
        session_id: str, 
        natural_query: str,
        sql_query: str,
        execution_time: float,
        results_count: int,
        success: bool,
        database_type: str,
        error_message: str = None
    ) -> QueryHistory:
        """
        Add a query to session history.
        
        Args:
            session_id: Unique session identifier
            natural_query: Original natural language query
            sql_query: Generated SQL query
            execution_time: Query execution time in seconds
            results_count: Number of results returned
            success: Whether query executed successfully
            database_type: Type of database (postgresql, mysql, sqlite)
            error_message: Error message if query failed
            
        Returns:
            QueryHistory object for the added query
        """
        # Validate inputs
        if not session_id or not session_id.strip():
            raise ValidationError("session_id", "empty", "Session ID cannot be empty")
        
        if not natural_query or not natural_query.strip():
            raise ValidationError("natural_query", "empty", "Query cannot be empty")
        
        # Create history entry
        history_entry = QueryHistory(
            id=str(uuid.uuid4()),
            query=natural_query.strip(),
            sql=sql_query.strip() if sql_query else "",
            timestamp=datetime.now(),
            results_count=results_count,
            execution_time=execution_time,
            success=success,
            database_type=database_type,
            session_id=session_id,
            error_message=error_message
        )
        
        # Add to session history
        if session_id not in self._sessions:
            self._sessions[session_id] = []
        
        self._sessions[session_id].append(history_entry)
        
        # Maintain history limit
        if len(self._sessions[session_id]) > self.max_history_per_session:
            self._sessions[session_id] = self._sessions[session_id][-self.max_history_per_session:]
        
        # Update session statistics
        await self._update_session_stats(session_id, history_entry)
        
        # Update last activity
        self._session_last_activity[session_id] = datetime.now()
        
        return history_entry
    
    async def get_context(
        self, 
        session_id: str, 
        last_n: int = 5,
        successful_only: bool = False
    ) -> List[QueryHistory]:
        """
        Get recent queries for context.
        
        Args:
            session_id: Session identifier
            last_n: Number of recent queries to return
            successful_only: Only return successful queries
            
        Returns:
            List of recent QueryHistory objects
        """
        if session_id not in self._sessions:
            return []
        
        history = self._sessions[session_id]
        
        if successful_only:
            history = [q for q in history if q.success]
        
        return history[-last_n:] if history else []
    
    async def get_query_by_id(self, session_id: str, query_id: str) -> Optional[QueryHistory]:
        """
        Get a specific query by ID.
        
        Args:
            session_id: Session identifier
            query_id: Query identifier
            
        Returns:
            QueryHistory object if found, None otherwise
        """
        if session_id not in self._sessions:
            return None
        
        for query in self._sessions[session_id]:
            if query.id == query_id:
                return query
        
        return None
    
    async def get_similar_queries(
        self, 
        session_id: str, 
        query: str, 
        limit: int = 3
    ) -> List[QueryHistory]:
        """
        Find similar queries from history.
        
        Args:
            session_id: Session identifier
            query: Query to find similarities for
            limit: Maximum number of similar queries to return
            
        Returns:
            List of similar QueryHistory objects
        """
        if session_id not in self._sessions:
            return []
        
        query_lower = query.lower()
        similar = []
        
        for hist_query in self._sessions[session_id]:
            if hist_query.success:  # Only consider successful queries
                # Simple similarity based on common words
                similarity_score = self._calculate_similarity(query_lower, hist_query.query.lower())
                if similarity_score > 0.3:  # Threshold for similarity
                    similar.append((hist_query, similarity_score))
        
        # Sort by similarity score and return top matches
        similar.sort(key=lambda x: x[1], reverse=True)
        return [query for query, score in similar[:limit]]
    
    async def suggest_followup(self, session_id: str, current_query: str = None) -> List[str]:
        """
        Suggest follow-up queries based on history and context.
        
        Args:
            session_id: Session identifier
            current_query: Current query context (optional)
            
        Returns:
            List of suggested follow-up queries
        """
        if session_id not in self._sessions:
            return self._get_default_suggestions()
        
        recent_history = await self.get_context(session_id, 3, successful_only=True)
        suggestions = []
        
        if not recent_history:
            return self._get_default_suggestions()
        
        # Analyze recent queries for patterns
        recent_tables = set()
        recent_operations = set()
        
        for query in recent_history:
            # Extract table names and operations from SQL
            sql_lower = query.sql.lower()
            
            # Simple pattern extraction
            if 'select' in sql_lower:
                recent_operations.add('SELECT')
            if 'count' in sql_lower:
                recent_operations.add('COUNT')
            if 'group by' in sql_lower:
                recent_operations.add('GROUP BY')
            if 'order by' in sql_lower:
                recent_operations.add('ORDER BY')
            
            # Extract table names (basic parsing)
            if 'from' in sql_lower:
                parts = sql_lower.split('from')[1].split()
                if parts:
                    table_name = parts[0].strip('(),')
                    recent_tables.add(table_name)
        
        # Generate contextual suggestions
        if recent_tables:
            table = list(recent_tables)[0]  # Use first table found
            suggestions.extend([
                f"Show me the total count of records in {table}",
                f"What are the different categories in {table}?",
                f"Show me the most recent entries in {table}"
            ])
        
        if 'users' in recent_tables:
            suggestions.extend([
                "How many users registered this month?",
                "Show me user activity statistics",
                "What's the average age of users?"
            ])
        
        if 'orders' in recent_tables:
            suggestions.extend([
                "Show me today's orders",
                "What's the total revenue this week?",
                "Which products are selling the most?"
            ])
        
        # Remove duplicates and limit
        suggestions = list(dict.fromkeys(suggestions))  # Remove duplicates
        return suggestions[:5]
    
    async def get_session_stats(self, session_id: str) -> SessionStats:
        """
        Get session statistics.
        
        Args:
            session_id: Session identifier
            
        Returns:
            SessionStats object with session analytics
        """
        if session_id in self._session_stats:
            return self._session_stats[session_id]
        return SessionStats()
    
    async def cleanup_old_sessions(self, max_age_hours: int = 24):
        """
        Clean up old inactive sessions.
        
        Args:
            max_age_hours: Maximum age of sessions to keep
        """
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        sessions_to_remove = []
        
        for session_id, last_activity in self._session_last_activity.items():
            if last_activity < cutoff_time:
                sessions_to_remove.append(session_id)
        
        for session_id in sessions_to_remove:
            if session_id in self._sessions:
                del self._sessions[session_id]
            if session_id in self._session_stats:
                del self._session_stats[session_id]
            del self._session_last_activity[session_id]
    
    async def export_session_history(self, session_id: str) -> Dict[str, Any]:
        """
        Export session history for backup or analysis.
        
        Args:
            session_id: Session identifier
            
        Returns:
            Dictionary containing session history and stats
        """
        if session_id not in self._sessions:
            return {"session_id": session_id, "history": [], "stats": SessionStats().to_dict()}
        
        history_data = [query.to_dict() for query in self._sessions[session_id]]
        stats_data = self._session_stats[session_id].__dict__.copy()
        
        return {
            "session_id": session_id,
            "history": history_data,
            "stats": stats_data,
            "exported_at": datetime.now().isoformat()
        }
    
    def _calculate_similarity(self, query1: str, query2: str) -> float:
        """Calculate simple similarity score between two queries."""
        words1 = set(query1.split())
        words2 = set(query2.split())
        
        if not words1 or not words2:
            return 0.0
        
        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))
        
        return intersection / union if union > 0 else 0.0
    
    async def _update_session_stats(self, session_id: str, query: QueryHistory):
        """Update session statistics with new query."""
        stats = self._session_stats[session_id]
        
        stats.total_queries += 1
        
        if query.success:
            stats.successful_queries += 1
        else:
            stats.failed_queries += 1
        
        # Update average execution time
        total_time = stats.average_execution_time * (stats.total_queries - 1)
        stats.average_execution_time = (total_time + query.execution_time) / stats.total_queries
    
    def _get_default_suggestions(self) -> List[str]:
        """Get default query suggestions for new sessions."""
        return [
            "Show me all tables in the database",
            "How many records are in the users table?",
            "What columns are available in the orders table?",
            "Show me the latest 10 entries",
            "Give me a summary of the data"
        ]


# Global session manager instance
session_manager = SessionManager(
    max_history_per_session=config.max_result_rows if config else 50
)
