"""
Query optimization and analysis service for the Natural Language SQL MCP Server.

This module provides intelligent SQL query analysis, optimization suggestions,
and performance predictions to help users write better queries.
"""

import re
import sqlparse
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from ..database.base_manager import TableSchema
from ..core.exceptions import QueryExecutionError, ValidationError


class QueryComplexity(Enum):
    """Enum for query complexity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    VERY_HIGH = "very_high"


@dataclass
class OptimizationSuggestion:
    """Represents an optimization suggestion for a query."""
    
    category: str
    severity: str  # 'low', 'medium', 'high'
    message: str
    explanation: str
    example: Optional[str] = None


@dataclass
class QueryAnalysis:
    """Complete analysis of a SQL query."""
    
    complexity: QueryComplexity
    complexity_score: int
    estimated_cost: str
    warnings: List[str]
    optimizations: List[OptimizationSuggestion]
    operations: List[str]
    tables: List[str]
    has_joins: bool
    has_subqueries: bool
    has_aggregations: bool
    estimated_rows: Optional[int] = None


class QueryOptimizer:
    """
    Analyzes and optimizes SQL queries for better performance.
    
    Provides functionality for:
    - Query complexity analysis
    - Performance optimization suggestions
    - Query pattern recognition
    - Cost estimation
    """
    
    def __init__(self):
        """Initialize the query optimizer."""
        self.expensive_operations = {
            'GROUP BY': 2,
            'ORDER BY': 2,
            'DISTINCT': 1,
            'JOIN': 3,
            'INNER JOIN': 3,
            'LEFT JOIN': 4,
            'RIGHT JOIN': 4,
            'FULL JOIN': 5,
            'CROSS JOIN': 6,
            'UNION': 3,
            'UNION ALL': 2,
            'SUBQUERY': 4,
            'WINDOW': 4,
            'HAVING': 2,
            'EXISTS': 3,
            'IN \\(SELECT': 4,  # Subquery in IN clause
            'NOT EXISTS': 3,
            'CASE WHEN': 1
        }
    
    async def optimize_query(self, sql: str, schemas: List[TableSchema]) -> str:
        """
        Optimize a SQL query by adding performance improvements.
        
        Args:
            sql: The SQL query to optimize
            schemas: Database table schemas for context
            
        Returns:
            Optimized SQL query string
        """
        if not sql or not sql.strip():
            raise ValidationError("sql", "empty", "SQL query cannot be empty")
        
        optimized_sql = sql.strip()
        
        # Parse the SQL to understand structure
        try:
            parsed = sqlparse.parse(optimized_sql)[0]
        except Exception:
            # If parsing fails, return original with basic optimizations
            return self._apply_basic_optimizations(optimized_sql)
        
        # Apply various optimizations
        optimized_sql = self._add_reasonable_limit(optimized_sql)
        optimized_sql = self._optimize_where_clauses(optimized_sql, schemas)
        optimized_sql = self._optimize_joins(optimized_sql)
        
        return optimized_sql
    
    async def analyze_query(self, sql: str, schemas: List[TableSchema] = None) -> QueryAnalysis:
        """
        Perform comprehensive analysis of a SQL query.
        
        Args:
            sql: The SQL query to analyze
            schemas: Optional database schemas for enhanced analysis
            
        Returns:
            QueryAnalysis object with detailed analysis
        """
        if not sql or not sql.strip():
            raise ValidationError("sql", "empty", "SQL query cannot be empty")
        
        sql_upper = sql.upper()
        sql_clean = re.sub(r'\s+', ' ', sql.strip())
        
        # Calculate complexity score
        complexity_score = self._calculate_complexity_score(sql_upper)
        
        # Determine complexity level
        if complexity_score <= 2:
            complexity = QueryComplexity.LOW
        elif complexity_score <= 5:
            complexity = QueryComplexity.MEDIUM
        elif complexity_score <= 8:
            complexity = QueryComplexity.HIGH
        else:
            complexity = QueryComplexity.VERY_HIGH
        
        # Estimate cost
        estimated_cost = self._estimate_query_cost(complexity_score)
        
        # Generate warnings
        warnings = self._generate_warnings(sql_upper, complexity_score)
        
        # Generate optimization suggestions
        optimizations = self._generate_optimizations(sql_upper, schemas)
        
        # Extract query operations and tables
        operations = self._extract_operations(sql_upper)
        tables = self._extract_tables(sql_clean)
        
        # Analyze query patterns
        has_joins = any(join in sql_upper for join in ['JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN'])
        has_subqueries = '(' in sql and ('SELECT' in sql_upper.split('(', 1)[1] if '(' in sql else False)
        has_aggregations = any(func in sql_upper for func in ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'GROUP BY'])
        
        return QueryAnalysis(
            complexity=complexity,
            complexity_score=complexity_score,
            estimated_cost=estimated_cost,
            warnings=warnings,
            optimizations=optimizations,
            operations=operations,
            tables=tables,
            has_joins=has_joins,
            has_subqueries=has_subqueries,
            has_aggregations=has_aggregations
        )
    
    def _calculate_complexity_score(self, sql: str) -> int:
        """Calculate complexity score based on operations present."""
        score = 0
        
        for operation, points in self.expensive_operations.items():
            if operation == 'IN \\(SELECT':
                # Special case for subqueries in IN clause
                if re.search(r'IN\s*\(\s*SELECT', sql):
                    score += points
            else:
                if operation in sql:
                    # Count occurrences for some operations
                    if operation in ['JOIN', 'UNION']:
                        count = sql.count(operation)
                        score += points * count
                    else:
                        score += points
        
        # Additional complexity for nested queries
        nesting_level = sql.count('(') - sql.count(')')
        if abs(nesting_level) > 2:
            score += 2
        
        return score
    
    def _estimate_query_cost(self, complexity_score: int) -> str:
        """Estimate query execution cost based on complexity."""
        if complexity_score <= 2:
            return "Very Low"
        elif complexity_score <= 4:
            return "Low"
        elif complexity_score <= 6:
            return "Medium"
        elif complexity_score <= 8:
            return "High"
        else:
            return "Very High"
    
    def _generate_warnings(self, sql: str, complexity_score: int) -> List[str]:
        """Generate performance warnings for the query."""
        warnings = []
        
        if complexity_score > 6:
            warnings.append("Query has high complexity - consider optimization or breaking into smaller queries")
        
        if 'SELECT *' in sql:
            warnings.append("Avoid SELECT * - specify only needed columns for better performance")
        
        if 'FULL JOIN' in sql or 'CROSS JOIN' in sql:
            warnings.append("Full and cross joins can be very expensive - ensure they are necessary")
        
        if re.search(r'IN\s*\(\s*SELECT', sql):
            warnings.append("Consider using EXISTS instead of IN with subqueries for better performance")
        
        if sql.count('OR') > 3:
            warnings.append("Multiple OR conditions can slow down queries - consider using UNION or restructuring")
        
        if 'HAVING' in sql and 'WHERE' not in sql:
            warnings.append("Consider filtering with WHERE before grouping rather than using only HAVING")
        
        if re.search(r'LIKE\s+\'%.*%\'', sql):
            warnings.append("Leading wildcard in LIKE pattern prevents index usage")
        
        return warnings
    
    def _generate_optimizations(self, sql: str, schemas: List[TableSchema] = None) -> List[OptimizationSuggestion]:
        """Generate specific optimization suggestions."""
        optimizations = []
        
        # Index suggestions
        if 'WHERE' in sql:
            optimizations.append(OptimizationSuggestion(
                category="indexing",
                severity="medium",
                message="Ensure indexes exist on WHERE clause columns",
                explanation="Indexes on filtered columns can significantly improve query performance",
                example="CREATE INDEX idx_column_name ON table_name (column_name);"
            ))
        
        if 'ORDER BY' in sql:
            optimizations.append(OptimizationSuggestion(
                category="indexing",
                severity="medium",
                message="Consider composite index for ORDER BY columns",
                explanation="Indexes on ORDER BY columns can eliminate sorting overhead",
                example="CREATE INDEX idx_sort ON table_name (sort_column1, sort_column2);"
            ))
        
        # Query structure suggestions
        if 'SELECT *' in sql:
            optimizations.append(OptimizationSuggestion(
                category="structure",
                severity="high",
                message="Replace SELECT * with specific column names",
                explanation="Selecting only needed columns reduces I/O and memory usage",
                example="SELECT id, name, email FROM users; -- instead of SELECT * FROM users;"
            ))
        
        if re.search(r'IN\s*\(\s*SELECT', sql):
            optimizations.append(OptimizationSuggestion(
                category="structure",
                severity="medium",
                message="Consider using EXISTS instead of IN with subquery",
                explanation="EXISTS can be more efficient than IN for subqueries",
                example="WHERE EXISTS (SELECT 1 FROM table WHERE condition) -- instead of IN (SELECT id FROM table WHERE condition)"
            ))
        
        # LIMIT suggestions
        if 'SELECT' in sql and 'LIMIT' not in sql and 'TOP' not in sql:
            optimizations.append(OptimizationSuggestion(
                category="performance",
                severity="medium",
                message="Add LIMIT clause to prevent large result sets",
                explanation="Limiting results improves response time and reduces memory usage",
                example="SELECT * FROM table WHERE condition LIMIT 100;"
            ))
        
        # Join optimization
        if 'JOIN' in sql:
            optimizations.append(OptimizationSuggestion(
                category="joins",
                severity="medium",
                message="Ensure join conditions use indexed columns",
                explanation="Joins on indexed columns are much faster",
                example="Create indexes on both sides of join conditions"
            ))
        
        return optimizations
    
    def _extract_operations(self, sql: str) -> List[str]:
        """Extract SQL operations from the query."""
        operations = []
        
        operation_patterns = [
            'SELECT', 'INSERT', 'UPDATE', 'DELETE',
            'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN',
            'GROUP BY', 'ORDER BY', 'HAVING', 'UNION', 'DISTINCT',
            'EXISTS', 'IN', 'LIKE', 'BETWEEN'
        ]
        
        for op in operation_patterns:
            if op in sql:
                operations.append(op)
        
        return operations
    
    def _extract_tables(self, sql: str) -> List[str]:
        """Extract table names from the SQL query."""
        tables = []
        
        # Simple regex patterns for table extraction
        # This is basic and could be improved with proper SQL parsing
        from_pattern = r'\bFROM\s+(\w+)'
        join_pattern = r'\bJOIN\s+(\w+)'
        update_pattern = r'\bUPDATE\s+(\w+)'
        insert_pattern = r'\bINSERT\s+INTO\s+(\w+)'
        
        patterns = [from_pattern, join_pattern, update_pattern, insert_pattern]
        
        sql_upper = sql.upper()
        for pattern in patterns:
            matches = re.findall(pattern, sql_upper)
            tables.extend(matches)
        
        # Remove duplicates while preserving order
        return list(dict.fromkeys(tables))
    
    def _add_reasonable_limit(self, sql: str) -> str:
        """Add LIMIT clause if missing from SELECT queries."""
        sql_upper = sql.upper().strip()
        
        # Only add LIMIT to SELECT queries without existing LIMIT
        if (sql_upper.startswith('SELECT') and 
            'LIMIT' not in sql_upper and 
            'TOP' not in sql_upper):
            return sql.rstrip(';') + ' LIMIT 1000;'
        
        return sql
    
    def _optimize_where_clauses(self, sql: str, schemas: List[TableSchema] = None) -> str:
        """Optimize WHERE clauses for better performance."""
        # This is a simplified optimization
        # In a real implementation, you'd use proper SQL parsing
        return sql
    
    def _optimize_joins(self, sql: str) -> str:
        """Optimize JOIN operations."""
        # This is a simplified optimization
        # In a real implementation, you'd analyze join conditions
        return sql
    
    def _apply_basic_optimizations(self, sql: str) -> str:
        """Apply basic optimizations when SQL parsing fails."""
        optimized = sql
        
        # Add LIMIT if missing
        optimized = self._add_reasonable_limit(optimized)
        
        return optimized
    
    async def validate_query_complexity(self, sql: str) -> Dict[str, Any]:
        """
        Validate query complexity and provide recommendations.
        
        Args:
            sql: The SQL query to validate
            
        Returns:
            Dictionary with complexity analysis and recommendations
        """
        analysis = await self.analyze_query(sql)
        
        return {
            "complexity": analysis.complexity.value,
            "complexity_score": analysis.complexity_score,
            "estimated_cost": analysis.estimated_cost,
            "is_complex": analysis.complexity_score > 5,
            "warnings": analysis.warnings,
            "recommendations": [opt.message for opt in analysis.optimizations],
            "operations": analysis.operations,
            "tables": analysis.tables
        }
