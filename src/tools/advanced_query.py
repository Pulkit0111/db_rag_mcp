"""
Advanced query analysis and optimization tools for the MCP server.

This module provides sophisticated query analysis, optimization suggestions,
and intelligent query planning capabilities to help users write better SQL.
"""

import time
from typing import Dict, Any, List
from fastmcp import Context

from .connection import get_database_manager
from ..nlp.translator import get_translator
from ..nlp.query_optimizer import QueryOptimizer, QueryComplexity
from ..core.exceptions import (
    DatabaseConnectionError, 
    QueryTranslationError, 
    QueryExecutionError,
    ValidationError
)
from ..core.config import config
from ..core.session_manager import session_manager


def _get_session_id(ctx: Context) -> str:
    """Get session ID from context."""
    return getattr(ctx, 'session_id', 'default_session')


async def explain_query(ctx: Context, natural_language_query: str) -> Dict[str, Any]:
    """
    Explain what a natural language query will do before executing it.
    
    This tool translates the query to SQL, analyzes its complexity and performance
    characteristics, and provides detailed explanations without actually executing it.
    
    Args:
        natural_language_query: The natural language query to explain
        
    Returns:
        Dictionary containing query explanation, SQL, and analysis
    """
    try:
        session_id = _get_session_id(ctx)
        
        # Validate input
        if not natural_language_query or not natural_language_query.strip():
            raise ValidationError(
                "natural_language_query",
                "empty string",
                "Query must be a non-empty string"
            )
        
        await ctx.info(f"Analyzing query: {natural_language_query[:50]}...")
        
        # Get database manager and verify connection
        db_manager = get_database_manager(ctx)
        if not db_manager:
            raise DatabaseConnectionError(
                db_type="unknown",
                technical_details="No database manager found in session"
            )
        
        if not await db_manager.test_connection():
            raise DatabaseConnectionError(
                db_type="unknown", 
                technical_details="Database connection test failed"
            )
        
        # Get database schema context
        await ctx.info("Retrieving database schema for context")
        tables = await db_manager.get_tables()
        
        if not tables:
            raise QueryTranslationError(
                query=natural_language_query,
                reason="No tables found in database",
                technical_details="Database appears to be empty"
            )
        
        # Get schema for available tables
        schemas = []
        for table_name in tables[:10]:  # Limit to first 10 tables
            try:
                schema = await db_manager.get_table_schema(table_name)
                if schema.columns:
                    schemas.append(schema)
            except Exception as e:
                await ctx.warning(f"Could not access schema for table {table_name}: {str(e)}")
        
        if not schemas:
            raise QueryTranslationError(
                query=natural_language_query,
                reason="Could not access any table schemas",
                technical_details="All schema requests failed"
            )
        
        # Translate to SQL
        await ctx.info("Translating to SQL")
        translator = get_translator()
        db_type = config.database.db_type if config else "postgresql"
        
        translation_result = await translator.translate_to_select(
            natural_language_query,
            schemas,
            database_type=db_type
        )
        
        if not translation_result["success"]:
            raise QueryTranslationError(
                query=natural_language_query,
                reason=translation_result.get('error', 'Translation failed'),
                technical_details=str(translation_result)
            )
        
        sql_query = translation_result["sql_query"]
        await ctx.info("SQL translation completed")
        
        # Analyze query complexity and performance
        await ctx.info("Analyzing query complexity and performance")
        optimizer = QueryOptimizer()
        analysis = await optimizer.analyze_query(sql_query, schemas)
        
        # Generate detailed explanation
        explanation_parts = []
        explanation_parts.append(f"This query will: {natural_language_query}")
        
        if analysis.operations:
            explanation_parts.append(f"Operations: {', '.join(analysis.operations)}")
        
        if analysis.tables:
            explanation_parts.append(f"Tables accessed: {', '.join(analysis.tables)}")
        
        if analysis.has_joins:
            explanation_parts.append("Uses table joins - performance depends on join conditions and indexes")
        
        if analysis.has_aggregations:
            explanation_parts.append("Includes aggregation functions - may require grouping and sorting")
        
        if analysis.has_subqueries:
            explanation_parts.append("Contains subqueries - complexity may be higher")
        
        # Performance predictions
        performance_prediction = "Performance should be good"
        if analysis.complexity == QueryComplexity.HIGH:
            performance_prediction = "Performance may be slower due to complexity"
        elif analysis.complexity == QueryComplexity.VERY_HIGH:
            performance_prediction = "Performance likely to be slow - consider optimization"
        
        explanation_parts.append(f"Performance prediction: {performance_prediction}")
        
        # Format optimizations for display
        optimization_suggestions = []
        for opt in analysis.optimizations:
            suggestion = {
                "category": opt.category,
                "severity": opt.severity,
                "message": opt.message,
                "explanation": opt.explanation
            }
            if opt.example:
                suggestion["example"] = opt.example
            optimization_suggestions.append(suggestion)
        
        await ctx.info(f"Analysis complete - complexity: {analysis.complexity.value}")
        
        return {
            "success": True,
            "explanation": ". ".join(explanation_parts),
            "natural_query": natural_language_query,
            "sql_query": sql_query,
            "analysis": {
                "complexity": analysis.complexity.value,
                "complexity_score": analysis.complexity_score,
                "estimated_cost": analysis.estimated_cost,
                "operations": analysis.operations,
                "tables": analysis.tables,
                "has_joins": analysis.has_joins,
                "has_subqueries": analysis.has_subqueries,
                "has_aggregations": analysis.has_aggregations
            },
            "warnings": analysis.warnings,
            "optimizations": optimization_suggestions,
            "performance_prediction": performance_prediction
        }
        
    except (DatabaseConnectionError, QueryTranslationError, ValidationError) as e:
        await ctx.error(f"Query explanation failed: {e.user_message}")
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False)
        }
    except Exception as e:
        await ctx.error(f"Unexpected error during explanation: {str(e)}")
        return {
            "success": False,
            "error": {
                "message": "Failed to explain query",
                "technical_details": str(e) if config and config.debug else None
            }
        }


async def query_with_suggestions(ctx: Context, natural_language_query: str) -> Dict[str, Any]:
    """
    Execute a query and provide performance suggestions and optimizations.
    
    This tool executes the query like query_data but adds comprehensive
    performance analysis and optimization recommendations.
    
    Args:
        natural_language_query: The natural language query to execute with analysis
        
    Returns:
        Dictionary containing results, performance data, and optimization suggestions
    """
    try:
        session_id = _get_session_id(ctx)
        start_time = time.time()
        
        await ctx.info("Executing query with performance analysis")
        
        # First, execute the regular query
        from .query import query_data
        query_result = await query_data(ctx, natural_language_query)
        
        if not query_result["success"]:
            return query_result
        
        execution_time = query_result.get("execution_time", time.time() - start_time)
        sql_query = query_result.get("generated_sql", "")
        
        # Now analyze the executed query for suggestions
        await ctx.info("Analyzing query performance and generating suggestions")
        
        optimizer = QueryOptimizer()
        analysis = await optimizer.analyze_query(sql_query)
        
        # Performance analysis based on actual execution
        performance_analysis = {
            "execution_time": execution_time,
            "rows_returned": query_result.get("row_count", 0),
            "complexity": analysis.complexity.value,
            "estimated_vs_actual": "Good" if execution_time < 1.0 else "Could be improved"
        }
        
        # Generate contextual suggestions based on results
        contextual_suggestions = []
        
        if execution_time > 2.0:
            contextual_suggestions.append("Query took longer than expected - consider adding indexes or optimizing")
        
        if query_result.get("row_count", 0) > 1000:
            contextual_suggestions.append("Large result set returned - consider adding LIMIT clause or filters")
        
        if query_result.get("truncated", False):
            contextual_suggestions.append("Results were truncated - use more specific filters to get complete data")
        
        # Combine with optimizer suggestions
        all_suggestions = contextual_suggestions + [opt.message for opt in analysis.optimizations]
        
        # Add performance analysis to the result
        enhanced_result = query_result.copy()
        enhanced_result.update({
            "performance_analysis": performance_analysis,
            "complexity_analysis": {
                "level": analysis.complexity.value,
                "score": analysis.complexity_score,
                "warnings": analysis.warnings
            },
            "optimization_suggestions": all_suggestions,
            "query_insights": {
                "operations": analysis.operations,
                "tables_accessed": analysis.tables,
                "has_joins": analysis.has_joins,
                "has_aggregations": analysis.has_aggregations,
                "has_subqueries": analysis.has_subqueries
            }
        })
        
        await ctx.info(f"Performance analysis complete - {len(all_suggestions)} suggestions generated")
        
        return enhanced_result
        
    except Exception as e:
        await ctx.error(f"Error in query with suggestions: {str(e)}")
        return {
            "success": False,
            "error": {
                "message": "Failed to execute query with suggestions",
                "technical_details": str(e) if config and config.debug else None
            }
        }


async def aggregate_data(ctx: Context, natural_language_query: str, aggregation_type: str = "auto") -> Dict[str, Any]:
    """
    Specialized tool for aggregation queries with automatic optimization.
    
    This tool is optimized for queries involving COUNT, SUM, AVG, MIN, MAX,
    and GROUP BY operations, providing enhanced performance and analysis.
    
    Args:
        natural_language_query: Query requesting aggregated data
        aggregation_type: Type of aggregation ("count", "sum", "avg", "group", "auto")
        
    Returns:
        Dictionary containing aggregated results and performance insights
    """
    try:
        session_id = _get_session_id(ctx)
        
        # Validate inputs
        if not natural_language_query or not natural_language_query.strip():
            raise ValidationError(
                "natural_language_query",
                "empty string", 
                "Query must be a non-empty string"
            )
        
        valid_types = ["auto", "count", "sum", "avg", "min", "max", "group"]
        if aggregation_type.lower() not in valid_types:
            raise ValidationError(
                "aggregation_type",
                aggregation_type,
                f"Must be one of: {', '.join(valid_types)}"
            )
        
        await ctx.info(f"Processing aggregation query: {aggregation_type}")
        
        # Get database connection
        db_manager = get_database_manager(ctx)
        if not db_manager or not await db_manager.test_connection():
            raise DatabaseConnectionError(
                db_type="unknown",
                technical_details="No active database connection"
            )
        
        # Get schema context
        tables = await db_manager.get_tables()
        schemas = []
        for table_name in tables[:5]:  # Limit for aggregation queries
            try:
                schema = await db_manager.get_table_schema(table_name)
                if schema.columns:
                    schemas.append(schema)
            except Exception:
                continue
        
        if not schemas:
            raise QueryTranslationError(
                query=natural_language_query,
                reason="No accessible table schemas found"
            )
        
        # Enhance query for aggregation if needed
        enhanced_query = natural_language_query
        if aggregation_type != "auto":
            aggregation_hints = {
                "count": "count the number of",
                "sum": "sum the total of",
                "avg": "calculate the average of", 
                "min": "find the minimum value of",
                "max": "find the maximum value of",
                "group": "group by and summarize"
            }
            
            if aggregation_type in aggregation_hints and aggregation_hints[aggregation_type] not in enhanced_query.lower():
                enhanced_query = f"{aggregation_hints[aggregation_type]} {natural_language_query}"
        
        # Translate with aggregation context
        translator = get_translator()
        db_type = config.database.db_type if config else "postgresql"
        
        translation_result = await translator.translate_to_select(
            enhanced_query,
            schemas,
            database_type=db_type
        )
        
        if not translation_result["success"]:
            raise QueryTranslationError(
                query=enhanced_query,
                reason=translation_result.get('error', 'Translation failed')
            )
        
        sql_query = translation_result["sql_query"]
        
        # Optimize for aggregation
        optimizer = QueryOptimizer()
        optimized_sql = await optimizer.optimize_query(sql_query, schemas)
        
        # Execute the optimized query
        start_time = time.time()
        query_result = await db_manager.execute_query(optimized_sql)
        execution_time = time.time() - start_time
        
        if not query_result.success:
            raise QueryExecutionError(
                sql_query=optimized_sql,
                db_error=query_result.error_message
            )
        
        # Analyze aggregation performance
        analysis = await optimizer.analyze_query(optimized_sql, schemas)
        
        # Detect aggregation type from SQL
        sql_upper = optimized_sql.upper()
        detected_aggregations = []
        if 'COUNT(' in sql_upper:
            detected_aggregations.append('COUNT')
        if 'SUM(' in sql_upper:
            detected_aggregations.append('SUM') 
        if 'AVG(' in sql_upper:
            detected_aggregations.append('AVG')
        if 'MIN(' in sql_upper:
            detected_aggregations.append('MIN')
        if 'MAX(' in sql_upper:
            detected_aggregations.append('MAX')
        if 'GROUP BY' in sql_upper:
            detected_aggregations.append('GROUP BY')
        
        # Record in session history
        if config and config.enable_query_history:
            try:
                await session_manager.add_query(
                    session_id=session_id,
                    natural_query=natural_language_query,
                    sql_query=optimized_sql,
                    execution_time=execution_time,
                    results_count=query_result.row_count,
                    success=True,
                    database_type=db_type
                )
            except Exception as e:
                await ctx.warning(f"Failed to record aggregation query: {str(e)}")
        
        await ctx.info(f"Aggregation complete - {query_result.row_count} result rows")
        
        return {
            "success": True,
            "message": f"Aggregation query executed successfully",
            "original_query": natural_language_query,
            "enhanced_query": enhanced_query if enhanced_query != natural_language_query else None,
            "generated_sql": optimized_sql,
            "aggregation_type": aggregation_type,
            "detected_aggregations": detected_aggregations,
            "execution_time": round(execution_time, 3),
            "row_count": query_result.row_count,
            "results": query_result.data,
            "performance": {
                "complexity": analysis.complexity.value,
                "optimizations_applied": len(analysis.optimizations),
                "warnings": analysis.warnings
            }
        }
        
    except (DatabaseConnectionError, QueryTranslationError, QueryExecutionError, ValidationError) as e:
        await ctx.error(f"Aggregation query failed: {e.user_message}")
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False)
        }
    except Exception as e:
        await ctx.error(f"Unexpected error in aggregation: {str(e)}")
        return {
            "success": False,
            "error": {
                "message": "Failed to process aggregation query", 
                "technical_details": str(e) if config and config.debug else None
            }
        }
