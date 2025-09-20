"""
AI-powered query analysis and suggestion tools for the MCP server.

This module provides tools that leverage AI to provide intelligent query suggestions,
result explanations, optimizations, and natural language improvements.
"""

from typing import Dict, Any, List
from fastmcp import Context

from .connection import get_database_manager
from .query import query_data
from ..ai.query_intelligence import query_intelligence
from ..core.session_manager import session_manager
from ..core.exceptions import ValidationError, QueryExecutionError
from ..core.config import config


def _get_session_id(ctx: Context) -> str:
    """Get session ID from context."""
    return getattr(ctx, 'session_id', 'default_session')


async def explain_results(ctx: Context, natural_language_query: str) -> Dict[str, Any]:
    """
    Get AI-powered explanation of query results in natural language.
    
    This tool executes a query and provides an intelligent, natural language
    explanation of what the results mean and what insights they reveal.
    
    Args:
        natural_language_query: The natural language query to execute and explain
        
    Returns:
        Dictionary containing results and AI explanation
    """
    try:
        session_id = _get_session_id(ctx)
        
        # Validate input
        if not natural_language_query or not natural_language_query.strip():
            raise ValidationError(
                "natural_language_query",
                "empty string",
                "Query cannot be empty"
            )
        
        if not query_intelligence.is_available:
            return {
                "success": False,
                "message": "AI explanation service is not available",
                "suggestions": [
                    "Check OpenAI configuration",
                    "Ensure API key is properly set",
                    "Try using the regular query_data tool instead"
                ]
            }
        
        await ctx.info(f"Executing query with AI explanation: {natural_language_query[:50]}...")
        
        # Execute the query first
        query_result = await query_data(ctx, natural_language_query)
        
        if not query_result['success']:
            return {
                "success": False,
                "message": "Query execution failed - cannot generate explanation",
                "error": query_result.get('error', 'Unknown query error')
            }
        
        if not query_result.get('results'):
            # Generate explanation for empty results
            explanation = await query_intelligence.explain_results(
                natural_language_query,
                []
            )
            
            return {
                "success": True,
                "explanation": explanation,
                "results_count": 0,
                "original_query": natural_language_query,
                "message": "Query executed successfully but returned no results"
            }
        
        # Generate AI explanation of results
        await ctx.info("Generating AI explanation of results")
        explanation = await query_intelligence.explain_results(
            natural_language_query,
            query_result['results']
        )
        
        await ctx.info("AI explanation generated successfully")
        
        return {
            "success": True,
            "explanation": explanation,
            "results_count": len(query_result['results']),
            "original_query": natural_language_query,
            "generated_sql": query_result.get('generated_sql', ''),
            "execution_time": query_result.get('execution_time', 0),
            "sample_results": query_result['results'][:3] if query_result['results'] else []
        }
        
    except ValidationError as e:
        await ctx.error(f"Query explanation failed: {e.user_message}")
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False)
        }
    except Exception as e:
        await ctx.error(f"AI explanation error: {str(e)}")
        return {
            "success": False,
            "error": {
                "message": "Failed to generate AI explanation",
                "technical_details": str(e) if config and config.debug else None
            }
        }


async def suggest_related_queries(ctx: Context, natural_language_query: str, include_schema: bool = True) -> Dict[str, Any]:
    """
    Get AI-powered suggestions for related queries based on the current query and database schema.
    
    This tool analyzes your current query and suggests related questions you might
    want to ask to gain deeper insights into your data.
    
    Args:
        natural_language_query: The current query to base suggestions on
        include_schema: Whether to include database schema in suggestion generation
        
    Returns:
        Dictionary containing related query suggestions
    """
    try:
        session_id = _get_session_id(ctx)
        
        # Validate input
        if not natural_language_query or not natural_language_query.strip():
            raise ValidationError(
                "natural_language_query",
                "empty string",
                "Query cannot be empty"
            )
        
        if not query_intelligence.is_available:
            return {
                "success": False,
                "message": "AI suggestion service is not available",
                "suggestions": [
                    "Check OpenAI configuration",
                    "Ensure API key is properly set"
                ]
            }
        
        await ctx.info("Generating AI-powered query suggestions")
        
        # Get database context if requested
        schema_info = "Database schema information not available"
        if include_schema:
            try:
                db_manager = get_database_manager(ctx)
                if db_manager and await db_manager.test_connection():
                    tables = await db_manager.get_tables()
                    if tables:
                        # Build schema summary
                        table_list = tables[:10]  # Limit to prevent prompt overflow
                        schema_info = f"Available tables: {', '.join(table_list)}"
                        
                        # Get schema for a few key tables
                        for table in table_list[:3]:
                            try:
                                schema = await db_manager.get_table_schema(table)
                                if schema.columns:
                                    column_names = [col.get('column_name', '') for col in schema.columns[:5]]
                                    schema_info += f"\n{table} columns: {', '.join(column_names)}"
                            except Exception:
                                continue
            except Exception as e:
                await ctx.warning(f"Could not get schema context: {str(e)}")
        
        # Get recent queries for context
        recent_queries = []
        try:
            history = await session_manager.get_context(session_id, 3, successful_only=True)
            recent_queries = [h.query for h in history if h.query != natural_language_query]
        except Exception:
            pass
        
        # Generate AI suggestions
        suggestions = await query_intelligence.suggest_related_queries(
            natural_language_query,
            schema_info,
            recent_queries
        )
        
        await ctx.info(f"Generated {len(suggestions)} related query suggestions")
        
        return {
            "success": True,
            "original_query": natural_language_query,
            "related_queries": suggestions,
            "suggestion_count": len(suggestions),
            "context_used": {
                "schema_included": include_schema,
                "recent_queries_count": len(recent_queries),
                "has_database_context": "tables" in schema_info.lower()
            }
        }
        
    except ValidationError as e:
        await ctx.error(f"Query suggestion failed: {e.user_message}")
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False)
        }
    except Exception as e:
        await ctx.error(f"AI suggestion error: {str(e)}")
        return {
            "success": False,
            "error": {
                "message": "Failed to generate query suggestions",
                "technical_details": str(e) if config and config.debug else None
            }
        }


async def optimize_query(ctx: Context, natural_language_query: str) -> Dict[str, Any]:
    """
    Get AI-powered optimization suggestions for a query.
    
    This tool analyzes query performance and provides intelligent recommendations
    for improving query speed, structure, and efficiency.
    
    Args:
        natural_language_query: The natural language query to optimize
        
    Returns:
        Dictionary containing optimization suggestions and performance analysis
    """
    try:
        session_id = _get_session_id(ctx)
        
        # Validate input
        if not natural_language_query or not natural_language_query.strip():
            raise ValidationError(
                "natural_language_query",
                "empty string",
                "Query cannot be empty"
            )
        
        if not query_intelligence.is_available:
            return {
                "success": False,
                "message": "AI optimization service is not available",
                "suggestions": [
                    "Check OpenAI configuration",
                    "Ensure API key is properly set",
                    "Try using the advanced query tools instead"
                ]
            }
        
        await ctx.info(f"Analyzing query for optimization: {natural_language_query[:50]}...")
        
        # Execute query to get performance stats
        query_result = await query_data(ctx, natural_language_query)
        
        if not query_result['success']:
            # Query failed - try to get improvement suggestions
            await ctx.info("Query failed - generating improvement suggestions")
            
            error_message = None
            if 'error' in query_result:
                if isinstance(query_result['error'], dict):
                    error_message = query_result['error'].get('user_message', str(query_result['error']))
                else:
                    error_message = str(query_result['error'])
            
            improvement_result = await query_intelligence.improve_query(
                natural_language_query,
                error_message
            )
            
            if improvement_result.get('success'):
                return {
                    "success": True,
                    "original_query": natural_language_query,
                    "query_failed": True,
                    "error_message": error_message,
                    "improved_query": improvement_result.get('improved_query'),
                    "improvement_explanation": improvement_result.get('explanation'),
                    "suggestions": improvement_result.get('suggestions', []),
                    "optimizations": []
                }
            else:
                return {
                    "success": False,
                    "message": "Query failed and could not generate improvements",
                    "original_error": query_result.get('error', 'Unknown error')
                }
        
        # Prepare execution statistics
        execution_stats = {
            "execution_time": query_result.get("execution_time", 0),
            "row_count": query_result.get("row_count", 0),
            "success": True,
            "has_results": bool(query_result.get('results')),
            "was_truncated": query_result.get('truncated', False)
        }
        
        # Generate optimization suggestions using AI
        await ctx.info("Generating AI optimization suggestions")
        optimizations = await query_intelligence.suggest_optimizations(
            query_result.get('generated_sql', ''),
            execution_stats
        )
        
        # Analyze query intent for additional insights
        intent_analysis = await query_intelligence.analyze_query_intent(natural_language_query)
        
        await ctx.info(f"Generated {len(optimizations)} optimization suggestions")
        
        return {
            "success": True,
            "original_query": natural_language_query,
            "generated_sql": query_result.get('generated_sql', ''),
            "execution_stats": execution_stats,
            "optimizations": optimizations,
            "intent_analysis": intent_analysis.get('analysis', {}) if intent_analysis.get('success') else {},
            "performance_rating": self._calculate_performance_rating(execution_stats),
            "recommendations": self._generate_general_recommendations(execution_stats)
        }
        
    except ValidationError as e:
        await ctx.error(f"Query optimization failed: {e.user_message}")
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False)
        }
    except Exception as e:
        await ctx.error(f"AI optimization error: {str(e)}")
        return {
            "success": False,
            "error": {
                "message": "Failed to optimize query",
                "technical_details": str(e) if config and config.debug else None
            }
        }


async def improve_query_language(ctx: Context, original_query: str) -> Dict[str, Any]:
    """
    Get AI suggestions for improving natural language query phrasing.
    
    This tool helps you rephrase your questions to get better results from
    the natural language to SQL translation system.
    
    Args:
        original_query: The original natural language query to improve
        
    Returns:
        Dictionary containing improved query and suggestions
    """
    try:
        # Validate input
        if not original_query or not original_query.strip():
            raise ValidationError(
                "original_query",
                "empty string",
                "Query cannot be empty"
            )
        
        if not query_intelligence.is_available:
            return {
                "success": False,
                "message": "AI query improvement service is not available",
                "suggestions": [
                    "Check OpenAI configuration",
                    "Ensure API key is properly set"
                ]
            }
        
        await ctx.info("Generating improved query phrasing")
        
        # Get improvement suggestions
        improvement_result = await query_intelligence.improve_query(original_query)
        
        if not improvement_result.get('success'):
            return {
                "success": False,
                "message": "Could not generate query improvements",
                "error": improvement_result.get('error', 'Unknown error')
            }
        
        await ctx.info("Query improvements generated successfully")
        
        return {
            "success": True,
            "original_query": original_query,
            "improved_query": improvement_result.get('improved_query', original_query),
            "improvement_explanation": improvement_result.get('explanation', ''),
            "additional_suggestions": improvement_result.get('suggestions', []),
            "tips": [
                "Be specific about which data you want",
                "Mention table names if you know them",
                "Use clear time ranges for date queries",
                "Specify sort order when needed",
                "Include filter conditions explicitly"
            ]
        }
        
    except ValidationError as e:
        await ctx.error(f"Query improvement failed: {e.user_message}")
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False)
        }
    except Exception as e:
        await ctx.error(f"AI improvement error: {str(e)}")
        return {
            "success": False,
            "error": {
                "message": "Failed to improve query language",
                "technical_details": str(e) if config and config.debug else None
            }
        }


async def analyze_query_intent(ctx: Context, natural_language_query: str) -> Dict[str, Any]:
    """
    Analyze the intent and components of a natural language query using AI.
    
    This tool provides deep analysis of what you're trying to accomplish
    with your query and suggests ways to make it more effective.
    
    Args:
        natural_language_query: The natural language query to analyze
        
    Returns:
        Dictionary containing detailed intent analysis
    """
    try:
        # Validate input
        if not natural_language_query or not natural_language_query.strip():
            raise ValidationError(
                "natural_language_query",
                "empty string",
                "Query cannot be empty"
            )
        
        if not query_intelligence.is_available:
            return {
                "success": False,
                "message": "AI intent analysis service is not available",
                "suggestions": [
                    "Check OpenAI configuration",
                    "Ensure API key is properly set"
                ]
            }
        
        await ctx.info("Analyzing query intent with AI")
        
        # Get intent analysis
        analysis_result = await query_intelligence.analyze_query_intent(natural_language_query)
        
        if not analysis_result.get('success'):
            return {
                "success": False,
                "message": "Could not analyze query intent",
                "error": analysis_result.get('error', 'Unknown error')
            }
        
        analysis = analysis_result.get('analysis', {})
        
        await ctx.info("Query intent analysis completed")
        
        return {
            "success": True,
            "query": natural_language_query,
            "intent_analysis": {
                "query_type": analysis.get('query_type', 'Unknown'),
                "main_entities": analysis.get('entities', []),
                "operations": analysis.get('operations', []),
                "conditions": analysis.get('conditions', []),
                "expected_result_type": analysis.get('result_type', 'Unknown'),
                "confidence_score": analysis.get('confidence_score', 0)
            },
            "recommendations": self._generate_intent_recommendations(analysis),
            "query_complexity": self._assess_query_complexity(analysis)
        }
        
    except ValidationError as e:
        await ctx.error(f"Intent analysis failed: {e.user_message}")
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False)
        }
    except Exception as e:
        await ctx.error(f"AI intent analysis error: {str(e)}")
        return {
            "success": False,
            "error": {
                "message": "Failed to analyze query intent",
                "technical_details": str(e) if config and config.debug else None
            }
        }


def _calculate_performance_rating(execution_stats: Dict[str, Any]) -> str:
    """Calculate a performance rating based on execution statistics."""
    execution_time = execution_stats.get('execution_time', 0)
    row_count = execution_stats.get('row_count', 0)
    
    if execution_time < 0.1:
        return "Excellent"
    elif execution_time < 1.0:
        return "Good"
    elif execution_time < 5.0:
        return "Fair"
    else:
        return "Poor"


def _generate_general_recommendations(execution_stats: Dict[str, Any]) -> List[str]:
    """Generate general performance recommendations."""
    recommendations = []
    
    execution_time = execution_stats.get('execution_time', 0)
    row_count = execution_stats.get('row_count', 0)
    was_truncated = execution_stats.get('was_truncated', False)
    
    if execution_time > 2.0:
        recommendations.append("Consider adding WHERE clauses to filter data")
        recommendations.append("Check if appropriate indexes exist on filtered columns")
    
    if row_count > 1000:
        recommendations.append("Large result set - consider adding LIMIT clause")
    
    if was_truncated:
        recommendations.append("Results were truncated - use more specific filters")
    
    if not recommendations:
        recommendations.append("Query performance looks good")
    
    return recommendations


def _generate_intent_recommendations(analysis: Dict[str, Any]) -> List[str]:
    """Generate recommendations based on intent analysis."""
    recommendations = []
    
    query_type = analysis.get('query_type', '').upper()
    confidence = analysis.get('confidence_score', 0)
    
    if confidence < 0.7:
        recommendations.append("Consider rephrasing your query to be more specific")
    
    if query_type == 'SELECT':
        recommendations.append("Consider adding specific column names instead of SELECT *")
        recommendations.append("Add WHERE clauses to filter results")
    
    operations = analysis.get('operations', [])
    if 'aggregation' in [op.lower() for op in operations]:
        recommendations.append("Make sure grouping columns are specified clearly")
    
    return recommendations or ["Query intent is clear"]


def _assess_query_complexity(analysis: Dict[str, Any]) -> str:
    """Assess query complexity based on intent analysis."""
    operations = analysis.get('operations', [])
    conditions = analysis.get('conditions', [])
    entities = analysis.get('entities', [])
    
    complexity_score = len(operations) + len(conditions) + len(entities)
    
    if complexity_score <= 2:
        return "Simple"
    elif complexity_score <= 5:
        return "Moderate"
    else:
        return "Complex"
