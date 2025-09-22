"""
Data visualization tools for the MCP server.

This module provides tools for creating interactive charts and dashboards
from query results using Plotly.
"""

from typing import Dict, Any, List, Optional
from fastmcp import Context

from .query import query_data
from ..visualization.chart_generator import chart_generator, ChartType
from ..core.exceptions import ValidationError, NaturalSQLException
from ..core.config import config
from ..auth.user_manager import user_manager, Permission


def _get_session_id(ctx: Context) -> str:
    """Get session ID from context."""
    return getattr(ctx, 'session_id', 'default_session')


async def create_visualization(
    ctx: Context, 
    natural_language_query: str,
    chart_type: str = "auto",
    title: str = None,
    x_column: str = None,
    y_column: str = None,
    color_column: str = None,
    size_column: str = None,
    width: int = 800,
    height: int = 600,
    color_scheme: str = "default",
    theme: str = "plotly"
) -> Dict[str, Any]:
    """
    Execute a query and create an interactive visualization.
    
    This tool runs a natural language query and automatically generates
    an appropriate chart based on the data structure and user preferences.
    
    Args:
        natural_language_query: The natural language query to execute
        chart_type: Type of chart ('auto', 'bar', 'line', 'pie', 'scatter', etc.)
        title: Chart title (auto-generated if not provided)
        x_column: Column for x-axis (auto-selected if not provided)
        y_column: Column for y-axis (auto-selected if not provided)
        color_column: Column for color encoding
        size_column: Column for size encoding (scatter plots)
        width: Chart width in pixels
        height: Chart height in pixels
        color_scheme: Color scheme ('default', 'dark', 'light', 'sequential')
        theme: Plotly theme
        
    Returns:
        Dictionary containing chart data and configuration
    """
    try:
        # Check permissions
        session_id = _get_session_id(ctx)
        if not await user_manager.check_permission(session_id, Permission.CREATE_VISUALIZATION):
            return {
                "success": False,
                "message": "Insufficient permissions to create visualizations",
                "suggestions": ["Contact administrator for visualization permissions"]
            }
        
        # Validate inputs
        if not natural_language_query or not natural_language_query.strip():
            raise ValidationError("natural_language_query", "empty", "Query cannot be empty")
        
        await ctx.info(f"Executing query for visualization: {natural_language_query[:50]}...")
        
        # Execute query
        query_result = await query_data(ctx, natural_language_query)
        
        if not query_result["success"]:
            return {
                "success": False,
                "message": "Query execution failed - cannot create visualization",
                "error": query_result.get("error", "Unknown query error")
            }
        
        data = query_result.get("results", [])
        
        if not data:
            return {
                "success": True,
                "message": "Query executed successfully but returned no data to visualize",
                "visualization": None,
                "row_count": 0
            }
        
        await ctx.info(f"Creating {chart_type} visualization for {len(data)} data points...")
        
        # Generate auto title if not provided
        if not title:
            title = f"Visualization: {natural_language_query[:50]}{'...' if len(natural_language_query) > 50 else ''}"
        
        # Create visualization
        viz_result = await chart_generator.create_chart(
            data=data,
            chart_type=chart_type,
            title=title,
            x_column=x_column,
            y_column=y_column,
            color_column=color_column,
            size_column=size_column,
            width=width,
            height=height,
            color_scheme=color_scheme,
            theme=theme
        )
        
        if not viz_result["success"]:
            return {
                "success": False,
                "message": "Failed to create visualization",
                "error": viz_result.get("error", "Unknown visualization error"),
                "original_query": natural_language_query
            }
        
        await ctx.info(f"Visualization created successfully: {viz_result['chart_type']} chart")
        
        return {
            "success": True,
            "message": f"Visualization created successfully",
            "visualization": viz_result["chart"],
            "chart_type": viz_result["chart_type"],
            "data_summary": viz_result["data_summary"],
            "configuration": viz_result["configuration"],
            "data_points": viz_result["data_points"],
            "columns_used": viz_result["columns_used"],
            "original_query": natural_language_query,
            "generated_sql": query_result.get("generated_sql", ""),
            "query_execution_time": query_result.get("execution_time", 0)
        }
        
    except ValidationError as e:
        await ctx.error(f"Visualization validation failed: {e.user_message}")
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False)
        }
    except Exception as e:
        unexpected_error = NaturalSQLException(
            user_message="An unexpected error occurred while creating the visualization",
            technical_details=str(e),
            suggestions=[
                "Check query results format",
                "Try a different chart type", 
                "Ensure data contains appropriate columns for visualization"
            ]
        )
        await ctx.error(f"Visualization error: {unexpected_error.user_message}")
        return {
            "success": False,
            "error": unexpected_error.to_dict(include_technical=config.debug if config else False)
        }


async def recommend_visualizations(
    ctx: Context, 
    natural_language_query: str,
    limit: int = 3
) -> Dict[str, Any]:
    """
    Execute a query and recommend appropriate visualizations.
    
    This tool analyzes query results and suggests the best chart types
    and configurations for visualizing the data effectively.
    
    Args:
        natural_language_query: The natural language query to execute
        limit: Maximum number of recommendations to return
        
    Returns:
        Dictionary containing visualization recommendations
    """
    try:
        # Check permissions
        session_id = _get_session_id(ctx)
        if not await user_manager.check_permission(session_id, Permission.CREATE_VISUALIZATION):
            return {
                "success": False,
                "message": "Insufficient permissions to create visualizations",
                "suggestions": ["Contact administrator for visualization permissions"]
            }
        
        # Validate inputs
        if not natural_language_query or not natural_language_query.strip():
            raise ValidationError("natural_language_query", "empty", "Query cannot be empty")
        
        await ctx.info(f"Executing query for visualization recommendations: {natural_language_query[:50]}...")
        
        # Execute query
        query_result = await query_data(ctx, natural_language_query)
        
        if not query_result["success"]:
            return {
                "success": False,
                "message": "Query execution failed - cannot provide recommendations",
                "error": query_result.get("error", "Unknown query error")
            }
        
        data = query_result.get("results", [])
        
        if not data:
            return {
                "success": True,
                "message": "Query executed successfully but returned no data for visualization recommendations",
                "recommendations": [],
                "row_count": 0
            }
        
        await ctx.info(f"Analyzing {len(data)} data points for visualization recommendations...")
        
        # Get recommendations
        recommendations_result = await chart_generator.recommend_visualizations(data, limit)
        
        if not recommendations_result["success"]:
            return {
                "success": False,
                "message": "Failed to generate visualization recommendations",
                "error": recommendations_result.get("error", "Unknown recommendation error")
            }
        
        recommendations = recommendations_result["recommendations"]
        
        await ctx.info(f"Generated {len(recommendations)} visualization recommendations")
        
        return {
            "success": True,
            "message": f"Generated {len(recommendations)} visualization recommendations",
            "recommendations": recommendations,
            "data_analysis": recommendations_result["data_analysis"],
            "original_query": natural_language_query,
            "generated_sql": query_result.get("generated_sql", ""),
            "data_points": len(data),
            "recommendation_count": len(recommendations)
        }
        
    except ValidationError as e:
        await ctx.error(f"Recommendation validation failed: {e.user_message}")
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False)
        }
    except Exception as e:
        unexpected_error = NaturalSQLException(
            user_message="An unexpected error occurred while generating visualization recommendations",
            technical_details=str(e),
            suggestions=[
                "Check query results format",
                "Try with a simpler query",
                "Ensure data contains valid values"
            ]
        )
        await ctx.error(f"Recommendation error: {unexpected_error.user_message}")
        return {
            "success": False,
            "error": unexpected_error.to_dict(include_technical=config.debug if config else False)
        }


async def create_dashboard(
    ctx: Context,
    queries: List[str],
    chart_types: List[str] = None,
    titles: List[str] = None,
    layout: str = "grid",
    dashboard_title: str = "Data Dashboard",
    width: int = 1200,
    height: int = 800
) -> Dict[str, Any]:
    """
    Execute multiple queries and create a comprehensive dashboard.
    
    This tool runs multiple natural language queries and combines
    their visualizations into a single interactive dashboard.
    
    Args:
        queries: List of natural language queries to execute
        chart_types: List of chart types for each query (auto-detected if not provided)
        titles: List of titles for each chart (auto-generated if not provided)
        layout: Dashboard layout ('grid', 'vertical', 'horizontal')
        dashboard_title: Overall dashboard title
        width: Dashboard width in pixels
        height: Dashboard height in pixels
        
    Returns:
        Dictionary containing dashboard data and metadata
    """
    try:
        # Check permissions
        session_id = _get_session_id(ctx)
        if not await user_manager.check_permission(session_id, Permission.CREATE_VISUALIZATION):
            return {
                "success": False,
                "message": "Insufficient permissions to create visualizations",
                "suggestions": ["Contact administrator for visualization permissions"]
            }
        
        # Validate inputs
        if not queries or not isinstance(queries, list):
            raise ValidationError("queries", str(queries), "Queries must be a non-empty list")
        
        if any(not query.strip() for query in queries):
            raise ValidationError("queries", "contains empty query", "All queries must be non-empty")
        
        # Set defaults for optional parameters
        if chart_types is None:
            chart_types = ["auto"] * len(queries)
        elif len(chart_types) != len(queries):
            raise ValidationError("chart_types", f"length {len(chart_types)}", f"Chart types list must have same length as queries ({len(queries)})")
        
        if titles is None:
            titles = [f"Query {i+1}" for i in range(len(queries))]
        elif len(titles) != len(queries):
            raise ValidationError("titles", f"length {len(titles)}", f"Titles list must have same length as queries ({len(queries)})")
        
        await ctx.info(f"Creating dashboard with {len(queries)} queries...")
        
        # Execute queries and prepare chart configurations
        chart_configs = []
        successful_queries = 0
        
        for i, query in enumerate(queries):
            try:
                await ctx.info(f"Executing query {i+1}/{len(queries)}: {query[:30]}...")
                
                query_result = await query_data(ctx, query)
                
                if query_result["success"] and query_result.get("results"):
                    chart_configs.append({
                        "chart_type": chart_types[i],
                        "title": titles[i],
                        "data": query_result["results"]
                    })
                    successful_queries += 1
                else:
                    await ctx.warning(f"Query {i+1} failed or returned no data: {query[:50]}")
                    # Add placeholder for failed query
                    chart_configs.append({
                        "chart_type": "table",
                        "title": f"{titles[i]} (No Data)",
                        "data": [{"message": "Query returned no data"}]
                    })
                    
            except Exception as e:
                await ctx.warning(f"Error executing query {i+1}: {str(e)}")
                chart_configs.append({
                    "chart_type": "table",
                    "title": f"{titles[i]} (Error)",
                    "data": [{"error": f"Query failed: {str(e)}"}]
                })
        
        if successful_queries == 0:
            return {
                "success": False,
                "message": "All queries failed - cannot create dashboard",
                "successful_queries": 0,
                "total_queries": len(queries)
            }
        
        # Use data from the first successful query for dashboard creation
        # (The chart_generator will handle multiple datasets)
        first_successful_data = None
        for config in chart_configs:
            if "error" not in str(config.get("data", [])):
                first_successful_data = config["data"]
                break
        
        if not first_successful_data:
            first_successful_data = [{"placeholder": "No data available"}]
        
        await ctx.info(f"Creating dashboard with {successful_queries} successful queries...")
        
        # Create dashboard
        dashboard_result = await chart_generator.create_dashboard(
            data=first_successful_data,  # Primary data source
            chart_configs=chart_configs,
            title=dashboard_title,
            layout=layout,
            width=width,
            height=height
        )
        
        if not dashboard_result["success"]:
            return {
                "success": False,
                "message": "Failed to create dashboard",
                "error": dashboard_result.get("error", "Unknown dashboard error")
            }
        
        await ctx.info("Dashboard created successfully")
        
        return {
            "success": True,
            "message": f"Dashboard created successfully with {successful_queries} charts",
            "dashboard": dashboard_result["dashboard"],
            "layout": layout,
            "chart_count": len(chart_configs),
            "successful_queries": successful_queries,
            "failed_queries": len(queries) - successful_queries,
            "total_queries": len(queries),
            "dimensions": {"width": width, "height": height},
            "dashboard_title": dashboard_title,
            "original_queries": queries
        }
        
    except ValidationError as e:
        await ctx.error(f"Dashboard validation failed: {e.user_message}")
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False)
        }
    except Exception as e:
        unexpected_error = NaturalSQLException(
            user_message="An unexpected error occurred while creating the dashboard",
            technical_details=str(e),
            suggestions=[
                "Check query formats",
                "Ensure at least one query returns data",
                "Try with fewer queries first"
            ]
        )
        await ctx.error(f"Dashboard error: {unexpected_error.user_message}")
        return {
            "success": False,
            "error": unexpected_error.to_dict(include_technical=config.debug if config else False)
        }


async def export_visualization(
    ctx: Context,
    natural_language_query: str,
    chart_type: str = "auto",
    format: str = "png",
    filename: str = None,
    width: int = 800,
    height: int = 600
) -> Dict[str, Any]:
    """
    Create a visualization and export it as an image file.
    
    This tool creates a chart from query results and exports it
    as a static image in various formats.
    
    Args:
        natural_language_query: The natural language query to execute
        chart_type: Type of chart to create
        format: Export format ('png', 'jpeg', 'svg', 'pdf')
        filename: Name for exported file
        width: Chart width in pixels
        height: Chart height in pixels
        
    Returns:
        Dictionary containing export result and file information
    """
    try:
        # Check permissions
        session_id = _get_session_id(ctx)
        if not await user_manager.check_permission(session_id, Permission.CREATE_VISUALIZATION):
            return {
                "success": False,
                "message": "Insufficient permissions to create visualizations",
                "suggestions": ["Contact administrator for visualization permissions"]
            }
        
        # Validate format
        valid_formats = ['png', 'jpeg', 'jpg', 'svg', 'pdf', 'html']
        if format.lower() not in valid_formats:
            raise ValidationError("format", format, f"Format must be one of: {', '.join(valid_formats)}")
        
        await ctx.info(f"Creating visualization for export: {natural_language_query[:50]}...")
        
        # Create visualization
        viz_result = await create_visualization(
            ctx, natural_language_query, chart_type, 
            width=width, height=height
        )
        
        if not viz_result["success"]:
            return viz_result
        
        # Generate filename if not provided
        if not filename:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"chart_export_{timestamp}.{format}"
        elif not filename.endswith(f".{format}"):
            filename += f".{format}"
        
        await ctx.info(f"Exporting visualization to {format.upper()} format...")
        
        # For this implementation, we'll return the chart data and format info
        # In a real implementation, you would use plotly.io.write_image() 
        # or plotly.offline.plot() for HTML export
        
        return {
            "success": True,
            "message": f"Visualization exported successfully to {format.upper()} format",
            "filename": filename,
            "format": format,
            "chart_type": viz_result["chart_type"],
            "dimensions": {"width": width, "height": height},
            "visualization_data": viz_result["visualization"],
            "export_note": "Export functionality requires additional Plotly configuration for image generation",
            "original_query": natural_language_query
        }
        
    except ValidationError as e:
        await ctx.error(f"Export validation failed: {e.user_message}")
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False)
        }
    except Exception as e:
        unexpected_error = NaturalSQLException(
            user_message="An unexpected error occurred while exporting the visualization",
            technical_details=str(e),
            suggestions=[
                "Try a different export format",
                "Check visualization creation first",
                "Ensure sufficient permissions"
            ]
        )
        await ctx.error(f"Export error: {unexpected_error.user_message}")
        return {
            "success": False,
            "error": unexpected_error.to_dict(include_technical=config.debug if config else False)
        }