"""
Data visualization service using Plotly for the Natural Language SQL MCP Server.

This module provides intelligent chart generation from SQL query results,
with support for various chart types and automatic visualization recommendations.
"""

import json
from typing import Dict, Any, List, Optional, Tuple
from enum import Enum
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.io as pio

from ..core.config import config
from ..core.exceptions import ValidationError, NaturalSQLException


class ChartType(Enum):
    """Supported chart types."""
    
    BAR = "bar"
    LINE = "line"  
    PIE = "pie"
    SCATTER = "scatter"
    HISTOGRAM = "histogram"
    BOX = "box"
    HEATMAP = "heatmap"
    TABLE = "table"
    AREA = "area"
    DONUT = "donut"
    SUNBURST = "sunburst"
    TREEMAP = "treemap"


class DataType(Enum):
    """Data type classification for visualization recommendations."""
    
    NUMERIC = "numeric"
    CATEGORICAL = "categorical"
    DATETIME = "datetime"
    TEXT = "text"
    BOOLEAN = "boolean"


class ChartGenerator:
    """
    Generates interactive charts from query results using Plotly.
    
    Provides functionality for:
    - Automatic chart type recommendations
    - Interactive chart generation
    - Chart customization
    - Export in various formats
    """
    
    def __init__(self):
        """Initialize chart generator with Plotly settings."""
        # Set Plotly renderer for better compatibility
        pio.renderers.default = "json"
        
        # Chart color schemes
        self.color_schemes = {
            'default': px.colors.qualitative.Set3,
            'dark': px.colors.qualitative.Dark24,
            'light': px.colors.qualitative.Light24,
            'sequential': px.colors.sequential.Viridis,
            'diverging': px.colors.diverging.RdBu
        }
    
    async def create_chart(
        self, 
        data: List[Dict[str, Any]], 
        chart_type: str = "auto",
        title: str = None,
        x_column: str = None,
        y_column: str = None,
        color_column: str = None,
        size_column: str = None,
        group_column: str = None,
        color_scheme: str = "default",
        width: int = 800,
        height: int = 600,
        theme: str = "plotly",
        **kwargs
    ) -> Dict[str, Any]:
        """
        Create an interactive chart from data.
        
        Args:
            data: List of dictionaries containing the data
            chart_type: Type of chart to create ('auto' for automatic detection)
            title: Chart title
            x_column: Column for x-axis
            y_column: Column for y-axis
            color_column: Column for color encoding
            size_column: Column for size encoding
            group_column: Column for grouping/faceting
            color_scheme: Color scheme to use
            width: Chart width in pixels
            height: Chart height in pixels
            theme: Plotly theme
            **kwargs: Additional chart-specific parameters
            
        Returns:
            Dictionary containing chart data and metadata
        """
        try:
            if not data:
                raise ValidationError("data", "empty list", "Data cannot be empty")
            
            # Convert to DataFrame for easier manipulation
            df = pd.DataFrame(data)
            
            if df.empty:
                raise ValidationError("data", "empty dataframe", "Data contains no valid records")
            
            # Analyze data structure
            data_analysis = await self._analyze_data_structure(df)
            
            # Auto-detect chart type if requested
            if chart_type == "auto":
                chart_type = await self._recommend_chart_type(df, data_analysis, x_column, y_column)
            
            # Validate chart type
            try:
                chart_enum = ChartType(chart_type.lower())
            except ValueError:
                valid_types = [t.value for t in ChartType]
                raise ValidationError("chart_type", chart_type, f"Chart type must be one of: {', '.join(valid_types)}")
            
            # Auto-select columns if not specified
            if not x_column or not y_column:
                x_column, y_column = await self._auto_select_columns(df, data_analysis, chart_enum, x_column, y_column)
            
            # Generate chart
            fig = await self._generate_chart(
                df, chart_enum, title, x_column, y_column, color_column, 
                size_column, group_column, color_scheme, theme, **kwargs
            )
            
            # Update layout
            fig.update_layout(
                width=width,
                height=height,
                template=theme,
                title=title or f"{chart_type.title()} Chart",
                showlegend=True
            )
            
            # Convert to JSON for transport
            chart_json = fig.to_json()
            
            return {
                "success": True,
                "chart": json.loads(chart_json),
                "chart_type": chart_type,
                "data_summary": data_analysis,
                "configuration": {
                    "x_column": x_column,
                    "y_column": y_column,
                    "color_column": color_column,
                    "size_column": size_column,
                    "group_column": group_column,
                    "color_scheme": color_scheme,
                    "width": width,
                    "height": height,
                    "theme": theme
                },
                "data_points": len(df),
                "columns_used": [col for col in [x_column, y_column, color_column, size_column, group_column] if col]
            }
            
        except ValidationError as e:
            return {
                "success": False,
                "error": e.to_dict(include_technical=config.debug if config else False)
            }
        except Exception as e:
            return {
                "success": False,
                "error": {
                    "message": "Failed to create chart",
                    "technical_details": str(e) if config and config.debug else None,
                    "suggestions": [
                        "Check data format and types",
                        "Ensure required columns exist",
                        "Try a different chart type"
                    ]
                }
            }
    
    async def recommend_visualizations(
        self, 
        data: List[Dict[str, Any]], 
        limit: int = 3
    ) -> Dict[str, Any]:
        """
        Recommend appropriate visualizations for the given data.
        
        Args:
            data: List of dictionaries containing the data
            limit: Maximum number of recommendations to return
            
        Returns:
            Dictionary containing visualization recommendations
        """
        try:
            if not data:
                return {
                    "success": True,
                    "recommendations": [],
                    "message": "No data provided - cannot make recommendations"
                }
            
            df = pd.DataFrame(data)
            if df.empty:
                return {
                    "success": True,
                    "recommendations": [],
                    "message": "Data is empty - cannot make recommendations"
                }
            
            # Analyze data
            data_analysis = await self._analyze_data_structure(df)
            
            # Generate recommendations
            recommendations = []
            
            numeric_cols = data_analysis["numeric_columns"]
            categorical_cols = data_analysis["categorical_columns"]
            datetime_cols = data_analysis["datetime_columns"]
            
            # Time series recommendations
            if datetime_cols and numeric_cols:
                recommendations.append({
                    "chart_type": "line",
                    "title": f"Time Series: {numeric_cols[0]} over time",
                    "description": "Shows trends and patterns over time",
                    "x_column": datetime_cols[0],
                    "y_column": numeric_cols[0],
                    "suitability_score": 0.9,
                    "use_case": "Trend analysis and time-based patterns"
                })
            
            # Distribution recommendations
            if numeric_cols:
                recommendations.append({
                    "chart_type": "histogram",
                    "title": f"Distribution of {numeric_cols[0]}",
                    "description": "Shows the distribution and frequency of values",
                    "x_column": numeric_cols[0],
                    "suitability_score": 0.8,
                    "use_case": "Understanding data distribution and outliers"
                })
            
            # Categorical analysis
            if categorical_cols:
                if len(categorical_cols) >= 1 and numeric_cols:
                    recommendations.append({
                        "chart_type": "bar",
                        "title": f"{numeric_cols[0]} by {categorical_cols[0]}",
                        "description": "Compares values across different categories",
                        "x_column": categorical_cols[0],
                        "y_column": numeric_cols[0],
                        "suitability_score": 0.85,
                        "use_case": "Categorical comparison and ranking"
                    })
                
                # Pie chart for single categorical with counts
                if len(categorical_cols) >= 1:
                    recommendations.append({
                        "chart_type": "pie",
                        "title": f"Distribution of {categorical_cols[0]}",
                        "description": "Shows proportional breakdown of categories",
                        "x_column": categorical_cols[0],
                        "suitability_score": 0.7,
                        "use_case": "Part-to-whole relationships"
                    })
            
            # Correlation analysis
            if len(numeric_cols) >= 2:
                recommendations.append({
                    "chart_type": "scatter",
                    "title": f"{numeric_cols[0]} vs {numeric_cols[1]}",
                    "description": "Shows relationship between two numeric variables",
                    "x_column": numeric_cols[0],
                    "y_column": numeric_cols[1],
                    "color_column": categorical_cols[0] if categorical_cols else None,
                    "suitability_score": 0.75,
                    "use_case": "Correlation and pattern identification"
                })
            
            # Box plot for numeric vs categorical
            if numeric_cols and categorical_cols:
                recommendations.append({
                    "chart_type": "box",
                    "title": f"{numeric_cols[0]} distribution by {categorical_cols[0]}",
                    "description": "Shows distribution statistics across categories",
                    "x_column": categorical_cols[0],
                    "y_column": numeric_cols[0],
                    "suitability_score": 0.7,
                    "use_case": "Comparing distributions across groups"
                })
            
            # Heatmap for correlation matrix
            if len(numeric_cols) >= 3:
                recommendations.append({
                    "chart_type": "heatmap",
                    "title": "Correlation Matrix",
                    "description": "Shows correlations between numeric variables",
                    "suitability_score": 0.65,
                    "use_case": "Understanding variable relationships"
                })
            
            # Sort by suitability score and limit
            recommendations.sort(key=lambda x: x["suitability_score"], reverse=True)
            recommendations = recommendations[:limit]
            
            return {
                "success": True,
                "recommendations": recommendations,
                "data_analysis": data_analysis,
                "recommendation_count": len(recommendations)
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": {
                    "message": "Failed to generate visualization recommendations",
                    "technical_details": str(e) if config and config.debug else None,
                    "suggestions": [
                        "Check data format",
                        "Ensure data contains valid values",
                        "Try with a smaller dataset"
                    ]
                }
            }
    
    async def create_dashboard(
        self,
        data: List[Dict[str, Any]],
        chart_configs: List[Dict[str, Any]],
        title: str = "Data Dashboard",
        layout: str = "grid",
        width: int = 1200,
        height: int = 800
    ) -> Dict[str, Any]:
        """
        Create a multi-chart dashboard.
        
        Args:
            data: List of dictionaries containing the data
            chart_configs: List of chart configurations
            title: Dashboard title
            layout: Layout type ('grid', 'vertical', 'horizontal')
            width: Dashboard width
            height: Dashboard height
            
        Returns:
            Dictionary containing dashboard data
        """
        try:
            if not data:
                raise ValidationError("data", "empty list", "Data cannot be empty")
            
            if not chart_configs:
                raise ValidationError("chart_configs", "empty list", "Chart configurations cannot be empty")
            
            df = pd.DataFrame(data)
            
            # Determine subplot layout
            n_charts = len(chart_configs)
            if layout == "grid":
                cols = min(2, n_charts)
                rows = (n_charts + cols - 1) // cols
            elif layout == "vertical":
                rows = n_charts
                cols = 1
            else:  # horizontal
                rows = 1
                cols = n_charts
            
            # Create subplots
            fig = make_subplots(
                rows=rows, 
                cols=cols,
                subplot_titles=[config.get("title", f"Chart {i+1}") for i, config in enumerate(chart_configs)]
            )
            
            # Generate individual charts
            for i, chart_config in enumerate(chart_configs):
                row = (i // cols) + 1 if layout == "grid" else i + 1 if layout == "vertical" else 1
                col = (i % cols) + 1 if layout != "vertical" else 1
                
                # Create individual chart
                chart_result = await self.create_chart(data, **chart_config)
                
                if chart_result["success"]:
                    chart_fig = go.Figure(chart_result["chart"])
                    
                    # Add traces to subplot
                    for trace in chart_fig.data:
                        fig.add_trace(trace, row=row, col=col)
            
            # Update layout
            fig.update_layout(
                title=title,
                width=width,
                height=height,
                showlegend=True
            )
            
            return {
                "success": True,
                "dashboard": json.loads(fig.to_json()),
                "layout": layout,
                "chart_count": n_charts,
                "dimensions": {"width": width, "height": height}
            }
            
        except ValidationError as e:
            return {
                "success": False,
                "error": e.to_dict(include_technical=config.debug if config else False)
            }
        except Exception as e:
            return {
                "success": False,
                "error": {
                    "message": "Failed to create dashboard",
                    "technical_details": str(e) if config and config.debug else None
                }
            }
    
    async def _analyze_data_structure(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze DataFrame structure for visualization recommendations."""
        numeric_cols = []
        categorical_cols = []
        datetime_cols = []
        text_cols = []
        boolean_cols = []
        
        for col in df.columns:
            dtype = df[col].dtype
            unique_values = df[col].nunique()
            
            if pd.api.types.is_numeric_dtype(dtype):
                numeric_cols.append(col)
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                datetime_cols.append(col)
            elif pd.api.types.is_bool_dtype(dtype):
                boolean_cols.append(col)
            elif unique_values <= 20:  # Threshold for categorical
                categorical_cols.append(col)
            else:
                text_cols.append(col)
        
        return {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "numeric_columns": numeric_cols,
            "categorical_columns": categorical_cols,
            "datetime_columns": datetime_cols,
            "text_columns": text_cols,
            "boolean_columns": boolean_cols,
            "column_info": {
                col: {
                    "type": str(df[col].dtype),
                    "unique_values": df[col].nunique(),
                    "null_count": df[col].isnull().sum(),
                    "sample_values": df[col].dropna().head(3).tolist()
                } for col in df.columns
            }
        }
    
    async def _recommend_chart_type(
        self, 
        df: pd.DataFrame, 
        analysis: Dict[str, Any], 
        x_col: str = None, 
        y_col: str = None
    ) -> str:
        """Recommend appropriate chart type based on data analysis."""
        numeric_cols = analysis["numeric_columns"]
        categorical_cols = analysis["categorical_columns"]
        datetime_cols = analysis["datetime_columns"]
        
        # Time series
        if datetime_cols and (y_col in numeric_cols or numeric_cols):
            return "line"
        
        # Single numeric column - distribution
        if len(numeric_cols) == 1 and not categorical_cols:
            return "histogram"
        
        # Categorical vs numeric
        if categorical_cols and numeric_cols:
            if len(df) > 50:
                return "box"
            else:
                return "bar"
        
        # Two numeric columns
        if len(numeric_cols) >= 2:
            return "scatter"
        
        # Single categorical
        if len(categorical_cols) == 1 and not numeric_cols:
            return "pie"
        
        # Multiple numeric - correlation
        if len(numeric_cols) >= 3:
            return "heatmap"
        
        # Default
        return "table"
    
    async def _auto_select_columns(
        self, 
        df: pd.DataFrame, 
        analysis: Dict[str, Any], 
        chart_type: ChartType,
        x_col: str = None, 
        y_col: str = None
    ) -> Tuple[str, str]:
        """Auto-select appropriate columns for chart axes."""
        numeric_cols = analysis["numeric_columns"]
        categorical_cols = analysis["categorical_columns"]
        datetime_cols = analysis["datetime_columns"]
        
        if chart_type == ChartType.LINE:
            x_col = x_col or (datetime_cols[0] if datetime_cols else (categorical_cols[0] if categorical_cols else numeric_cols[0] if numeric_cols else df.columns[0]))
            y_col = y_col or (numeric_cols[0] if numeric_cols else df.columns[1] if len(df.columns) > 1 else df.columns[0])
        
        elif chart_type == ChartType.BAR:
            x_col = x_col or (categorical_cols[0] if categorical_cols else df.columns[0])
            y_col = y_col or (numeric_cols[0] if numeric_cols else df.columns[1] if len(df.columns) > 1 else df.columns[0])
        
        elif chart_type == ChartType.SCATTER:
            x_col = x_col or (numeric_cols[0] if numeric_cols else df.columns[0])
            y_col = y_col or (numeric_cols[1] if len(numeric_cols) > 1 else (numeric_cols[0] if numeric_cols else df.columns[1] if len(df.columns) > 1 else df.columns[0]))
        
        elif chart_type == ChartType.HISTOGRAM:
            x_col = x_col or (numeric_cols[0] if numeric_cols else df.columns[0])
            y_col = y_col or None
        
        elif chart_type == ChartType.PIE:
            x_col = x_col or (categorical_cols[0] if categorical_cols else df.columns[0])
            y_col = y_col or (numeric_cols[0] if numeric_cols else None)
        
        else:
            x_col = x_col or df.columns[0]
            y_col = y_col or (df.columns[1] if len(df.columns) > 1 else df.columns[0])
        
        return x_col, y_col
    
    async def _generate_chart(
        self, 
        df: pd.DataFrame, 
        chart_type: ChartType,
        title: str,
        x_col: str, 
        y_col: str, 
        color_col: str = None,
        size_col: str = None, 
        group_col: str = None,
        color_scheme: str = "default", 
        theme: str = "plotly",
        **kwargs
    ) -> go.Figure:
        """Generate the actual chart using Plotly."""
        colors = self.color_schemes.get(color_scheme, self.color_schemes["default"])
        
        if chart_type == ChartType.BAR:
            fig = px.bar(df, x=x_col, y=y_col, color=color_col, title=title, color_discrete_sequence=colors)
        
        elif chart_type == ChartType.LINE:
            fig = px.line(df, x=x_col, y=y_col, color=color_col, title=title, color_discrete_sequence=colors)
        
        elif chart_type == ChartType.SCATTER:
            fig = px.scatter(df, x=x_col, y=y_col, color=color_col, size=size_col, title=title, color_discrete_sequence=colors)
        
        elif chart_type == ChartType.PIE:
            values_col = y_col if y_col else df.columns[1] if len(df.columns) > 1 else None
            if values_col:
                fig = px.pie(df, names=x_col, values=values_col, title=title, color_discrete_sequence=colors)
            else:
                # Count occurrences
                value_counts = df[x_col].value_counts()
                fig = px.pie(values=value_counts.values, names=value_counts.index, title=title, color_discrete_sequence=colors)
        
        elif chart_type == ChartType.HISTOGRAM:
            fig = px.histogram(df, x=x_col, color=color_col, title=title, color_discrete_sequence=colors)
        
        elif chart_type == ChartType.BOX:
            fig = px.box(df, x=x_col, y=y_col, color=color_col, title=title, color_discrete_sequence=colors)
        
        elif chart_type == ChartType.HEATMAP:
            # Create correlation matrix for numeric columns
            numeric_df = df.select_dtypes(include=['number'])
            corr_matrix = numeric_df.corr()
            fig = px.imshow(corr_matrix, title=title, color_continuous_scale=colors if isinstance(colors, str) else 'RdBu')
        
        elif chart_type == ChartType.AREA:
            fig = px.area(df, x=x_col, y=y_col, color=color_col, title=title, color_discrete_sequence=colors)
        
        else:  # TABLE or unknown
            fig = go.Figure(data=[go.Table(
                header=dict(values=list(df.columns)),
                cells=dict(values=[df[col].tolist() for col in df.columns])
            )])
            fig.update_layout(title=title)
        
        return fig


# Global chart generator instance
chart_generator = ChartGenerator()
