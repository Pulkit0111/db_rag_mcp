"""
Chart generation service for the Natural Language SQL MCP Server.

This module provides intelligent data visualization capabilities using Plotly,
with automatic chart type detection, customization options, and export functionality.
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import plotly.offline as pyo
from typing import List, Dict, Any, Optional, Tuple
from enum import Enum
import numpy as np
from datetime import datetime
import logging

from ..core.exceptions import ValidationError
from ..core.config import config


logger = logging.getLogger(__name__)


class ChartType(Enum):
    """Enumeration of supported chart types."""
    BAR = "bar"
    LINE = "line"
    PIE = "pie"
    SCATTER = "scatter"
    HISTOGRAM = "histogram"
    BOX = "box"
    HEATMAP = "heatmap"
    AREA = "area"
    TREEMAP = "treemap"
    SUNBURST = "sunburst"
    AUTO = "auto"


class DataType(Enum):
    """Enumeration of data types for analysis."""
    NUMERIC = "numeric"
    CATEGORICAL = "categorical"
    DATETIME = "datetime"
    TEXT = "text"
    BOOLEAN = "boolean"


class ChartGenerator:
    """
    Generates interactive charts and visualizations from query results.
    
    Provides functionality for:
    - Automatic chart type detection based on data characteristics
    - Interactive Plotly chart generation
    - Chart customization and theming
    - Export to various formats (HTML, PNG, PDF, etc.)
    """
    
    def __init__(self):
        """Initialize the chart generator."""
        self.color_schemes = {
            "default": px.colors.qualitative.Set3,
            "professional": px.colors.qualitative.Dark2,
            "vibrant": px.colors.qualitative.Vivid,
            "pastel": px.colors.qualitative.Pastel,
            "dark": px.colors.qualitative.G10
        }
        
        self.chart_themes = {
            "default": "plotly",
            "white": "plotly_white",
            "dark": "plotly_dark",
            "minimal": "simple_white"
        }
    
    def _analyze_data_types(self, df: pd.DataFrame) -> Dict[str, DataType]:
        """
        Analyze DataFrame columns to determine data types.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary mapping column names to data types
        """
        column_types = {}
        
        for column in df.columns:
            series = df[column]
            
            # Check for datetime
            if pd.api.types.is_datetime64_any_dtype(series):
                column_types[column] = DataType.DATETIME
            elif series.dtype.name.startswith('datetime'):
                column_types[column] = DataType.DATETIME
            
            # Check for numeric
            elif pd.api.types.is_numeric_dtype(series):
                column_types[column] = DataType.NUMERIC
            
            # Check for boolean
            elif pd.api.types.is_bool_dtype(series):
                column_types[column] = DataType.BOOLEAN
            
            # Check if categorical (small number of unique values)
            elif series.nunique() <= min(20, len(df) * 0.1):
                column_types[column] = DataType.CATEGORICAL
            
            # Default to text
            else:
                column_types[column] = DataType.TEXT
        
        return column_types
    
    def _detect_chart_type(self, df: pd.DataFrame, column_types: Dict[str, DataType]) -> ChartType:
        """
        Automatically detect the most appropriate chart type.
        
        Args:
            df: DataFrame to analyze
            column_types: Column data types
            
        Returns:
            Recommended chart type
        """
        num_columns = len(df.columns)
        num_rows = len(df)
        
        numeric_cols = [col for col, dtype in column_types.items() if dtype == DataType.NUMERIC]
        categorical_cols = [col for col, dtype in column_types.items() if dtype == DataType.CATEGORICAL]
        datetime_cols = [col for col, dtype in column_types.items() if dtype == DataType.DATETIME]
        
        # Single column analysis
        if num_columns == 1:
            col_type = list(column_types.values())[0]
            if col_type == DataType.NUMERIC:
                return ChartType.HISTOGRAM
            elif col_type == DataType.CATEGORICAL:
                return ChartType.PIE
            else:
                return ChartType.BAR
        
        # Two columns analysis
        if num_columns == 2:
            if len(numeric_cols) == 2:
                return ChartType.SCATTER
            elif len(numeric_cols) == 1 and len(categorical_cols) == 1:
                return ChartType.BAR
            elif len(datetime_cols) == 1 and len(numeric_cols) == 1:
                return ChartType.LINE
            elif len(categorical_cols) == 2:
                return ChartType.HEATMAP
        
        # Multiple columns analysis
        if num_columns >= 3:
            if len(numeric_cols) >= 2:
                if len(datetime_cols) >= 1:
                    return ChartType.LINE
                else:
                    return ChartType.SCATTER
            elif len(categorical_cols) >= 2:
                return ChartType.TREEMAP
        
        # Default fallback
        if len(numeric_cols) > 0 and len(categorical_cols) > 0:
            return ChartType.BAR
        elif len(categorical_cols) > 0:
            return ChartType.PIE
        else:
            return ChartType.SCATTER
    
    def _prepare_data_for_chart(
        self, 
        df: pd.DataFrame, 
        chart_type: ChartType,
        column_types: Dict[str, DataType]
    ) -> Tuple[pd.DataFrame, Dict[str, str]]:
        """
        Prepare and clean data for charting.
        
        Args:
            df: Original DataFrame
            chart_type: Target chart type
            column_types: Column data types
            
        Returns:
            Tuple of (prepared_dataframe, column_mapping)
        """
        prepared_df = df.copy()
        column_mapping = {}
        
        # Handle missing values
        for col in prepared_df.columns:
            if column_types[col] == DataType.NUMERIC:
                prepared_df[col] = prepared_df[col].fillna(0)
            else:
                prepared_df[col] = prepared_df[col].fillna('Unknown')
        
        # Limit data points for performance
        max_points = config.max_result_rows if config else 1000
        if len(prepared_df) > max_points:
            if chart_type in [ChartType.LINE, ChartType.AREA]:
                # For time series, sample evenly
                step = len(prepared_df) // max_points
                prepared_df = prepared_df.iloc[::step]
            else:
                # For other charts, take top N
                prepared_df = prepared_df.head(max_points)
        
        # Create user-friendly column names
        for col in prepared_df.columns:
            clean_name = col.replace('_', ' ').title()
            column_mapping[col] = clean_name
        
        return prepared_df, column_mapping
    
    async def generate_chart(
        self,
        data: List[Dict[str, Any]],
        chart_type: str = "auto",
        title: str = None,
        theme: str = "default",
        color_scheme: str = "default",
        width: int = 800,
        height: int = 600,
        show_values: bool = False
    ) -> str:
        """
        Generate an interactive chart from data.
        
        Args:
            data: List of dictionaries containing the data
            chart_type: Type of chart to generate
            title: Chart title (auto-generated if None)
            theme: Visual theme for the chart
            color_scheme: Color scheme to use
            width: Chart width in pixels
            height: Chart height in pixels
            show_values: Whether to show data values on the chart
            
        Returns:
            HTML string containing the interactive chart
        """
        try:
            # Validate inputs
            if not data:
                return self._create_empty_chart_html("No data available to visualize")
            
            if chart_type not in [ct.value for ct in ChartType]:
                chart_type = "auto"
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            if df.empty:
                return self._create_empty_chart_html("Data is empty")
            
            # Analyze data types
            column_types = self._analyze_data_types(df)
            
            # Determine chart type
            if chart_type == "auto":
                detected_type = self._detect_chart_type(df, column_types)
            else:
                detected_type = ChartType(chart_type)
            
            # Prepare data
            prepared_df, column_mapping = self._prepare_data_for_chart(df, detected_type, column_types)
            
            # Generate chart
            fig = await self._create_chart(prepared_df, detected_type, column_types, column_mapping)
            
            # Apply styling
            fig = self._apply_styling(
                fig, 
                title=title or f"Data Visualization ({detected_type.value.title()})",
                theme=theme,
                color_scheme=color_scheme,
                width=width,
                height=height,
                show_values=show_values
            )
            
            # Convert to HTML
            html_output = fig.to_html(
                include_plotlyjs=True,
                div_id=f"chart_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                config={'displayModeBar': True, 'responsive': True}
            )
            
            logger.info(f"Generated {detected_type.value} chart with {len(prepared_df)} data points")
            
            return html_output
            
        except Exception as e:
            logger.error(f"Chart generation failed: {str(e)}")
            return self._create_error_chart_html(f"Error generating chart: {str(e)}")
    
    async def _create_chart(
        self,
        df: pd.DataFrame,
        chart_type: ChartType,
        column_types: Dict[str, DataType],
        column_mapping: Dict[str, str]
    ) -> go.Figure:
        """Create the specific chart based on type and data."""
        
        if chart_type == ChartType.BAR:
            return self._create_bar_chart(df, column_types, column_mapping)
        elif chart_type == ChartType.LINE:
            return self._create_line_chart(df, column_types, column_mapping)
        elif chart_type == ChartType.PIE:
            return self._create_pie_chart(df, column_types, column_mapping)
        elif chart_type == ChartType.SCATTER:
            return self._create_scatter_chart(df, column_types, column_mapping)
        elif chart_type == ChartType.HISTOGRAM:
            return self._create_histogram(df, column_types, column_mapping)
        elif chart_type == ChartType.BOX:
            return self._create_box_plot(df, column_types, column_mapping)
        elif chart_type == ChartType.HEATMAP:
            return self._create_heatmap(df, column_types, column_mapping)
        elif chart_type == ChartType.AREA:
            return self._create_area_chart(df, column_types, column_mapping)
        elif chart_type == ChartType.TREEMAP:
            return self._create_treemap(df, column_types, column_mapping)
        else:
            # Fallback to bar chart
            return self._create_bar_chart(df, column_types, column_mapping)
    
    def _create_bar_chart(self, df: pd.DataFrame, column_types: Dict[str, DataType], column_mapping: Dict[str, str]) -> go.Figure:
        """Create a bar chart."""
        # Find x and y columns
        categorical_cols = [col for col, dtype in column_types.items() if dtype == DataType.CATEGORICAL]
        numeric_cols = [col for col, dtype in column_types.items() if dtype == DataType.NUMERIC]
        
        if len(df.columns) >= 2:
            if categorical_cols and numeric_cols:
                x_col = categorical_cols[0]
                y_col = numeric_cols[0]
            else:
                x_col = df.columns[0]
                y_col = df.columns[1]
            
            fig = px.bar(
                df,
                x=x_col,
                y=y_col,
                labels={x_col: column_mapping[x_col], y_col: column_mapping[y_col]}
            )
        else:
            # Single column - count occurrences
            col = df.columns[0]
            value_counts = df[col].value_counts().reset_index()
            value_counts.columns = [col, 'count']
            
            fig = px.bar(
                value_counts,
                x=col,
                y='count',
                labels={col: column_mapping[col], 'count': 'Count'}
            )
        
        return fig
    
    def _create_line_chart(self, df: pd.DataFrame, column_types: Dict[str, DataType], column_mapping: Dict[str, str]) -> go.Figure:
        """Create a line chart."""
        datetime_cols = [col for col, dtype in column_types.items() if dtype == DataType.DATETIME]
        numeric_cols = [col for col, dtype in column_types.items() if dtype == DataType.NUMERIC]
        
        if datetime_cols and numeric_cols:
            x_col = datetime_cols[0]
            y_col = numeric_cols[0]
        elif len(df.columns) >= 2:
            x_col = df.columns[0]
            y_col = df.columns[1]
        else:
            # Single numeric column with index
            y_col = df.columns[0]
            fig = px.line(
                df.reset_index(),
                x='index',
                y=y_col,
                labels={'index': 'Index', y_col: column_mapping[y_col]}
            )
            return fig
        
        fig = px.line(
            df,
            x=x_col,
            y=y_col,
            labels={x_col: column_mapping[x_col], y_col: column_mapping[y_col]}
        )
        
        return fig
    
    def _create_pie_chart(self, df: pd.DataFrame, column_types: Dict[str, DataType], column_mapping: Dict[str, str]) -> go.Figure:
        """Create a pie chart."""
        if len(df.columns) >= 2:
            # Use first categorical/text column for names, second numeric for values
            categorical_cols = [col for col, dtype in column_types.items() if dtype in [DataType.CATEGORICAL, DataType.TEXT]]
            numeric_cols = [col for col, dtype in column_types.items() if dtype == DataType.NUMERIC]
            
            if categorical_cols and numeric_cols:
                names_col = categorical_cols[0]
                values_col = numeric_cols[0]
            else:
                names_col = df.columns[0]
                values_col = df.columns[1]
            
            fig = px.pie(
                df,
                names=names_col,
                values=values_col,
                labels={names_col: column_mapping[names_col], values_col: column_mapping[values_col]}
            )
        else:
            # Single column - count occurrences
            col = df.columns[0]
            value_counts = df[col].value_counts()
            
            fig = px.pie(
                values=value_counts.values,
                names=value_counts.index,
                labels={'names': column_mapping[col], 'values': 'Count'}
            )
        
        return fig
    
    def _create_scatter_chart(self, df: pd.DataFrame, column_types: Dict[str, DataType], column_mapping: Dict[str, str]) -> go.Figure:
        """Create a scatter plot."""
        numeric_cols = [col for col, dtype in column_types.items() if dtype == DataType.NUMERIC]
        
        if len(numeric_cols) >= 2:
            x_col = numeric_cols[0]
            y_col = numeric_cols[1]
            
            # Use third numeric column for size if available
            size_col = numeric_cols[2] if len(numeric_cols) > 2 else None
            
            fig = px.scatter(
                df,
                x=x_col,
                y=y_col,
                size=size_col,
                labels={
                    x_col: column_mapping[x_col],
                    y_col: column_mapping[y_col]
                }
            )
        else:
            # Fallback to first two columns
            x_col = df.columns[0]
            y_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
            
            fig = px.scatter(
                df,
                x=x_col,
                y=y_col,
                labels={x_col: column_mapping[x_col], y_col: column_mapping[y_col]}
            )
        
        return fig
    
    def _create_histogram(self, df: pd.DataFrame, column_types: Dict[str, DataType], column_mapping: Dict[str, str]) -> go.Figure:
        """Create a histogram."""
        numeric_cols = [col for col, dtype in column_types.items() if dtype == DataType.NUMERIC]
        
        if numeric_cols:
            col = numeric_cols[0]
        else:
            col = df.columns[0]
        
        fig = px.histogram(
            df,
            x=col,
            labels={col: column_mapping[col]}
        )
        
        return fig
    
    def _create_box_plot(self, df: pd.DataFrame, column_types: Dict[str, DataType], column_mapping: Dict[str, str]) -> go.Figure:
        """Create a box plot."""
        numeric_cols = [col for col, dtype in column_types.items() if dtype == DataType.NUMERIC]
        categorical_cols = [col for col, dtype in column_types.items() if dtype == DataType.CATEGORICAL]
        
        if numeric_cols and categorical_cols:
            fig = px.box(
                df,
                x=categorical_cols[0],
                y=numeric_cols[0],
                labels={
                    categorical_cols[0]: column_mapping[categorical_cols[0]],
                    numeric_cols[0]: column_mapping[numeric_cols[0]]
                }
            )
        elif numeric_cols:
            fig = px.box(
                df,
                y=numeric_cols[0],
                labels={numeric_cols[0]: column_mapping[numeric_cols[0]]}
            )
        else:
            # Fallback
            fig = px.box(df, y=df.columns[0])
        
        return fig
    
    def _create_heatmap(self, df: pd.DataFrame, column_types: Dict[str, DataType], column_mapping: Dict[str, str]) -> go.Figure:
        """Create a heatmap."""
        numeric_cols = [col for col, dtype in column_types.items() if dtype == DataType.NUMERIC]
        
        if len(numeric_cols) >= 2:
            # Correlation heatmap
            correlation_matrix = df[numeric_cols].corr()
            fig = px.imshow(
                correlation_matrix,
                labels=dict(color="Correlation"),
                title="Correlation Heatmap"
            )
        else:
            # Pivot table heatmap if possible
            if len(df.columns) >= 3:
                pivot_df = df.pivot_table(
                    index=df.columns[0],
                    columns=df.columns[1],
                    values=df.columns[2],
                    aggfunc='mean'
                ).fillna(0)
                
                fig = px.imshow(pivot_df)
            else:
                # Simple value heatmap
                fig = px.imshow([df.iloc[:10].values])
        
        return fig
    
    def _create_area_chart(self, df: pd.DataFrame, column_types: Dict[str, DataType], column_mapping: Dict[str, str]) -> go.Figure:
        """Create an area chart."""
        datetime_cols = [col for col, dtype in column_types.items() if dtype == DataType.DATETIME]
        numeric_cols = [col for col, dtype in column_types.items() if dtype == DataType.NUMERIC]
        
        if datetime_cols and numeric_cols:
            fig = px.area(
                df,
                x=datetime_cols[0],
                y=numeric_cols[0],
                labels={
                    datetime_cols[0]: column_mapping[datetime_cols[0]],
                    numeric_cols[0]: column_mapping[numeric_cols[0]]
                }
            )
        else:
            # Fallback to line chart style
            x_col = df.columns[0]
            y_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
            
            fig = px.area(
                df,
                x=x_col,
                y=y_col,
                labels={x_col: column_mapping[x_col], y_col: column_mapping[y_col]}
            )
        
        return fig
    
    def _create_treemap(self, df: pd.DataFrame, column_types: Dict[str, DataType], column_mapping: Dict[str, str]) -> go.Figure:
        """Create a treemap."""
        categorical_cols = [col for col, dtype in column_types.items() if dtype in [DataType.CATEGORICAL, DataType.TEXT]]
        numeric_cols = [col for col, dtype in column_types.items() if dtype == DataType.NUMERIC]
        
        if len(categorical_cols) >= 1 and len(numeric_cols) >= 1:
            fig = px.treemap(
                df,
                path=[categorical_cols[0]] + (categorical_cols[1:2] if len(categorical_cols) > 1 else []),
                values=numeric_cols[0],
                labels={
                    categorical_cols[0]: column_mapping[categorical_cols[0]],
                    numeric_cols[0]: column_mapping[numeric_cols[0]]
                }
            )
        else:
            # Fallback - create treemap from value counts
            col = df.columns[0]
            value_counts = df[col].value_counts().reset_index()
            value_counts.columns = [col, 'count']
            
            fig = px.treemap(
                value_counts,
                path=[col],
                values='count',
                labels={col: column_mapping[col]}
            )
        
        return fig
    
    def _apply_styling(
        self,
        fig: go.Figure,
        title: str,
        theme: str,
        color_scheme: str,
        width: int,
        height: int,
        show_values: bool
    ) -> go.Figure:
        """Apply styling and theme to the chart."""
        # Update layout
        fig.update_layout(
            title=title,
            width=width,
            height=height,
            template=self.chart_themes.get(theme, "plotly"),
            showlegend=True,
            hovermode='closest'
        )
        
        # Apply color scheme
        if color_scheme in self.color_schemes:
            fig.update_traces(
                marker_color=self.color_schemes[color_scheme][:len(fig.data)]
            )
        
        # Show values if requested
        if show_values:
            fig.update_traces(texttemplate='%{y}', textposition='outside')
        
        return fig
    
    def _create_empty_chart_html(self, message: str) -> str:
        """Create HTML for empty chart."""
        return f"""
        <div style="text-align: center; padding: 50px; font-family: Arial, sans-serif;">
            <h3>No Data to Display</h3>
            <p>{message}</p>
        </div>
        """
    
    def _create_error_chart_html(self, error_message: str) -> str:
        """Create HTML for error state."""
        return f"""
        <div style="text-align: center; padding: 50px; font-family: Arial, sans-serif; color: red;">
            <h3>Chart Generation Error</h3>
            <p>{error_message}</p>
        </div>
        """
