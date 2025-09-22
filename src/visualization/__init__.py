"""
Data visualization module for the Natural Language SQL MCP Server.

This module provides chart generation and dashboard capabilities using Plotly.
"""

from .chart_generator import ChartGenerator, ChartType, chart_generator

__all__ = [
    'ChartGenerator',
    'ChartType', 
    'chart_generator'
]
