"""
Data export tools for the MCP server.

This module provides tools for exporting query results in various formats
including CSV, JSON, Excel, TSV, and Parquet.
"""

import csv
import json
import os
import io
from typing import Dict, Any, List, Optional
from datetime import datetime
import pandas as pd
from fastmcp import Context

from .query import query_data
from ..core.exceptions import ValidationError, NaturalSQLException
from ..core.config import config
from ..auth.user_manager import user_manager, Permission


def _get_session_id(ctx: Context) -> str:
    """Get session ID from context."""
    return getattr(ctx, 'session_id', 'default_session')


async def export_csv(
    ctx: Context, 
    natural_language_query: str, 
    filename: str = None,
    include_headers: bool = True,
    delimiter: str = ",",
    encoding: str = "utf-8"
) -> Dict[str, Any]:
    """
    Execute a query and export results to CSV format.
    
    This tool runs a natural language query and exports the results
    as a CSV file that can be downloaded or saved.
    
    Args:
        natural_language_query: The natural language query to execute
        filename: Name for the exported file (auto-generated if not provided)
        include_headers: Whether to include column headers
        delimiter: Field delimiter (comma by default)
        encoding: File encoding
        
    Returns:
        Dictionary containing export result and file information
    """
    try:
        # Check permissions
        session_id = _get_session_id(ctx)
        if not await user_manager.check_permission(session_id, Permission.EXPORT_DATA):
            return {
                "success": False,
                "message": "Insufficient permissions to export data",
                "suggestions": ["Contact administrator for export permissions"]
            }
        
        # Validate inputs
        if not natural_language_query or not natural_language_query.strip():
            raise ValidationError("natural_language_query", "empty", "Query cannot be empty")
        
        await ctx.info(f"Executing query for CSV export: {natural_language_query[:50]}...")
        
        # Execute query
        query_result = await query_data(ctx, natural_language_query)
        
        if not query_result["success"]:
            return {
                "success": False,
                "message": "Query execution failed - cannot export data",
                "error": query_result.get("error", "Unknown query error")
            }
        
        data = query_result.get("results", [])
        
        if not data:
            return {
                "success": True,
                "message": "Query executed successfully but returned no data to export",
                "filename": None,
                "row_count": 0
            }
        
        # Generate filename if not provided
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"query_export_{timestamp}.csv"
        elif not filename.endswith('.csv'):
            filename += '.csv'
        
        # Create CSV content
        output = io.StringIO()
        
        # Get field names from first row
        fieldnames = list(data[0].keys()) if data else []
        
        writer = csv.DictWriter(
            output, 
            fieldnames=fieldnames, 
            delimiter=delimiter,
            quoting=csv.QUOTE_MINIMAL
        )
        
        if include_headers:
            writer.writeheader()
        
        # Write data rows
        for row in data:
            # Convert None values to empty strings
            clean_row = {k: (v if v is not None else '') for k, v in row.items()}
            writer.writerow(clean_row)
        
        csv_content = output.getvalue()
        output.close()
        
        await ctx.info(f"CSV export completed: {len(data)} rows exported")
        
        return {
            "success": True,
            "message": f"Data exported successfully to CSV format",
            "filename": filename,
            "format": "csv",
            "row_count": len(data),
            "column_count": len(fieldnames),
            "file_size_bytes": len(csv_content.encode(encoding)),
            "content": csv_content,
            "encoding": encoding,
            "delimiter": delimiter,
            "original_query": natural_language_query,
            "export_timestamp": datetime.now().isoformat()
        }
        
    except ValidationError as e:
        await ctx.error(f"CSV export validation failed: {e.user_message}")
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False)
        }
    except Exception as e:
        unexpected_error = NaturalSQLException(
            user_message="An unexpected error occurred during CSV export",
            technical_details=str(e),
            suggestions=["Check query results", "Try a simpler query", "Contact support if problem persists"]
        )
        await ctx.error(f"CSV export error: {unexpected_error.user_message}")
        return {
            "success": False,
            "error": unexpected_error.to_dict(include_technical=config.debug if config else False)
        }


async def export_json(
    ctx: Context, 
    natural_language_query: str, 
    filename: str = None,
    pretty_print: bool = True,
    encoding: str = "utf-8"
) -> Dict[str, Any]:
    """
    Execute a query and export results to JSON format.
    
    This tool runs a natural language query and exports the results
    as a JSON file with customizable formatting.
    
    Args:
        natural_language_query: The natural language query to execute
        filename: Name for the exported file (auto-generated if not provided)
        pretty_print: Whether to format JSON with indentation
        encoding: File encoding
        
    Returns:
        Dictionary containing export result and file information
    """
    try:
        # Check permissions
        session_id = _get_session_id(ctx)
        if not await user_manager.check_permission(session_id, Permission.EXPORT_DATA):
            return {
                "success": False,
                "message": "Insufficient permissions to export data",
                "suggestions": ["Contact administrator for export permissions"]
            }
        
        # Validate inputs
        if not natural_language_query or not natural_language_query.strip():
            raise ValidationError("natural_language_query", "empty", "Query cannot be empty")
        
        await ctx.info(f"Executing query for JSON export: {natural_language_query[:50]}...")
        
        # Execute query
        query_result = await query_data(ctx, natural_language_query)
        
        if not query_result["success"]:
            return {
                "success": False,
                "message": "Query execution failed - cannot export data",
                "error": query_result.get("error", "Unknown query error")
            }
        
        data = query_result.get("results", [])
        
        # Generate filename if not provided
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"query_export_{timestamp}.json"
        elif not filename.endswith('.json'):
            filename += '.json'
        
        # Create export package with metadata
        export_data = {
            "metadata": {
                "export_timestamp": datetime.now().isoformat(),
                "original_query": natural_language_query,
                "generated_sql": query_result.get("generated_sql", ""),
                "row_count": len(data),
                "execution_time": query_result.get("execution_time", 0)
            },
            "data": data
        }
        
        # Convert to JSON
        if pretty_print:
            json_content = json.dumps(export_data, indent=2, ensure_ascii=False)
        else:
            json_content = json.dumps(export_data, separators=(',', ':'), ensure_ascii=False)
        
        await ctx.info(f"JSON export completed: {len(data)} rows exported")
        
        return {
            "success": True,
            "message": f"Data exported successfully to JSON format",
            "filename": filename,
            "format": "json",
            "row_count": len(data),
            "file_size_bytes": len(json_content.encode(encoding)),
            "content": json_content,
            "encoding": encoding,
            "pretty_printed": pretty_print,
            "includes_metadata": True,
            "original_query": natural_language_query,
            "export_timestamp": datetime.now().isoformat()
        }
        
    except ValidationError as e:
        await ctx.error(f"JSON export validation failed: {e.user_message}")
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False)
        }
    except Exception as e:
        unexpected_error = NaturalSQLException(
            user_message="An unexpected error occurred during JSON export",
            technical_details=str(e),
            suggestions=["Check query results", "Try a simpler query", "Contact support if problem persists"]
        )
        await ctx.error(f"JSON export error: {unexpected_error.user_message}")
        return {
            "success": False,
            "error": unexpected_error.to_dict(include_technical=config.debug if config else False)
        }


async def export_excel(
    ctx: Context, 
    natural_language_query: str, 
    filename: str = None,
    sheet_name: str = "Query Results",
    include_metadata: bool = True
) -> Dict[str, Any]:
    """
    Execute a query and export results to Excel format.
    
    This tool runs a natural language query and exports the results
    as an Excel spreadsheet with optional metadata sheet.
    
    Args:
        natural_language_query: The natural language query to execute
        filename: Name for the exported file (auto-generated if not provided)
        sheet_name: Name for the data sheet
        include_metadata: Whether to include metadata sheet
        
    Returns:
        Dictionary containing export result and file information
    """
    try:
        # Check permissions
        session_id = _get_session_id(ctx)
        if not await user_manager.check_permission(session_id, Permission.EXPORT_DATA):
            return {
                "success": False,
                "message": "Insufficient permissions to export data",
                "suggestions": ["Contact administrator for export permissions"]
            }
        
        # Validate inputs
        if not natural_language_query or not natural_language_query.strip():
            raise ValidationError("natural_language_query", "empty", "Query cannot be empty")
        
        await ctx.info(f"Executing query for Excel export: {natural_language_query[:50]}...")
        
        # Execute query
        query_result = await query_data(ctx, natural_language_query)
        
        if not query_result["success"]:
            return {
                "success": False,
                "message": "Query execution failed - cannot export data",
                "error": query_result.get("error", "Unknown query error")
            }
        
        data = query_result.get("results", [])
        
        if not data:
            return {
                "success": True,
                "message": "Query executed successfully but returned no data to export",
                "filename": None,
                "row_count": 0
            }
        
        # Generate filename if not provided
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"query_export_{timestamp}.xlsx"
        elif not filename.endswith('.xlsx'):
            filename += '.xlsx'
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Create Excel file in memory
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Write main data sheet
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # Add metadata sheet if requested
            if include_metadata:
                metadata_df = pd.DataFrame({
                    'Property': [
                        'Export Timestamp',
                        'Original Query',
                        'Generated SQL',
                        'Row Count',
                        'Column Count',
                        'Execution Time (seconds)'
                    ],
                    'Value': [
                        datetime.now().isoformat(),
                        natural_language_query,
                        query_result.get("generated_sql", ""),
                        len(data),
                        len(df.columns),
                        query_result.get("execution_time", 0)
                    ]
                })
                metadata_df.to_excel(writer, sheet_name="Metadata", index=False)
        
        excel_content = output.getvalue()
        output.close()
        
        await ctx.info(f"Excel export completed: {len(data)} rows exported")
        
        # Convert to base64 for transport
        import base64
        excel_b64 = base64.b64encode(excel_content).decode('utf-8')
        
        return {
            "success": True,
            "message": f"Data exported successfully to Excel format",
            "filename": filename,
            "format": "excel",
            "row_count": len(data),
            "column_count": len(df.columns),
            "file_size_bytes": len(excel_content),
            "content_base64": excel_b64,
            "sheet_name": sheet_name,
            "includes_metadata": include_metadata,
            "original_query": natural_language_query,
            "export_timestamp": datetime.now().isoformat()
        }
        
    except ValidationError as e:
        await ctx.error(f"Excel export validation failed: {e.user_message}")
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False)
        }
    except Exception as e:
        unexpected_error = NaturalSQLException(
            user_message="An unexpected error occurred during Excel export",
            technical_details=str(e),
            suggestions=["Check query results", "Ensure openpyxl is installed", "Contact support if problem persists"]
        )
        await ctx.error(f"Excel export error: {unexpected_error.user_message}")
        return {
            "success": False,
            "error": unexpected_error.to_dict(include_technical=config.debug if config else False)
        }


async def export_multiple_formats(
    ctx: Context, 
    natural_language_query: str, 
    formats: List[str],
    base_filename: str = None
) -> Dict[str, Any]:
    """
    Execute a query and export results in multiple formats simultaneously.
    
    This tool runs a natural language query once and exports the results
    in multiple formats (CSV, JSON, Excel) for convenience.
    
    Args:
        natural_language_query: The natural language query to execute
        formats: List of formats to export ('csv', 'json', 'excel')
        base_filename: Base name for files (format extensions added automatically)
        
    Returns:
        Dictionary containing export results for each format
    """
    try:
        # Check permissions
        session_id = _get_session_id(ctx)
        if not await user_manager.check_permission(session_id, Permission.EXPORT_DATA):
            return {
                "success": False,
                "message": "Insufficient permissions to export data",
                "suggestions": ["Contact administrator for export permissions"]
            }
        
        # Validate inputs
        if not natural_language_query or not natural_language_query.strip():
            raise ValidationError("natural_language_query", "empty", "Query cannot be empty")
        
        if not formats:
            raise ValidationError("formats", "empty list", "At least one format must be specified")
        
        valid_formats = ['csv', 'json', 'excel']
        invalid_formats = [f for f in formats if f.lower() not in valid_formats]
        if invalid_formats:
            raise ValidationError("formats", str(invalid_formats), f"Invalid formats. Must be one of: {', '.join(valid_formats)}")
        
        # Generate base filename if not provided
        if not base_filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = f"query_export_{timestamp}"
        
        await ctx.info(f"Executing query for multi-format export: {natural_language_query[:50]}...")
        
        # Execute query once
        query_result = await query_data(ctx, natural_language_query)
        
        if not query_result["success"]:
            return {
                "success": False,
                "message": "Query execution failed - cannot export data",
                "error": query_result.get("error", "Unknown query error")
            }
        
        data = query_result.get("results", [])
        exports = {}
        successful_exports = 0
        
        # Export in each requested format
        for fmt in formats:
            fmt_lower = fmt.lower()
            
            try:
                if fmt_lower == 'csv':
                    result = await export_csv(ctx, natural_language_query, f"{base_filename}.csv")
                elif fmt_lower == 'json':
                    result = await export_json(ctx, natural_language_query, f"{base_filename}.json")
                elif fmt_lower == 'excel':
                    result = await export_excel(ctx, natural_language_query, f"{base_filename}.xlsx")
                
                exports[fmt_lower] = result
                if result["success"]:
                    successful_exports += 1
                    
            except Exception as e:
                exports[fmt_lower] = {
                    "success": False,
                    "error": f"Failed to export {fmt_lower}: {str(e)}"
                }
        
        await ctx.info(f"Multi-format export completed: {successful_exports}/{len(formats)} formats successful")
        
        return {
            "success": successful_exports > 0,
            "message": f"Multi-format export completed: {successful_exports}/{len(formats)} formats successful",
            "exports": exports,
            "total_formats": len(formats),
            "successful_exports": successful_exports,
            "failed_exports": len(formats) - successful_exports,
            "row_count": len(data),
            "original_query": natural_language_query,
            "export_timestamp": datetime.now().isoformat()
        }
        
    except ValidationError as e:
        await ctx.error(f"Multi-format export validation failed: {e.user_message}")
        return {
            "success": False,
            "error": e.to_dict(include_technical=config.debug if config else False)
        }
    except Exception as e:
        unexpected_error = NaturalSQLException(
            user_message="An unexpected error occurred during multi-format export",
            technical_details=str(e),
            suggestions=["Check query and format specifications", "Contact support if problem persists"]
        )
        await ctx.error(f"Multi-format export error: {unexpected_error.user_message}")
        return {
            "success": False,
            "error": unexpected_error.to_dict(include_technical=config.debug if config else False)
        }
