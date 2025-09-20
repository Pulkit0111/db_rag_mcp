"""
Natural Language to SQL Translator Service.

This module provides functionality to translate natural language queries
into SQL statements using Large Language Models.
"""

import logging
import re
from typing import Dict, Any, List, Optional
from openai import AsyncOpenAI

from ..core.config import config
from ..database.base_manager import TableSchema


logger = logging.getLogger(__name__)


class SQLTranslator:
    """Translates natural language to SQL using LLMs."""
    
    def __init__(self):
        """Initialize the SQL translator with OpenAI client."""
        if not config.llm.api_key:
            raise ValueError("LLM API key not configured")
        
        self.client = AsyncOpenAI(
            api_key=config.llm.api_key,
            base_url=config.llm.base_url
        )
        self.model = config.llm.model
    
    async def translate_to_select(
        self, 
        natural_query: str,
        tables_schema: List[TableSchema],
        database_type: str = "postgresql"
    ) -> Dict[str, Any]:
        """
        Translate a natural language query to a SELECT SQL statement.
        
        Args:
            natural_query: The natural language query
            tables_schema: List of table schemas for context
            database_type: Type of database (postgresql, mysql, etc.)
            
        Returns:
            Dictionary with SQL query and metadata
        """
        try:
            # Create system prompt with schema information
            system_prompt = self._create_system_prompt(tables_schema, database_type, "SELECT")
            
            # Create user prompt
            user_prompt = f"""
Please convert this natural language query to SQL:

"{natural_query}"

Return ONLY the SQL query, without any explanation or additional text.
"""
            
            # Call LLM
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1,  # Low temperature for consistency
                max_tokens=500
            )
            
            sql_query = response.choices[0].message.content.strip()
            
            # Clean up the SQL query
            sql_query = self._clean_sql_query(sql_query)
            
            # Validate that it's a SELECT query
            if not self._is_select_query(sql_query):
                raise ValueError("Generated query is not a valid SELECT statement")
            
            logger.info(f"Successfully translated natural language to SQL: {sql_query}")
            
            return {
                "success": True,
                "sql_query": sql_query,
                "query_type": "SELECT",
                "original_query": natural_query,
                "model_used": self.model
            }
            
        except Exception as e:
            logger.error(f"Failed to translate query: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "original_query": natural_query
            }
    
    async def translate_to_insert(
        self,
        natural_command: str,
        tables_schema: List[TableSchema],
        database_type: str = "postgresql"
    ) -> Dict[str, Any]:
        """
        Translate a natural language command to an INSERT SQL statement.
        
        Args:
            natural_command: The natural language command
            tables_schema: List of table schemas for context
            database_type: Type of database
            
        Returns:
            Dictionary with SQL query and metadata
        """
        try:
            system_prompt = self._create_system_prompt(tables_schema, database_type, "INSERT")
            
            user_prompt = f"""
Please convert this natural language command to an INSERT SQL statement:

"{natural_command}"

Return ONLY the SQL statement, without any explanation or additional text.
"""
            
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1,
                max_tokens=500
            )
            
            sql_query = response.choices[0].message.content.strip()
            sql_query = self._clean_sql_query(sql_query)
            
            if not self._is_insert_query(sql_query):
                raise ValueError("Generated query is not a valid INSERT statement")
            
            logger.info(f"Successfully translated natural language to INSERT: {sql_query}")
            
            return {
                "success": True,
                "sql_query": sql_query,
                "query_type": "INSERT",
                "original_command": natural_command,
                "model_used": self.model
            }
            
        except Exception as e:
            logger.error(f"Failed to translate INSERT command: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "original_command": natural_command
            }
    
    async def translate_to_update(
        self,
        natural_command: str,
        tables_schema: List[TableSchema],
        database_type: str = "postgresql"
    ) -> Dict[str, Any]:
        """
        Translate a natural language command to an UPDATE SQL statement.
        
        Args:
            natural_command: The natural language command
            tables_schema: List of table schemas for context
            database_type: Type of database
            
        Returns:
            Dictionary with SQL query and metadata
        """
        try:
            system_prompt = self._create_system_prompt(tables_schema, database_type, "UPDATE")
            
            user_prompt = f"""
Please convert this natural language command to an UPDATE SQL statement:

"{natural_command}"

Return ONLY the SQL statement, without any explanation or additional text.
IMPORTANT: Always include a WHERE clause to prevent accidental bulk updates.
"""
            
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1,
                max_tokens=500
            )
            
            sql_query = response.choices[0].message.content.strip()
            sql_query = self._clean_sql_query(sql_query)
            
            if not self._is_update_query(sql_query):
                raise ValueError("Generated query is not a valid UPDATE statement")
            
            # Safety check: ensure WHERE clause exists
            if "WHERE" not in sql_query.upper():
                raise ValueError("UPDATE statement must include a WHERE clause for safety")
            
            logger.info(f"Successfully translated natural language to UPDATE: {sql_query}")
            
            return {
                "success": True,
                "sql_query": sql_query,
                "query_type": "UPDATE",
                "original_command": natural_command,
                "model_used": self.model
            }
            
        except Exception as e:
            logger.error(f"Failed to translate UPDATE command: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "original_command": natural_command
            }
    
    async def translate_to_delete(
        self,
        natural_command: str,
        tables_schema: List[TableSchema],
        database_type: str = "postgresql"
    ) -> Dict[str, Any]:
        """
        Translate a natural language command to a DELETE SQL statement.
        
        Args:
            natural_command: The natural language command
            tables_schema: List of table schemas for context
            database_type: Type of database
            
        Returns:
            Dictionary with SQL query and metadata
        """
        try:
            system_prompt = self._create_system_prompt(tables_schema, database_type, "DELETE")
            
            user_prompt = f"""
Please convert this natural language command to a DELETE SQL statement:

"{natural_command}"

Return ONLY the SQL statement, without any explanation or additional text.
CRITICAL: Always include a WHERE clause to prevent accidental bulk deletions.
"""
            
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1,
                max_tokens=500
            )
            
            sql_query = response.choices[0].message.content.strip()
            sql_query = self._clean_sql_query(sql_query)
            
            if not self._is_delete_query(sql_query):
                raise ValueError("Generated query is not a valid DELETE statement")
            
            # Safety check: ensure WHERE clause exists
            if "WHERE" not in sql_query.upper():
                raise ValueError("DELETE statement must include a WHERE clause for safety")
            
            logger.info(f"Successfully translated natural language to DELETE: {sql_query}")
            
            return {
                "success": True,
                "sql_query": sql_query,
                "query_type": "DELETE",
                "original_command": natural_command,
                "model_used": self.model
            }
            
        except Exception as e:
            logger.error(f"Failed to translate DELETE command: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "original_command": natural_command
            }
    
    def _create_system_prompt(self, tables_schema: List[TableSchema], database_type: str, query_type: str) -> str:
        """
        Create a system prompt with database schema information.
        
        Args:
            tables_schema: List of table schemas
            database_type: Type of database
            query_type: Type of SQL query (SELECT, INSERT, etc.)
            
        Returns:
            System prompt string
        """
        schema_info = ""
        for table in tables_schema:
            schema_info += f"\nTable: {table.table_name}\n"
            schema_info += "Columns:\n"
            for col in table.columns:
                nullable = "NULL" if col.get('is_nullable') == 'YES' else "NOT NULL"
                schema_info += f"  - {col['column_name']} ({col['data_type']}) {nullable}\n"
            
            if table.primary_keys:
                schema_info += f"Primary Keys: {', '.join(table.primary_keys)}\n"
            
            if table.foreign_keys:
                fk_info = []
                for fk in table.foreign_keys:
                    fk_info.append(f"{fk['column']} -> {fk['foreign_table']}.{fk['foreign_column']}")
                schema_info += f"Foreign Keys: {', '.join(fk_info)}\n"
            
            schema_info += "\n"
        
        return f"""You are an expert SQL translator for {database_type} databases.
Your task is to convert natural language queries into valid {query_type} SQL statements.

Database Schema:
{schema_info}

Guidelines:
1. Generate only valid {database_type} SQL syntax
2. Use appropriate table and column names from the schema
3. For {query_type} queries, ensure proper syntax and safety measures
4. Do not include explanations, only return the SQL statement
5. Use double quotes for identifiers if needed
6. Be precise with data types and constraints
"""
    
    def _clean_sql_query(self, sql: str) -> str:
        """Clean up the generated SQL query."""
        # Remove markdown code blocks if present
        sql = re.sub(r'^```[sql]*\n?', '', sql, flags=re.MULTILINE)
        sql = re.sub(r'\n?```$', '', sql, flags=re.MULTILINE)
        
        # Remove extra whitespace
        sql = sql.strip()
        
        # Ensure semicolon at end if not present
        if not sql.endswith(';'):
            sql += ';'
        
        return sql
    
    def _is_select_query(self, sql: str) -> bool:
        """Check if the query is a valid SELECT statement."""
        return sql.strip().upper().startswith('SELECT')
    
    def _is_insert_query(self, sql: str) -> bool:
        """Check if the query is a valid INSERT statement."""
        return sql.strip().upper().startswith('INSERT')
    
    def _is_update_query(self, sql: str) -> bool:
        """Check if the query is a valid UPDATE statement."""
        return sql.strip().upper().startswith('UPDATE')
    
    def _is_delete_query(self, sql: str) -> bool:
        """Check if the query is a valid DELETE statement."""
        return sql.strip().upper().startswith('DELETE')


# Global translator instance
_translator = None


def get_translator() -> SQLTranslator:
    """Get the global SQL translator instance."""
    global _translator
    if _translator is None:
        _translator = SQLTranslator()
    return _translator
