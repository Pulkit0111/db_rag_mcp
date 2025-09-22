"""
AI-powered query intelligence service for the Natural Language SQL MCP Server.

This module provides intelligent query analysis, optimization suggestions,
result explanations, and smart query recommendations using large language models.
"""

import json
import re
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import logging

try:
    from openai import AsyncOpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    AsyncOpenAI = None

from ..core.config import config
from ..core.exceptions import ValidationError, QueryExecutionError
from ..database.base_manager import TableSchema


logger = logging.getLogger(__name__)


class QueryIntelligence:
    """
    AI-powered query intelligence and optimization service.
    
    Provides functionality for:
    - Query optimization suggestions using AI
    - Natural language explanations of results
    - Related query suggestions
    - Performance analysis and recommendations
    - Smart query completion and correction
    """
    
    def __init__(self):
        """Initialize the query intelligence service."""
        if not OPENAI_AVAILABLE:
            logger.warning("OpenAI not available - AI features will be disabled")
            self.client = None
        elif not config or not config.llm.api_key:
            logger.warning("OpenAI API key not configured - AI features will be disabled")
            self.client = None
        else:
            self.client = AsyncOpenAI(
                api_key=config.llm.api_key,
                base_url=config.llm.base_url
            )
    
    @property
    def is_available(self) -> bool:
        """Check if AI intelligence is available."""
        return self.client is not None
    
    async def suggest_optimizations(self, sql: str, execution_stats: Dict) -> List[str]:
        """
        Generate AI-powered optimization suggestions for a SQL query.
        
        Args:
            sql: The SQL query to analyze
            execution_stats: Dictionary with execution statistics
            
        Returns:
            List of optimization suggestions
        """
        if not self.is_available:
            return ["AI optimization suggestions are not available - OpenAI not configured"]
        
        try:
            # Create detailed prompt for optimization analysis
            prompt = self._create_optimization_prompt(sql, execution_stats)
            
            response = await self.client.chat.completions.create(
                model=config.llm.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=config.llm.max_tokens
            )
            
            suggestions_text = response.choices[0].message.content.strip()
            
            # Parse the suggestions into a list
            suggestions = self._parse_suggestions(suggestions_text)
            
            logger.info(f"Generated {len(suggestions)} optimization suggestions")
            return suggestions
            
        except Exception as e:
            logger.error(f"Error generating optimization suggestions: {str(e)}")
            return [f"Error generating optimization suggestions: {str(e)}"]
    
    async def explain_results(self, query: str, results: List[Dict]) -> str:
        """
        Generate natural language explanation of query results.
        
        Args:
            query: The original natural language query
            results: Query result data
            
        Returns:
            Natural language explanation of the results
        """
        if not self.is_available:
            return "AI result explanations are not available - OpenAI not configured"
        
        try:
            # Create summary of results
            result_summary = self._create_result_summary(query, results)
            
            # Create prompt for explanation
            prompt = self._create_explanation_prompt(query, result_summary)
            
            response = await self.client.chat.completions.create(
                model=config.llm.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=min(500, config.llm.max_tokens)
            )
            
            explanation = response.choices[0].message.content.strip()
            
            logger.info("Generated result explanation")
            return explanation
            
        except Exception as e:
            logger.error(f"Error generating result explanation: {str(e)}")
            return f"Unable to generate explanation: {str(e)}"
    
    async def suggest_related_queries(
        self, 
        query: str, 
        schema_info: str, 
        recent_queries: List[str] = None
    ) -> List[str]:
        """
        Suggest related queries based on current query and database schema.
        
        Args:
            query: The current/original query
            schema_info: Database schema information
            recent_queries: List of recent queries for context
            
        Returns:
            List of suggested related queries
        """
        if not self.is_available:
            return ["AI query suggestions are not available - OpenAI not configured"]
        
        try:
            # Create prompt for related query suggestions
            prompt = self._create_related_queries_prompt(query, schema_info, recent_queries)
            
            response = await self.client.chat.completions.create(
                model=config.llm.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.5,
                max_tokens=config.llm.max_tokens
            )
            
            suggestions_text = response.choices[0].message.content.strip()
            
            # Parse suggestions into a list
            suggestions = self._parse_query_suggestions(suggestions_text)
            
            logger.info(f"Generated {len(suggestions)} related query suggestions")
            return suggestions[:5]  # Limit to top 5 suggestions
            
        except Exception as e:
            logger.error(f"Error generating related queries: {str(e)}")
            return [f"Error generating suggestions: {str(e)}"]
    
    async def improve_query(self, original_query: str, error_message: str = None) -> Dict[str, Any]:
        """
        Suggest improvements to a natural language query.
        
        Args:
            original_query: The original natural language query
            error_message: Optional error message if query failed
            
        Returns:
            Dictionary with improved query and explanation
        """
        if not self.is_available:
            return {
                "success": False,
                "message": "AI query improvement is not available - OpenAI not configured"
            }
        
        try:
            # Create prompt for query improvement
            prompt = self._create_improvement_prompt(original_query, error_message)
            
            response = await self.client.chat.completions.create(
                model=config.llm.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=config.llm.max_tokens
            )
            
            improvement_text = response.choices[0].message.content.strip()
            
            # Parse the improvement response
            improved_data = self._parse_improvement_response(improvement_text)
            
            logger.info("Generated query improvement suggestions")
            
            return {
                "success": True,
                "original_query": original_query,
                "improved_query": improved_data.get("improved_query", original_query),
                "explanation": improved_data.get("explanation", ""),
                "suggestions": improved_data.get("suggestions", []),
                "error_addressed": error_message is not None
            }
            
        except Exception as e:
            logger.error(f"Error improving query: {str(e)}")
            return {
                "success": False,
                "error": f"Error improving query: {str(e)}"
            }
    
    async def analyze_query_intent(self, query: str) -> Dict[str, Any]:
        """
        Analyze the intent and components of a natural language query.
        
        Args:
            query: Natural language query to analyze
            
        Returns:
            Dictionary with intent analysis
        """
        if not self.is_available:
            return {
                "success": False,
                "message": "AI intent analysis is not available - OpenAI not configured"
            }
        
        try:
            # Create prompt for intent analysis
            prompt = f"""
            Analyze this natural language database query and identify:
            1. Query type (SELECT, INSERT, UPDATE, DELETE, etc.)
            2. Main entities/tables likely involved
            3. Key operations (filtering, aggregation, joining, etc.)
            4. Specific requirements or conditions
            5. Expected result type (single record, list, count, summary, etc.)
            
            Query: "{query}"
            
            Provide analysis in JSON format with keys: query_type, entities, operations, conditions, result_type, confidence_score.
            """
            
            response = await self.client.chat.completions.create(
                model=config.llm.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                max_tokens=400
            )
            
            analysis_text = response.choices[0].message.content.strip()
            
            # Try to parse as JSON, fallback to text analysis
            try:
                analysis_data = json.loads(analysis_text)
            except json.JSONDecodeError:
                analysis_data = self._parse_intent_text(analysis_text)
            
            logger.info("Generated query intent analysis")
            
            return {
                "success": True,
                "query": query,
                "analysis": analysis_data
            }
            
        except Exception as e:
            logger.error(f"Error analyzing query intent: {str(e)}")
            return {
                "success": False,
                "error": f"Error analyzing intent: {str(e)}"
            }
    
    def _create_optimization_prompt(self, sql: str, execution_stats: Dict) -> str:
        """Create prompt for SQL optimization suggestions."""
        return f"""
        Analyze this SQL query and provide specific optimization suggestions:
        
        SQL Query:
        {sql}
        
        Execution Statistics:
        - Execution Time: {execution_stats.get('execution_time', 'N/A')} seconds
        - Rows Returned: {execution_stats.get('row_count', 'N/A')}
        - Success: {execution_stats.get('success', 'N/A')}
        
        Provide 3-5 specific, actionable optimization suggestions focusing on:
        1. Index recommendations
        2. Query structure improvements
        3. Performance considerations
        4. Best practices
        
        Format each suggestion as a bullet point with clear, specific advice.
        """
    
    def _create_explanation_prompt(self, query: str, result_summary: str) -> str:
        """Create prompt for result explanation."""
        return f"""
        Explain these database query results in simple, natural language that a non-technical user would understand.
        
        Original Query: "{query}"
        
        Results Summary: {result_summary}
        
        Provide a clear, concise explanation of:
        1. What the data shows
        2. Key insights or patterns
        3. What this means in practical terms
        
        Keep the explanation conversational and avoid technical jargon.
        """
    
    def _create_related_queries_prompt(
        self, 
        query: str, 
        schema_info: str, 
        recent_queries: List[str] = None
    ) -> str:
        """Create prompt for related query suggestions."""
        recent_context = ""
        if recent_queries:
            recent_context = f"""
            Recent queries for context:
            {chr(10).join(f'- {q}' for q in recent_queries[:3])}
            """
        
        return f"""
        Based on this database query and schema, suggest 5 related queries that would provide complementary insights:
        
        Original Query: "{query}"
        
        Database Schema Info: {schema_info}
        
        {recent_context}
        
        Suggest related queries that would:
        1. Explore different aspects of the same data
        2. Provide comparative analysis
        3. Dive deeper into specific areas
        4. Show trends over time
        5. Aggregate or summarize the data differently
        
        Return as a numbered list of natural language queries.
        """
    
    def _create_improvement_prompt(self, original_query: str, error_message: str = None) -> str:
        """Create prompt for query improvement."""
        error_context = ""
        if error_message:
            error_context = f"""
            The query failed with this error: {error_message}
            
            Please address this error in your improvement.
            """
        
        return f"""
        Improve this natural language database query to make it clearer, more specific, and more likely to return useful results:
        
        Original Query: "{original_query}"
        
        {error_context}
        
        Provide:
        1. An improved version of the query
        2. Explanation of what was improved
        3. Additional suggestions for getting better results
        
        Format your response as:
        IMPROVED QUERY: [your improved query]
        EXPLANATION: [what you changed and why]
        SUGGESTIONS: [additional tips for better queries]
        """
    
    def _create_result_summary(self, query: str, results: List[Dict]) -> str:
        """Create a summary of query results for AI analysis."""
        if not results:
            return "No results returned."
        
        num_results = len(results)
        
        if results:
            columns = list(results[0].keys())
            sample_record = results[0]
            
            # Create sample data representation
            sample_data = {k: str(v)[:50] + "..." if len(str(v)) > 50 else v 
                          for k, v in sample_record.items()}
            
            summary = f"""
            Found {num_results} records with columns: {', '.join(columns)}
            
            Sample record: {json.dumps(sample_data, indent=2)}
            
            Data characteristics:
            - Total records: {num_results}
            - Columns: {len(columns)}
            """
            
            # Add basic statistics for numeric columns
            numeric_stats = []
            for col in columns:
                values = [r.get(col) for r in results if r.get(col) is not None]
                if values and all(isinstance(v, (int, float)) for v in values):
                    numeric_stats.append(f"{col}: min={min(values)}, max={max(values)}, avg={sum(values)/len(values):.2f}")
            
            if numeric_stats:
                summary += f"\n\nNumeric statistics:\n" + "\n".join(numeric_stats)
            
            return summary
        
        return f"Found {num_results} records but no data available."
    
    def _parse_suggestions(self, suggestions_text: str) -> List[str]:
        """Parse optimization suggestions from AI response."""
        # Split by bullet points or numbers
        lines = suggestions_text.split('\n')
        suggestions = []
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Remove bullet points, numbers, and other prefixes
            line = re.sub(r'^[\d\.\-\*\+]\s*', '', line)
            line = re.sub(r'^[ivx]+\.\s*', '', line, flags=re.IGNORECASE)
            
            if len(line) > 10:  # Filter out very short lines
                suggestions.append(line)
        
        return suggestions[:10]  # Limit to 10 suggestions
    
    def _parse_query_suggestions(self, suggestions_text: str) -> List[str]:
        """Parse query suggestions from AI response."""
        lines = suggestions_text.split('\n')
        suggestions = []
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Remove numbering and bullet points
            line = re.sub(r'^\d+\.\s*', '', line)
            line = re.sub(r'^[\-\*\+]\s*', '', line)
            
            # Filter out instructional text
            if (len(line) > 15 and 
                not line.lower().startswith('here are') and
                not line.lower().startswith('based on') and
                '?' in line or 'show' in line.lower() or 'find' in line.lower() or 'get' in line.lower()):
                suggestions.append(line)
        
        return suggestions
    
    def _parse_improvement_response(self, improvement_text: str) -> Dict[str, Any]:
        """Parse query improvement response."""
        result = {
            "improved_query": "",
            "explanation": "",
            "suggestions": []
        }
        
        sections = improvement_text.split('\n')
        current_section = None
        
        for line in sections:
            line = line.strip()
            if not line:
                continue
            
            if line.upper().startswith('IMPROVED QUERY:'):
                current_section = 'improved_query'
                result['improved_query'] = line.split(':', 1)[1].strip()
            elif line.upper().startswith('EXPLANATION:'):
                current_section = 'explanation'
                result['explanation'] = line.split(':', 1)[1].strip()
            elif line.upper().startswith('SUGGESTIONS:'):
                current_section = 'suggestions'
                continue
            elif current_section == 'explanation':
                result['explanation'] += ' ' + line
            elif current_section == 'suggestions':
                if line.startswith('-') or line.startswith('*') or re.match(r'^\d+\.', line):
                    cleaned_line = re.sub(r'^[\d\.\-\*\+]\s*', '', line)
                    result['suggestions'].append(cleaned_line)
        
        return result
    
    def _parse_intent_text(self, analysis_text: str) -> Dict[str, Any]:
        """Parse intent analysis when JSON parsing fails."""
        return {
            "query_type": "SELECT",  # Default assumption
            "entities": [],
            "operations": [],
            "conditions": [],
            "result_type": "list",
            "confidence_score": 0.5,
            "raw_analysis": analysis_text
        }


# Global query intelligence instance
query_intelligence = QueryIntelligence()
