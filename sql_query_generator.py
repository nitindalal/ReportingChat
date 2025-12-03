"""
SQL Query Generator module for converting natural language to SQL
"""

import pandas as pd
import re
import os
from typing import Dict, Optional, Tuple
import google.generativeai as genai
from config import GEMINI_MODEL
from database import get_database

# Optional import for pandasql (fallback)
try:
    from pandasql import sqldf
    PANDASQL_AVAILABLE = True
except ImportError:
    PANDASQL_AVAILABLE = False
    sqldf = None


def get_api_key() -> Optional[str]:
    """Get Gemini API key from environment variable or Streamlit secrets"""
    try:
        import streamlit as st
        # Try Streamlit secrets first
        if hasattr(st, 'secrets') and 'gemini' in st.secrets and 'api_key' in st.secrets.gemini:
            return st.secrets.gemini.api_key
    except:
        pass
    
    # Fall back to environment variable
    return os.getenv('GEMINI_API_KEY')


def generate_table_schema(df: pd.DataFrame, column_mapping: Dict[str, Optional[str]], use_database: bool = True) -> str:
    """
    Generate a SQL table schema description for the LLM.
    
    Args:
        df: DataFrame (used if database not available)
        column_mapping: Column name mappings
        use_database: If True, try to get schema from database
    
    Returns:
        String description of the table schema
    """
    # Try to get schema from database first
    if use_database:
        try:
            db = get_database()
            schema = db.get_table_info()
            if schema and not schema.startswith("Error"):
                # Add column mappings as comments
                schema += "\n\n-- Column Mappings:"
                for standard_col, actual_col in column_mapping.items():
                    if actual_col:
                        schema += f"\n-- {standard_col} -> {actual_col}"
                return schema
        except Exception:
            pass  # Fall through to DataFrame-based schema
    
    # Fallback to generating schema from DataFrame
    schema_parts = ["CREATE TABLE campaign_data ("]
    
    # Map standard columns to actual column names
    reverse_mapping = {v: k for k, v in column_mapping.items() if v}
    
    for col in df.columns:
        col_type = "TEXT" if df[col].dtype == 'object' else "REAL"
        standard_name = reverse_mapping.get(col, col.lower().replace(' ', '_'))
        schema_parts.append(f"  {col} {col_type},  -- Standard name: {standard_name}")
    
    schema_parts.append(");")
    
    # Add column descriptions
    schema_parts.append("\n-- Column Mappings:")
    for standard_col, actual_col in column_mapping.items():
        if actual_col:
            schema_parts.append(f"-- {standard_col} -> {actual_col}")
    
    return "\n".join(schema_parts)


def generate_sql_from_nl(question: str, schema: str, sample_data: str) -> Optional[str]:
    """
    Use LLM to convert natural language question to SQL.
    
    Args:
        question: Natural language question
        schema: SQL schema description
        sample_data: Sample of the data to help understand structure
    
    Returns:
        SQL query string or None if conversion fails
    """
    api_key = get_api_key()
    if not api_key:
        print("DEBUG: No API key found for SQL generation")
        return None
    
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(GEMINI_MODEL)
    
    prompt = f"""You are a SQL query generator. Convert the following natural language question into a SQL query.

Database Schema:
{schema}

Sample Data (first 3 rows):
{sample_data}

User Question: {question}

IMPORTANT RULES:
1. Generate ONLY a valid SQL SELECT query, nothing else
2. Use the exact column names from the schema (case-sensitive)
3. Use the table name "campaign_data" in your query
4. Do NOT include any explanations, comments, or markdown formatting
5. Use standard SQL syntax compatible with SQLite
6. For aggregations, use appropriate functions (SUM, COUNT, AVG, etc.)
7. For filtering, use WHERE clauses
8. For grouping, use GROUP BY
9. For ordering, use ORDER BY
10. Return ONLY the SQL query, no other text

SQL Query:"""

    try:
        print("DEBUG: Calling Gemini to generate SQL...")
        response = model.generate_content(prompt)
        sql_query = response.text.strip()
        print(f"DEBUG: Raw LLM response: {sql_query[:200]}...")  # First 200 chars
        
        # Clean up the SQL query - remove markdown code blocks if present
        sql_query = re.sub(r'```sql\s*', '', sql_query, flags=re.IGNORECASE)
        sql_query = re.sub(r'```\s*', '', sql_query)
        sql_query = sql_query.strip()
        
        print(f"DEBUG: Cleaned SQL: {sql_query}")
        
        # Basic validation - must start with SELECT
        if not sql_query.upper().startswith('SELECT'):
            print("DEBUG: SQL validation failed - doesn't start with SELECT")
            return None
        
        return sql_query
    
    except Exception as e:
        print(f"DEBUG: Error generating SQL: {str(e)}")
        return None


def execute_sql_query(df: pd.DataFrame, sql_query: str, use_database: bool = True) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Execute SQL query on pandas DataFrame or SQLite database.
    
    Args:
        df: DataFrame to query (used as fallback if database not available)
        sql_query: SQL query string
        use_database: If True, try to use SQLite database first
    
    Returns:
        Tuple of (result DataFrame, error message)
    """
    # Try database first if enabled
    if use_database:
        try:
            print("DEBUG: Attempting database execution...")
            # Try to get database path from session state if available
            db_path = None
            try:
                import streamlit as st
                if hasattr(st, 'session_state') and 'database_path' in st.session_state:
                    db_path = st.session_state.database_path
                    print(f"DEBUG: Using database path from session state: {db_path}")
            except:
                pass
            
            db = get_database(db_path)
            print(f"DEBUG: Database instance: {db}, path: {db.db_path}")
            
            # Execute SQL (database.execute_sql handles thread safety internally)
            result, error = db.execute_sql(sql_query)
            if result is not None:
                print(f"DEBUG: Database execution successful, rows: {len(result)}")
                return result, None
            else:
                print(f"DEBUG: Database execution failed: {error}")
                # If database fails, fall through to pandasql
        except Exception as e:
            print(f"DEBUG: Database exception: {str(e)}")
            import traceback
            traceback.print_exc()
            # Fall through to pandasql
    
    # Fallback to pandasql for in-memory execution
    print(f"DEBUG: Falling back to pandasql, available: {PANDASQL_AVAILABLE}")
    if not PANDASQL_AVAILABLE:
        return None, "SQL execution failed: pandasql not available and database unavailable. Please install pandasql: pip3 install pandasql"
    
    try:
        print("DEBUG: Executing with pandasql...")
        result = sqldf(sql_query, locals())
        print(f"DEBUG: pandasql execution successful, rows: {len(result)}")
        return result, None
    except Exception as e:
        print(f"DEBUG: pandasql execution error: {str(e)}")
        return None, f"SQL execution error: {str(e)}"


def is_data_retrieval_query(question: str) -> bool:
    """
    Determine if a question is a data retrieval query (should use SQL)
    vs an analysis query (should use LLM analysis).
    
    Returns:
        True if it's a data retrieval query, False if it needs analysis
    """
    question_lower = question.lower().strip()
    
    # More comprehensive retrieval patterns using regex for word boundaries
    retrieval_patterns = [
        r'\blist\b', r'\bshow\b', r'\bwhat are\b', r'\bhow many\b',
        r'\bcount\b', r'\btotal\b', r'\bsum\b', r'\baverage\b',
        r'\bavg\b', r'\bmin\b', r'\bmax\b', r'\bwhich\b',
        r'\ball\b', r'\bfind\b', r'\bget\b', r'\bretrieve\b',
        r'\bdisplay\b', r'\bwhat is the\b', r'\bwhat are the\b',
        r'\bnames\b.*\bcampaign', r'\bcampaign.*\bnames\b',
        r'\bwhat\s+campaigns', r'\bcampaign\s+names'
    ]
    
    # Keywords that suggest analysis (higher priority)
    analysis_keywords = [
        'analyze', 'analysis', 'insight', 'recommend', 'suggest',
        'optimize', 'improve', 'best performing', 'worst performing',
        'why', 'explain', 'compare', 'trend', 'pattern', 'strategy',
        'should i', 'what should', 'how can', 'advice', 'guidance',
        'performance', 'optimization', 'recommendation'
    ]
    
    # Check for analysis keywords first (higher priority)
    if any(keyword in question_lower for keyword in analysis_keywords):
        return False
    
    # Check for retrieval patterns using regex
    if any(re.search(pattern, question_lower) for pattern in retrieval_patterns):
        return True
    
    # Default to analysis for ambiguous queries
    return False


def format_sql_result(result_df: pd.DataFrame) -> str:
    """
    Format SQL query result as a readable string.
    
    Args:
        result_df: DataFrame result from SQL query
    
    Returns:
        Formatted string representation
    """
    if result_df is None or result_df.empty:
        return "No results found."
    
    # Convert to markdown table for better display
    return result_df.to_markdown(index=False)


def query_with_sql(df: pd.DataFrame, column_mapping: Dict[str, Optional[str]], question: str, use_database: bool = True) -> Tuple[Optional[str], Optional[str]]:
    """
    Attempt to answer question using SQL query.
    
    Args:
        df: DataFrame with campaign data
        column_mapping: Mapping of standard to actual column names
        question: Natural language question
        use_database: If True, use SQLite database for execution
    
    Returns:
        Tuple of (formatted result string, error message)
    """
    print(f"DEBUG: Generating SQL for question: '{question}'")
    
    # Generate schema
    schema = generate_table_schema(df, column_mapping, use_database)
    print(f"DEBUG: Schema generated: {len(schema)} chars")
    
    # Get sample data
    if use_database:
        try:
            db = get_database()
            sample_data = db.get_sample_data(limit=3)
        except Exception as e:
            print(f"DEBUG: Database sample failed: {e}")
            sample_data = df.head(3).to_string()
    else:
        sample_data = df.head(3).to_string()
    
    # Generate SQL from natural language
    sql_query = generate_sql_from_nl(question, schema, sample_data)
    
    if not sql_query:
        print("DEBUG: SQL generation failed - returned None")
        return None, "Could not generate SQL query from question"
    
    print(f"DEBUG: Generated SQL: {sql_query}")
    
    # Execute SQL
    print("DEBUG: Executing SQL query...")
    result_df, error = execute_sql_query(df, sql_query, use_database)
    
    if error:
        print(f"DEBUG: SQL execution error: {error}")
        return None, error
    
    print(f"DEBUG: SQL execution successful, rows: {len(result_df)}")
    
    # Format result
    formatted_result = format_sql_result(result_df)
    
    return formatted_result, None

