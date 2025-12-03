"""
LLM client module for Gemini API integration
"""

import json
import os
import pandas as pd
import google.generativeai as genai
from typing import Dict, Optional, Tuple
from config import GEMINI_MODEL
from metrics_calculator import (
    get_aggregate_metrics, 
    get_campaign_summary, 
    get_date_range,
    compute_all_metrics,
    get_all_campaign_names
)
from sql_query_generator import (
    is_data_retrieval_query,
    query_with_sql
)
from database import get_database


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


def _get_db_path_from_session() -> Optional[str]:
    """Retrieve database path from Streamlit session if available."""
    try:
        import streamlit as st
        if hasattr(st, "session_state") and 'database_path' in st.session_state:
            return st.session_state.database_path
    except Exception:
        return None
    return None


def _serialize_campaign_records(df: Optional[pd.DataFrame]) -> Optional[list]:
    """Convert campaign performance dataframe to JSON-serializable records with rounded metrics."""
    if df is None:
        return None
    
    records = []
    for row in df.to_dict(orient="records"):
        serialized = {}
        for key, value in row.items():
            if isinstance(value, (int, float)):
                serialized[key] = round(value, 3)
            else:
                serialized[key] = value
        records.append(serialized)
    return records


def get_sql_campaign_breakdown(
    df: pd.DataFrame,
    column_mapping: Dict[str, Optional[str]],
    limit: int = 25
) -> Tuple[Optional[list], Optional[str]]:
    """
    Try to produce campaign-level performance using SQL first, with pandas fallback if SQL is unavailable.
    Returns list of dicts for safe inclusion in prompts.
    """
    db_path = _get_db_path_from_session()
    db = get_database(db_path)
    
    try:
        sql_df, sql_error = db.get_campaign_performance_summary(column_mapping, limit=limit)
    except Exception as e:
        sql_df, sql_error = None, f"Database error: {str(e)}"
    
    if sql_df is not None:
        return _serialize_campaign_records(sql_df), None
    
    # Fallback to pandas groupby if SQL path fails
    campaign_col = column_mapping.get('campaign_name')
    if campaign_col and campaign_col in df.columns:
        agg_map = {}
        for standard_col in ['impressions', 'clicks', 'cost', 'conversions', 'revenue']:
            actual_col = column_mapping.get(standard_col)
            if actual_col and actual_col in df.columns:
                agg_map[actual_col] = 'sum'
        
        if agg_map:
            grouped = df.groupby(campaign_col).agg(agg_map).reset_index()
            rename_map = {campaign_col: 'campaign'}
            for standard_col, actual_col in column_mapping.items():
                if actual_col in grouped.columns:
                    rename_map[actual_col] = standard_col
            grouped = grouped.rename(columns=rename_map)
            
            # Compute derived aggregates on the grouped data using standard names
            if 'clicks' in grouped.columns and 'impressions' in grouped.columns:
                grouped['ctr'] = grouped['clicks'].div(grouped['impressions'].replace({0: pd.NA})).mul(100)
            
            if 'cost' in grouped.columns and 'clicks' in grouped.columns:
                grouped['cpc'] = grouped['cost'].div(grouped['clicks'].replace({0: pd.NA}))
            
            if 'cost' in grouped.columns and 'conversions' in grouped.columns:
                grouped['cpa'] = grouped['cost'].div(grouped['conversions'].replace({0: pd.NA}))
            
            if 'revenue' in grouped.columns and 'cost' in grouped.columns:
                grouped['roas'] = grouped['revenue'].div(grouped['cost'].replace({0: pd.NA}))
            
            if 'conversions' in grouped.columns and 'clicks' in grouped.columns:
                grouped['cvr'] = grouped['conversions'].div(grouped['clicks'].replace({0: pd.NA})).mul(100)
            
            sort_col = 'cost' if 'cost' in grouped.columns else None
            if not sort_col:
                for candidate in ['revenue', 'conversions', 'clicks', 'impressions']:
                    if candidate in grouped.columns:
                        sort_col = candidate
                        break
            if sort_col:
                grouped = grouped.sort_values(by=sort_col, ascending=False)
            
            return _serialize_campaign_records(grouped.head(limit)), sql_error
    
    return None, sql_error or "Could not aggregate campaign performance (missing campaign column)."


def create_compact_summary(df, column_mapping: Dict[str, Optional[str]]) -> Dict:
    """
    Create a compact JSON summary of the campaign data for the LLM.
    
    Returns:
        Dictionary with summary data
    """
    aggregates = get_aggregate_metrics(df, column_mapping)
    computed_metrics = compute_all_metrics(df, column_mapping)
    top_campaigns = get_campaign_summary(df, column_mapping, top_n=5)
    all_campaign_names = get_all_campaign_names(df, column_mapping)
    date_range = get_date_range(df, column_mapping)
    campaign_breakdown, breakdown_error = get_sql_campaign_breakdown(df, column_mapping, limit=25)
    
    summary = {
        'totals': aggregates,
        'metrics': {k: v for k, v in computed_metrics.items() if v is not None},
        'all_campaign_names': all_campaign_names,  # All unique campaign names
        'top_campaigns': top_campaigns[:5],  # Top 5 campaigns with metrics
        'date_range': date_range,
        'total_rows': len(df),
        'campaign_breakdown': campaign_breakdown if campaign_breakdown else [],
        'campaign_breakdown_error': breakdown_error
    }
    
    return summary


def query_gemini(question: str, summary: Dict) -> str:
    """
    Send question and summary to Gemini and get response.
    Used for analysis and insights, not data retrieval.
    
    Args:
        question: User's natural language question
        summary: Compact JSON summary of campaign data
    
    Returns:
        Gemini's response as a string
    """
    api_key = get_api_key()
    
    if not api_key:
        raise ValueError(
            "Gemini API key not found. Please set GEMINI_API_KEY environment variable "
            "or add it to Streamlit secrets (st.secrets.gemini.api_key)"
        )
    
    # Configure Gemini
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(GEMINI_MODEL)
    
    # Create prompt
    summary_json = json.dumps(summary, indent=2)
    
    prompt = f"""You are an advertising performance analyst assistant. Use only the data provided (campaign_breakdown is SQL-aggregated and is the source of truth).

Campaign Performance Summary (JSON):
{summary_json}

User Question: {question}

Write a clean, readable markdown answer (no code fences or inline JSON). Use consistent spacing and simple bullets. Structure:
- Title line summarizing the user ask (plain text).
- Overall Snapshot: 4–6 bullets. Each bullet starts with a bold label (e.g., **Date Range:**, **Totals:**, **Efficiency:**, **Volume:**) followed by a short sentence. Include ROAS, CTR, CPC, CPA, CVR, cost, revenue, conversions, clicks, impressions, and date range if available.
- KPI Table: a small markdown table with headers ROAS | CTR% | CVR% | CPC | CPA | Cost | Revenue | Conversions | Clicks | Impressions. Use 'n/a' if unavailable.
- Campaign Breakdown: for each campaign (sorted by highest cost, or revenue if cost missing), add a bullet with the campaign name in bold, then 3–5 sub-bullets covering: cost, revenue, conversions, CTR, CPC, CPA, ROAS, CVR, plus one Insight sentence and one Next steps sentence (both metric-backed).

Style rules:
- Plain text markdown only; no italics/underscores/code fences.
- Always include spaces between numbers and words; never run words together.
- Round numbers to 2 decimals; use thousands separators for large numbers (e.g., 12,345.67).
- One short sentence per bullet/sub-bullet.
- If a metric is missing, write 'n/a' instead of guessing."""

    try:
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        raise Exception(f"Error querying Gemini API: {str(e)}")


def process_query(df: pd.DataFrame, column_mapping: Dict[str, Optional[str]], question: str) -> Tuple[str, Optional[str]]:
    """
    Process a user question by routing to SQL (for data retrieval) or LLM (for analysis).
    
    Args:
        df: DataFrame with campaign data
        column_mapping: Mapping of standard to actual column names
        question: User's natural language question
    
    Returns:
        Tuple of (response text, error message if any)
    """
    # DEBUG: Check classification
    is_retrieval = is_data_retrieval_query(question)
    print(f"DEBUG: Question: '{question}'")
    print(f"DEBUG: Classified as retrieval: {is_retrieval}")
    
    # Determine if this is a data retrieval query
    if is_retrieval:
        print("DEBUG: Attempting SQL query path...")
        # Try SQL query first
        sql_result, sql_error = query_with_sql(df, column_mapping, question)
        
        print(f"DEBUG: SQL result: {sql_result is not None}")
        print(f"DEBUG: SQL error: {sql_error}")
        
        if sql_result:
            # SQL query succeeded - return direct result
            return sql_result, None
        else:
            # SQL failed, fall back to LLM with summary
            print(f"DEBUG: SQL failed, falling back to LLM. Error: {sql_error}")
            summary = create_compact_summary(df, column_mapping)
            try:
                llm_response = query_gemini(question, summary)
                # Prepend a note about fallback
                response = f"⚠️ *Note: Direct SQL query failed ({sql_error}), using AI analysis instead.*\n\n{llm_response}"
                return response, None
            except Exception as e:
                return None, f"SQL query failed: {sql_error}. LLM also failed: {str(e)}"
    else:
        print("DEBUG: Using LLM analysis path...")
        # Analysis query - use LLM with summary
        summary = create_compact_summary(df, column_mapping)
        try:
            response = query_gemini(question, summary)
            return response, None
        except Exception as e:
            return None, str(e)


def format_response(response_text: str) -> str:
    """
    Format the LLM response for display.
    Can add markdown formatting if needed.
    """
    return response_text
