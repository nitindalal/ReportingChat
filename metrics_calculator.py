"""
Metrics calculator module for computing advertising metrics
"""

import pandas as pd
from typing import Dict, Optional, List
from config import METRIC_FORMULAS, STANDARD_COLUMNS


def compute_metric(df: pd.DataFrame, column_mapping: Dict[str, Optional[str]], metric_name: str) -> Optional[float]:
    """
    Compute a single metric using the formula from config.
    
    Args:
        df: DataFrame with campaign data
        column_mapping: Mapping of standard column names to actual column names
        metric_name: Name of the metric to compute (e.g., 'CTR', 'CPC')
    
    Returns:
        Computed metric value or None if columns are missing
    """
    if metric_name not in METRIC_FORMULAS:
        return None
    
    metric_config = METRIC_FORMULAS[metric_name]
    required_cols = metric_config['required_columns']
    
    # Check if all required columns are available
    for col in required_cols:
        if col not in column_mapping or column_mapping[col] is None:
            return None
    
    # Get the actual column names from mapping
    col_values = {}
    for standard_col in required_cols:
        actual_col = column_mapping[standard_col]
        col_values[standard_col] = df[actual_col]
    
    # Compute metric using the formula
    try:
        # For aggregate metrics, sum the columns first
        summed_values = {}
        for col_name, series in col_values.items():
            summed_values[col_name] = series.sum()
        
        # Apply formula with summed values
        result = metric_config['formula'](**summed_values)
        return round(result, 2) if result is not None else None
    
    except Exception as e:
        return None


def compute_all_metrics(df: pd.DataFrame, column_mapping: Dict[str, Optional[str]]) -> Dict[str, Optional[float]]:
    """
    Compute all available metrics based on available columns.
    
    Returns:
        Dictionary of metric names to values
    """
    metrics = {}
    
    for metric_name in METRIC_FORMULAS.keys():
        value = compute_metric(df, column_mapping, metric_name)
        metrics[metric_name] = value
    
    return metrics


def get_aggregate_metrics(df: pd.DataFrame, column_mapping: Dict[str, Optional[str]]) -> Dict[str, float]:
    """
    Get aggregate totals for standard columns.
    
    Returns:
        Dictionary of column names to total values
    """
    aggregates = {}
    
    for standard_col, actual_col in column_mapping.items():
        if actual_col and standard_col in ['impressions', 'clicks', 'cost', 'conversions', 'revenue']:
            try:
                aggregates[standard_col] = float(df[actual_col].sum())
            except:
                aggregates[standard_col] = 0.0
    
    return aggregates


def get_campaign_summary(df: pd.DataFrame, column_mapping: Dict[str, Optional[str]], top_n: int = 5) -> List[Dict]:
    """
    Get summary of top campaigns by revenue or cost.
    
    Returns:
        List of dictionaries with campaign metrics
    """
    if 'campaign_name' not in column_mapping or column_mapping['campaign_name'] is None:
        return []
    
    campaign_col = column_mapping['campaign_name']
    
    # Group by campaign and aggregate
    agg_dict = {}
    for standard_col, actual_col in column_mapping.items():
        if actual_col and standard_col in ['impressions', 'clicks', 'cost', 'conversions', 'revenue']:
            agg_dict[actual_col] = 'sum'
    
    if not agg_dict:
        return []
    
    campaign_df = df.groupby(campaign_col).agg(agg_dict).reset_index()
    
    # Compute metrics per campaign
    top_campaigns = []
    for _, row in campaign_df.head(top_n).iterrows():
        campaign_data = {
            'campaign_name': row[campaign_col],
        }
        
        # Add available metrics
        for standard_col, actual_col in column_mapping.items():
            if actual_col and standard_col in ['impressions', 'clicks', 'cost', 'conversions', 'revenue']:
                campaign_data[standard_col] = float(row[actual_col])
        
        # Compute campaign-level metrics
        if 'cost' in campaign_data and 'clicks' in campaign_data and campaign_data['clicks'] > 0:
            campaign_data['CPC'] = round(campaign_data['cost'] / campaign_data['clicks'], 2)
        
        if 'clicks' in campaign_data and 'impressions' in campaign_data and campaign_data['impressions'] > 0:
            campaign_data['CTR'] = round(campaign_data['clicks'] / campaign_data['impressions'] * 100, 2)
        
        if 'revenue' in campaign_data and 'cost' in campaign_data and campaign_data['cost'] > 0:
            campaign_data['ROAS'] = round(campaign_data['revenue'] / campaign_data['cost'], 2)
        
        top_campaigns.append(campaign_data)
    
    return top_campaigns


def get_all_campaign_names(df: pd.DataFrame, column_mapping: Dict[str, Optional[str]]) -> List[str]:
    """
    Get all unique campaign names from the data.
    
    Returns:
        List of unique campaign names
    """
    if 'campaign_name' not in column_mapping or column_mapping['campaign_name'] is None:
        return []
    
    campaign_col = column_mapping['campaign_name']
    try:
        return sorted(df[campaign_col].unique().tolist())
    except:
        return []


def get_date_range(df: pd.DataFrame, column_mapping: Dict[str, Optional[str]]) -> Optional[Dict[str, str]]:
    """
    Get date range from the data if date column is available.
    
    Returns:
        Dictionary with 'start' and 'end' dates or None
    """
    if 'date' not in column_mapping or column_mapping['date'] is None:
        return None
    
    date_col = column_mapping['date']
    try:
        df[date_col] = pd.to_datetime(df[date_col])
        return {
            'start': str(df[date_col].min().date()),
            'end': str(df[date_col].max().date())
        }
    except:
        return None


def add_row_level_metrics(df: pd.DataFrame, column_mapping: Dict[str, Optional[str]]) -> pd.DataFrame:
    """
    Add computed metric columns to the dataframe for SQL storage and analysis.
    Columns are prefixed with 'metric_' to avoid collisions with user-provided data.
    """
    df_with_metrics = df.copy()
    
    impressions_col = column_mapping.get('impressions')
    clicks_col = column_mapping.get('clicks')
    cost_col = column_mapping.get('cost')
    conversions_col = column_mapping.get('conversions')
    revenue_col = column_mapping.get('revenue')
    
    def has_columns(*cols: Optional[str]) -> bool:
        return all(col and col in df_with_metrics.columns for col in cols)
    
    if has_columns(clicks_col, impressions_col):
        denom = df_with_metrics[impressions_col].replace({0: pd.NA})
        df_with_metrics['metric_ctr'] = df_with_metrics[clicks_col].div(denom).mul(100)
    
    if has_columns(cost_col, clicks_col):
        denom = df_with_metrics[clicks_col].replace({0: pd.NA})
        df_with_metrics['metric_cpc'] = df_with_metrics[cost_col].div(denom)
    
    if has_columns(cost_col, conversions_col):
        denom = df_with_metrics[conversions_col].replace({0: pd.NA})
        df_with_metrics['metric_cpa'] = df_with_metrics[cost_col].div(denom)
    
    if has_columns(revenue_col, cost_col):
        denom = df_with_metrics[cost_col].replace({0: pd.NA})
        df_with_metrics['metric_roas'] = df_with_metrics[revenue_col].div(denom)
    
    if has_columns(conversions_col, clicks_col):
        denom = df_with_metrics[clicks_col].replace({0: pd.NA})
        df_with_metrics['metric_cvr'] = df_with_metrics[conversions_col].div(denom).mul(100)
    
    return df_with_metrics
