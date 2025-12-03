"""
Data processing module for CSV parsing and column detection
"""

import pandas as pd
from typing import Dict, Optional, List, Tuple
from config import STANDARD_COLUMNS


def normalize_column_name(col_name: str) -> str:
    """Normalize column name to lowercase and strip whitespace"""
    return col_name.lower().strip()


def detect_standard_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    """
    Detect standard columns in the dataframe by matching against known variations.
    Returns a mapping of standard column names to actual column names in the CSV.
    """
    column_mapping = {}
    df_columns = [normalize_column_name(col) for col in df.columns]
    
    for standard_col, variations in STANDARD_COLUMNS.items():
        for variation in variations:
            normalized_variation = normalize_column_name(variation)
            if normalized_variation in df_columns:
                # Find the original column name (preserving case)
                original_col = df.columns[df_columns.index(normalized_variation)]
                column_mapping[standard_col] = original_col
                break
    
    return column_mapping


def process_csv(uploaded_file) -> Tuple[pd.DataFrame, Dict[str, Optional[str]]]:
    """
    Process uploaded CSV file and detect columns.
    
    Returns:
        tuple: (dataframe, column_mapping)
    """
    try:
        # Read CSV
        df = pd.read_csv(uploaded_file)
        
        # Detect standard columns
        column_mapping = detect_standard_columns(df)
        
        return df, column_mapping
    
    except Exception as e:
        raise ValueError(f"Error processing CSV: {str(e)}")


def validate_required_columns(column_mapping: Dict[str, Optional[str]], required: List[str]) -> List[str]:
    """
    Check if required columns are present in the mapping.
    Returns list of missing columns.
    """
    missing = []
    for col in required:
        if col not in column_mapping or column_mapping[col] is None:
            missing.append(col)
    return missing


def get_column_mapping_summary(column_mapping: Dict[str, Optional[str]]) -> Dict[str, str]:
    """
    Get a user-friendly summary of detected columns.
    """
    summary = {}
    for standard_col, actual_col in column_mapping.items():
        if actual_col:
            summary[standard_col] = actual_col
        else:
            summary[standard_col] = "Not detected"
    return summary
