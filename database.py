"""
Database module for storing campaign data in SQLite
"""

import pandas as pd
import sqlite3
import tempfile
import os
from typing import Dict, Optional, Tuple
from pathlib import Path
from metrics_calculator import add_row_level_metrics

# Try to import SQLAlchemy for better pandas to_sql support
try:
    from sqlalchemy import create_engine
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False
    create_engine = None


class CampaignDatabase:
    """Manages SQLite database for campaign data"""
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize database connection.
        
        Args:
            db_path: Path to SQLite database file. If None, uses temporary file.
        """
        if db_path is None:
            # Use temporary file that persists for the session
            temp_dir = Path(tempfile.gettempdir())
            db_path = str(temp_dir / "ads_reporting_copilot.db")
        
        self.db_path = db_path
        self.conn = None
        self.table_name = "campaign_data"
    
    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        """Safely quote identifiers for SQLite queries."""
        return f'"{identifier.replace("\"", "\"\"")}"'
    
    def connect(self):
        """Create database connection (always creates new connection for thread safety)"""
        # Always create a new connection to avoid thread safety issues
        # SQLite connections can't be shared across threads in Streamlit
        print(f"DEBUG: Creating new connection to database: {self.db_path}")
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        # Enable foreign keys
        self.conn.execute("PRAGMA foreign_keys = ON")
        return self.conn
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def store_dataframe(self, df: pd.DataFrame, column_mapping: Dict[str, Optional[str]]) -> bool:
        """
        Store DataFrame in SQLite database.
        
        Args:
            df: DataFrame to store
            column_mapping: Mapping of standard to actual column names
        
        Returns:
            True if successful
        """
        try:
            # Ensure connection is open
            if not self.conn:
                self.connect()
            
            df_to_store = add_row_level_metrics(df, column_mapping)
            
            print(f"DEBUG: Storing dataframe to database: {self.db_path}, table: {self.table_name}")
            print(f"DEBUG: DataFrame shape: {df_to_store.shape}, columns: {list(df_to_store.columns)}")
            print(f"DEBUG: Connection object: {self.conn}")
            print(f"DEBUG: Database file exists: {os.path.exists(self.db_path)}")
            print(f"DEBUG: SQLAlchemy available: {SQLALCHEMY_AVAILABLE}")
            
            # Ensure the DataFrame is not empty
            if df_to_store.empty:
                print("DEBUG: ERROR - DataFrame is empty!")
                return False
            
            # Use SQLAlchemy engine if available, otherwise use connection directly
            if SQLALCHEMY_AVAILABLE:
                # Use SQLAlchemy engine for better compatibility
                # Convert to absolute path and use proper URL format
                abs_db_path = os.path.abspath(self.db_path)
                
                # Ensure directory exists
                db_dir = os.path.dirname(abs_db_path)
                if db_dir and not os.path.exists(db_dir):
                    os.makedirs(db_dir, exist_ok=True)
                    print(f"DEBUG: Created database directory: {db_dir}")
                
                # SQLAlchemy needs 3 slashes for absolute paths: sqlite:///absolute/path
                engine_url = f'sqlite:///{abs_db_path}'
                print(f"DEBUG: Writing to SQL using SQLAlchemy engine: {engine_url}")
                print(f"DEBUG: Absolute path: {abs_db_path}")
                print(f"DEBUG: Path exists before write: {os.path.exists(abs_db_path)}")
                
                try:
                    engine = create_engine(engine_url, echo=False)
                    print(f"DEBUG: Engine created, about to call to_sql...")
                    df_to_store.to_sql(self.table_name, engine, if_exists='replace', index=False)
                    print(f"DEBUG: to_sql completed successfully")
                    engine.dispose()
                    print(f"DEBUG: Engine disposed")
                    
                    # Check if file was created
                    print(f"DEBUG: Path exists after write: {os.path.exists(abs_db_path)}")
                    print(f"DEBUG: File size: {os.path.getsize(abs_db_path) if os.path.exists(abs_db_path) else 0} bytes")
                except Exception as to_sql_error:
                    print(f"DEBUG: ERROR in to_sql: {str(to_sql_error)}")
                    import traceback
                    traceback.print_exc()
                    raise
                
                # Don't try to close/reconnect - the old connection might be from a different thread
                # Verification will use SQLAlchemy engine which is thread-safe
                self.conn = None
            else:
                # Fallback to direct connection (may not work with all pandas versions)
                print(f"DEBUG: Writing to SQL using direct connection")
                df_to_store.to_sql(self.table_name, self.conn, if_exists='replace', index=False)
                self.conn.commit()
            
            # Verify the data was stored using a fresh connection
            try:
                # Use SQLAlchemy engine for verification to avoid thread issues
                if SQLALCHEMY_AVAILABLE:
                    abs_db_path = os.path.abspath(self.db_path)
                    engine_url = f'sqlite:///{abs_db_path}'
                    verify_engine = create_engine(engine_url, echo=False)
                    result_df = pd.read_sql_query(f"SELECT COUNT(*) as count FROM {self.table_name}", verify_engine)
                    count = result_df.iloc[0]['count']
                    verify_engine.dispose()
                else:
                    # Fallback to direct connection
                    fresh_conn = sqlite3.connect(self.db_path, check_same_thread=False)
                    cursor = fresh_conn.cursor()
                    cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
                    count = cursor.fetchone()[0]
                    fresh_conn.close()
                
                print(f"DEBUG: Database storage successful, rows stored: {count}")
                
                # Verify table exists
                if SQLALCHEMY_AVAILABLE:
                    verify_engine = create_engine(engine_url, echo=False)
                    table_check_df = pd.read_sql_query(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                        verify_engine,
                        params=(self.table_name,)
                    )
                    table_check = len(table_check_df) > 0
                    verify_engine.dispose()
                else:
                    fresh_conn = sqlite3.connect(self.db_path, check_same_thread=False)
                    cursor = fresh_conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (self.table_name,))
                    table_check = cursor.fetchone() is not None
                    fresh_conn.close()
                
                print(f"DEBUG: Table existence check: {table_check}")
                
                if not table_check:
                    print(f"DEBUG: ERROR - Table '{self.table_name}' does not exist after storage!")
                    return False
                
                if count == 0:
                    print("DEBUG: WARNING - Table created but has 0 rows!")
                    return False
                
                return True
            except Exception as verify_error:
                print(f"DEBUG: Error during verification: {str(verify_error)}")
                import traceback
                traceback.print_exc()
                return False
        except Exception as e:
            error_msg = str(e)
            print(f"DEBUG: Error storing dataframe: {error_msg}")
            print(f"DEBUG: Error type: {type(e).__name__}")
            import traceback
            print("DEBUG: Full traceback:")
            traceback.print_exc()
            # Also print to stderr for better visibility
            import sys
            print(f"ERROR: Database storage failed: {error_msg}", file=sys.stderr)
            return False
    
    def execute_sql(self, sql_query: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        Execute SQL query on the database.
        Uses SQLAlchemy engine to avoid thread safety issues.
        
        Args:
            sql_query: SQL query string
        
        Returns:
            Tuple of (result DataFrame, error message)
        """
        try:
            # Always use SQLAlchemy engine for thread-safe operations
            if SQLALCHEMY_AVAILABLE:
                abs_db_path = os.path.abspath(self.db_path)
                engine_url = f'sqlite:///{abs_db_path}'
                engine = create_engine(engine_url, echo=False)
                
                # Check if table exists
                table_check_df = pd.read_sql_query(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    engine,
                    params=(self.table_name,)
                )
                
                if len(table_check_df) == 0:
                    engine.dispose()
                    return None, f"Table '{self.table_name}' does not exist in database. Please upload a CSV file first."
                
                print(f"DEBUG: Executing SQL on database: {sql_query}")
                # Execute query and return as DataFrame
                result_df = pd.read_sql_query(sql_query, engine)
                engine.dispose()
                return result_df, None
            else:
                # Fallback to direct connection (with thread safety disabled)
                fresh_conn = sqlite3.connect(self.db_path, check_same_thread=False)
                cursor = fresh_conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (self.table_name,))
                table_exists = cursor.fetchone()
                
                if not table_exists:
                    fresh_conn.close()
                    return None, f"Table '{self.table_name}' does not exist in database. Please upload a CSV file first."
                
                print(f"DEBUG: Executing SQL on database: {sql_query}")
                result_df = pd.read_sql_query(sql_query, fresh_conn)
                fresh_conn.close()
                return result_df, None
        
        except Exception as e:
            error_msg = f"SQL execution error: {str(e)}"
            print(f"DEBUG: Database SQL error: {error_msg}")
            import traceback
            traceback.print_exc()
            return None, error_msg
    
    def get_campaign_performance_summary(
        self,
        column_mapping: Dict[str, Optional[str]],
        limit: int = 50
    ) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        Aggregate campaign-level performance using SQL so downstream analysis is grounded in the database.
        
        Args:
            column_mapping: Mapping of standard columns to actual CSV columns
            limit: Optional limit for the number of campaigns returned
        
        Returns:
            Tuple of (DataFrame, error message)
        """
        campaign_col = column_mapping.get('campaign_name')
        if not campaign_col:
            return None, "Campaign name column is missing; cannot build campaign summary."
        
        impressions_col = column_mapping.get('impressions')
        clicks_col = column_mapping.get('clicks')
        cost_col = column_mapping.get('cost')
        conversions_col = column_mapping.get('conversions')
        revenue_col = column_mapping.get('revenue')
        
        select_parts = [f"{self._quote_identifier(campaign_col)} AS campaign"]
        
        if impressions_col:
            select_parts.append(f"SUM({self._quote_identifier(impressions_col)}) AS impressions")
        if clicks_col:
            select_parts.append(f"SUM({self._quote_identifier(clicks_col)}) AS clicks")
        if cost_col:
            select_parts.append(f"SUM({self._quote_identifier(cost_col)}) AS cost")
        if conversions_col:
            select_parts.append(f"SUM({self._quote_identifier(conversions_col)}) AS conversions")
        if revenue_col:
            select_parts.append(f"SUM({self._quote_identifier(revenue_col)}) AS revenue")
        
        if clicks_col and impressions_col:
            select_parts.append(
                f"CASE WHEN SUM({self._quote_identifier(impressions_col)}) > 0 "
                f"THEN SUM({self._quote_identifier(clicks_col)}) * 100.0 / SUM({self._quote_identifier(impressions_col)}) END AS ctr"
            )
        if cost_col and clicks_col:
            select_parts.append(
                f"CASE WHEN SUM({self._quote_identifier(clicks_col)}) > 0 "
                f"THEN SUM({self._quote_identifier(cost_col)}) / SUM({self._quote_identifier(clicks_col)}) END AS cpc"
            )
        if cost_col and conversions_col:
            select_parts.append(
                f"CASE WHEN SUM({self._quote_identifier(conversions_col)}) > 0 "
                f"THEN SUM({self._quote_identifier(cost_col)}) / SUM({self._quote_identifier(conversions_col)}) END AS cpa"
            )
        if revenue_col and cost_col:
            select_parts.append(
                f"CASE WHEN SUM({self._quote_identifier(cost_col)}) > 0 "
                f"THEN SUM({self._quote_identifier(revenue_col)}) / SUM({self._quote_identifier(cost_col)}) END AS roas"
            )
        if conversions_col and clicks_col:
            select_parts.append(
                f"CASE WHEN SUM({self._quote_identifier(clicks_col)}) > 0 "
                f"THEN SUM({self._quote_identifier(conversions_col)}) * 100.0 / SUM({self._quote_identifier(clicks_col)}) END AS cvr"
            )
        
        select_clause = ",\n  ".join(select_parts)
        query = (
            f"SELECT\n  {select_clause}\n"
            f"FROM {self.table_name}\n"
            f"GROUP BY {self._quote_identifier(campaign_col)}\n"
        )
        
        # Order by highest cost (fallback to clicks/impressions) to surface biggest spenders first
        if cost_col:
            query += "ORDER BY cost DESC\n"
        elif revenue_col:
            query += "ORDER BY revenue DESC\n"
        elif clicks_col:
            query += "ORDER BY clicks DESC\n"
        elif impressions_col:
            query += "ORDER BY impressions DESC\n"
        
        if limit and limit > 0:
            query += f"LIMIT {int(limit)}"
        
        return self.execute_sql(query)
    
    def get_table_info(self) -> str:
        """
        Get table schema information for SQL generation.
        
        Returns:
            String description of table schema
        """
        try:
            # Use SQLAlchemy for thread safety
            if SQLALCHEMY_AVAILABLE:
                abs_db_path = os.path.abspath(self.db_path)
                engine_url = f'sqlite:///{abs_db_path}'
                engine = create_engine(engine_url, echo=False)
                
                # Check if table exists
                table_check_df = pd.read_sql_query(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    engine,
                    params=(self.table_name,)
                )
                if len(table_check_df) == 0:
                    engine.dispose()
                    return f"Error: Table '{self.table_name}' does not exist"
                
                # Get table schema
                columns_df = pd.read_sql_query(f"PRAGMA table_info({self.table_name})", engine)
                engine.dispose()
                
                if len(columns_df) == 0:
                    return f"Error: No columns found in table '{self.table_name}'"
                
                schema_parts = [f"CREATE TABLE {self.table_name} ("]
                for _, row in columns_df.iterrows():
                    schema_parts.append(f"  {row['name']} {row['type']},")
                schema_parts.append(");")
                
                return "\n".join(schema_parts)
            else:
                # Fallback
                fresh_conn = sqlite3.connect(self.db_path, check_same_thread=False)
                cursor = fresh_conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (self.table_name,))
                if not cursor.fetchone():
                    fresh_conn.close()
                    return f"Error: Table '{self.table_name}' does not exist"
                
                cursor.execute(f"PRAGMA table_info({self.table_name})")
                columns = cursor.fetchall()
                fresh_conn.close()
                
                if not columns:
                    return f"Error: No columns found in table '{self.table_name}'"
                
                schema_parts = [f"CREATE TABLE {self.table_name} ("]
                for col in columns:
                    col_id, col_name, col_type, not_null, default_val, pk = col
                    schema_parts.append(f"  {col_name} {col_type},")
                schema_parts.append(");")
                
                return "\n".join(schema_parts)
        
        except Exception as e:
            return f"Error getting table info: {str(e)}"
    
    def get_sample_data(self, limit: int = 3) -> str:
        """
        Get sample data from the table.
        
        Args:
            limit: Number of sample rows
        
        Returns:
            String representation of sample data
        """
        try:
            # Use SQLAlchemy for thread safety
            if SQLALCHEMY_AVAILABLE:
                abs_db_path = os.path.abspath(self.db_path)
                engine_url = f'sqlite:///{abs_db_path}'
                engine = create_engine(engine_url, echo=False)
                
                # Check if table exists
                table_check_df = pd.read_sql_query(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    engine,
                    params=(self.table_name,)
                )
                if len(table_check_df) == 0:
                    engine.dispose()
                    return f"Error: Table '{self.table_name}' does not exist"
                
                sample_df = pd.read_sql_query(
                    f"SELECT * FROM {self.table_name} LIMIT {limit}",
                    engine
                )
                engine.dispose()
                return sample_df.to_string()
            else:
                # Fallback
                fresh_conn = sqlite3.connect(self.db_path, check_same_thread=False)
                cursor = fresh_conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (self.table_name,))
                if not cursor.fetchone():
                    fresh_conn.close()
                    return f"Error: Table '{self.table_name}' does not exist"
                
                sample_df = pd.read_sql_query(
                    f"SELECT * FROM {self.table_name} LIMIT {limit}",
                    fresh_conn
                )
                fresh_conn.close()
                return sample_df.to_string()
        
        except Exception as e:
            return f"Error getting sample data: {str(e)}"
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


# Global database instance (session-scoped)
_db_instance: Optional[CampaignDatabase] = None


def get_database(db_path: Optional[str] = None) -> CampaignDatabase:
    """
    Get or create global database instance.
    
    Args:
        db_path: Optional database path. If provided, uses that path.
                 If None and instance exists, uses existing instance.
                 If None and no instance, creates new with default path.
    
    Returns:
        CampaignDatabase instance
    """
    global _db_instance
    
    # If db_path is provided and different from current, create new instance
    if db_path is not None:
        if _db_instance is None or _db_instance.db_path != db_path:
            if _db_instance:
                _db_instance.close()
            _db_instance = CampaignDatabase(db_path)
            print(f"DEBUG: Created new database instance with path: {db_path}")
        return _db_instance
    
    # Use existing instance or create new one
    if _db_instance is None:
        _db_instance = CampaignDatabase()
        print(f"DEBUG: Created new database instance with default path: {_db_instance.db_path}")
    else:
        print(f"DEBUG: Using existing database instance: {_db_instance.db_path}")
    
    return _db_instance


def reset_database():
    """Reset global database instance"""
    global _db_instance
    if _db_instance:
        _db_instance.close()
    _db_instance = None
    print("DEBUG: Database instance reset")
