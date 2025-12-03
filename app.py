"""
Ads Reporting Copilot - Main Streamlit Application
"""

import streamlit as st
import pandas as pd
from data_processor import process_csv, detect_standard_columns, get_column_mapping_summary
from metrics_calculator import compute_all_metrics, add_row_level_metrics
from visualizer import display_metric_cards, display_time_series, display_campaign_comparison, display_data_preview
from llm_client import process_query, get_api_key
from database import get_database, reset_database

# Page configuration
st.set_page_config(
    page_title="Ads Reporting Copilot",
    page_icon="📊",
    layout="wide"
)

# Initialize session state
if 'df' not in st.session_state:
    st.session_state.df = None
if 'column_mapping' not in st.session_state:
    st.session_state.column_mapping = {}
if 'computed_metrics' not in st.session_state:
    st.session_state.computed_metrics = {}
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'database_initialized' not in st.session_state:
    st.session_state.database_initialized = False
if 'database_path' not in st.session_state:
    st.session_state.database_path = None


def main():
    st.title("📊 Ads Reporting Copilot")
    st.markdown("Upload your campaign performance CSV and get AI-powered insights!")
    
    # Sidebar for file upload
    with st.sidebar:
        st.header("Upload Data")
        uploaded_file = st.file_uploader(
            "Choose a CSV file",
            type=['csv'],
            help="Upload a CSV file with campaign performance data"
        )
        
        if uploaded_file is not None:
            try:
                # Process CSV
                df, column_mapping = process_csv(uploaded_file)
                df_with_metrics = add_row_level_metrics(df, column_mapping)
                st.session_state.df = df_with_metrics
                st.session_state.column_mapping = column_mapping
                
                # Store in SQLite database for SQL queries
                storage_error = None
                try:
                    # Use existing database path if available, otherwise create new
                    db = get_database(st.session_state.database_path)
                    # Store the database path in session state for consistency
                    st.session_state.database_path = db.db_path
                    print(f"DEBUG: About to store dataframe, database path: {db.db_path}")
                    success = db.store_dataframe(df, column_mapping)
                    if success:
                        st.session_state.database_initialized = True
                        st.success("✅ File uploaded and stored in database!")
                    else:
                        storage_error = "Database storage returned False. Check console for details."
                        st.warning("⚠️ File uploaded but database storage failed")
                        st.error("**Error Details:** Check the terminal/console where Streamlit is running for detailed debug output.")
                        st.info("💡 SQL queries will use pandasql fallback. The app will still work for analysis queries.")
                        st.success("✅ File uploaded successfully!")
                except Exception as e:
                    import traceback
                    error_trace = traceback.format_exc()
                    storage_error = str(e)
                    print(f"DEBUG: Database storage exception:\n{error_trace}")
                    st.warning(f"⚠️ File uploaded but database storage failed: {str(e)}")
                    st.error("**Full Error:** Check the terminal/console where Streamlit is running for the complete traceback.")
                    st.info("💡 SQL queries will use pandasql fallback. The app will still work for analysis queries.")
                    st.success("✅ File uploaded successfully!")
                
                # Compute metrics
                st.session_state.computed_metrics = compute_all_metrics(df, column_mapping)
                
                # Show detected columns
                st.markdown("### Detected Columns")
                mapping_summary = get_column_mapping_summary(column_mapping)
                for standard_col, actual_col in mapping_summary.items():
                    if actual_col != "Not detected":
                        st.text(f"• {standard_col}: {actual_col}")
                    else:
                        st.text(f"• {standard_col}: ⚠️ Not found")
                
            except Exception as e:
                st.error(f"Error processing file: {str(e)}")
                st.session_state.df = None
        
        # API Key check
        st.markdown("---")
        st.markdown("### API Configuration")
        api_key = get_api_key()
        if api_key:
            st.success("✅ Gemini API key configured")
        else:
            st.warning("⚠️ Gemini API key not found")
            st.info("Set GEMINI_API_KEY environment variable or add to Streamlit secrets")
    
    # Main content area
    if st.session_state.df is not None:
        # Tabs for different views
        tab1, tab2 = st.tabs(["📈 Metrics Dashboard", "💬 Ask Questions"])
        
        with tab1:
            st.header("Campaign Performance Metrics")
            
            # Display metric cards
            display_metric_cards(
                st.session_state.df,
                st.session_state.column_mapping,
                st.session_state.computed_metrics
            )
            
            st.markdown("---")
            
            # Data preview
            display_data_preview(st.session_state.df)
            
            st.markdown("---")
            
            # Time series charts
            display_time_series(st.session_state.df, st.session_state.column_mapping)
            
            # Campaign comparison
            display_campaign_comparison(st.session_state.df, st.session_state.column_mapping)
        
        with tab2:
            st.header("Ask Questions About Your Campaign Performance")
            
            # Check API key
            api_key = get_api_key()
            if not api_key:
                st.error("⚠️ Gemini API key not configured. Please set GEMINI_API_KEY environment variable or add to Streamlit secrets.")
                st.stop()
            
            # Display chat history
            for i, (role, message) in enumerate(st.session_state.chat_history):
                with st.chat_message(role):
                    st.markdown(message)
            
            # Chat input
            user_question = st.chat_input("Ask a question about your campaign performance...")
            
            if user_question:
                # Add user question to chat
                st.session_state.chat_history.append(("user", user_question))
                
                with st.chat_message("user"):
                    st.markdown(user_question)
                
                # Generate response
                with st.chat_message("assistant"):
                    with st.spinner("Processing your question..."):
                        try:
                            # Process query (routes to SQL or LLM as appropriate)
                            response, error = process_query(
                                st.session_state.df,
                                st.session_state.column_mapping,
                                user_question
                            )
                            
                            if error:
                                st.error(f"Error: {error}")
                                st.session_state.chat_history.append(("assistant", f"Error: {error}"))
                            else:
                                # Display response
                                st.markdown(response)
                                st.session_state.chat_history.append(("assistant", response))
                        
                        except Exception as e:
                            error_msg = f"Error: {str(e)}"
                            st.error(error_msg)
                            st.session_state.chat_history.append(("assistant", error_msg))
            
            # Clear chat button
            if st.session_state.chat_history:
                if st.button("Clear Chat History"):
                    st.session_state.chat_history = []
                    st.rerun()
    
    else:
        # Welcome screen
        st.info("👈 Please upload a CSV file using the sidebar to get started.")
        
        st.markdown("""
        ### How to use:
        1. **Upload CSV**: Click on the file uploader in the sidebar
        2. **View Metrics**: Check the Metrics Dashboard tab for performance metrics
        3. **Ask Questions**: Use the Ask Questions tab to get AI-powered insights
        
        ### Expected CSV Columns:
        The app will automatically detect common column names:
        - **Impressions** (or Impr, Imp, Views)
        - **Clicks** (or Click, Clk)
        - **Cost** (or Spend, Spending, Expense)
        - **Conversions** (or Conv, Conversion)
        - **Revenue** (or Rev, Sales, Income)
        - **Campaign Name** (or Campaign, Ad Group)
        - **Date** (or Day, Time, Timestamp)
        
        ### Computed Metrics:
        - **CTR**: Click-Through Rate (%)
        - **CPC**: Cost Per Click
        - **CPA**: Cost Per Acquisition
        - **ROAS**: Return on Ad Spend
        """)


if __name__ == "__main__":
    main()
