"""
Visualization module for displaying metrics and charts
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from typing import Dict, Optional, List
from metrics_calculator import get_aggregate_metrics, get_campaign_summary, get_date_range


def display_metric_cards(df: pd.DataFrame, column_mapping: Dict[str, Optional[str]], computed_metrics: Dict[str, Optional[float]]):
    """Display summary metric cards"""
    aggregates = get_aggregate_metrics(df, column_mapping)
    
    # Create columns for metric cards
    cols = st.columns(4)
    
    metric_display = [
        ('Impressions', 'impressions', aggregates),
        ('Clicks', 'clicks', aggregates),
        ('Cost', 'cost', aggregates),
        ('Conversions', 'conversions', aggregates),
    ]
    
    for idx, (label, key, data) in enumerate(metric_display):
        with cols[idx]:
            value = data.get(key, 0)
            if key == 'cost':
                st.metric(label, f"${value:,.2f}")
            else:
                st.metric(label, f"{value:,.0f}")
    
    # Computed metrics row
    st.markdown("### Computed Metrics")
    computed_cols = st.columns(5)
    
    computed_display = [
        ('CTR', 'CTR', computed_metrics, '%'),
        ('CPC', 'CPC', computed_metrics, '$'),
        ('CPA', 'CPA', computed_metrics, '$'),
        ('ROAS', 'ROAS', computed_metrics, 'x'),
        ('CVR', 'CVR', computed_metrics, '%'),
    ]
    
    for idx, (label, key, data, suffix) in enumerate(computed_display):
        with computed_cols[idx]:
            value = data.get(key)
            if value is not None:
                if suffix == '%':
                    st.metric(label, f"{value:.2f}%")
                elif suffix == '$':
                    st.metric(label, f"${value:.2f}")
                else:
                    st.metric(label, f"{value:.2f}x")
            else:
                st.metric(label, "N/A")


def display_time_series(df: pd.DataFrame, column_mapping: Dict[str, Optional[str]]):
    """Display time series chart if date column is available"""
    if 'date' not in column_mapping or column_mapping['date'] is None:
        return
    
    date_col = column_mapping['date']
    
    try:
        df_copy = df.copy()
        df_copy[date_col] = pd.to_datetime(df_copy[date_col])
        
        # Group by date
        date_df = df_copy.groupby(date_col).agg({
            col: 'sum' for standard_col, col in column_mapping.items()
            if col and standard_col in ['impressions', 'clicks', 'cost', 'conversions', 'revenue']
        }).reset_index()
        
        if 'cost' in column_mapping and column_mapping['cost']:
            cost_col = column_mapping['cost']
            if cost_col in date_df.columns:
                fig = px.line(date_df, x=date_col, y=cost_col, 
                            title='Cost Over Time',
                            labels={date_col: 'Date', cost_col: 'Cost ($)'})
                st.plotly_chart(fig, use_container_width=True)
        
        if 'revenue' in column_mapping and column_mapping['revenue']:
            revenue_col = column_mapping['revenue']
            if revenue_col in date_df.columns:
                fig = px.line(date_df, x=date_col, y=revenue_col,
                            title='Revenue Over Time',
                            labels={date_col: 'Date', revenue_col: 'Revenue ($)'})
                st.plotly_chart(fig, use_container_width=True)
    
    except Exception as e:
        st.warning(f"Could not generate time series chart: {str(e)}")


def display_campaign_comparison(df: pd.DataFrame, column_mapping: Dict[str, Optional[str]], top_n: int = 10):
    """Display campaign comparison charts"""
    if 'campaign_name' not in column_mapping or column_mapping['campaign_name'] is None:
        return
    
    campaign_summary = get_campaign_summary(df, column_mapping, top_n)
    
    if not campaign_summary:
        return
    
    campaign_df = pd.DataFrame(campaign_summary)
    
    # Revenue by campaign
    if 'revenue' in campaign_df.columns:
        fig = px.bar(campaign_df, x='campaign_name', y='revenue',
                    title=f'Top {top_n} Campaigns by Revenue',
                    labels={'revenue': 'Revenue ($)', 'campaign_name': 'Campaign'})
        fig.update_xaxes(tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
    
    # Cost by campaign
    if 'cost' in campaign_df.columns:
        fig = px.bar(campaign_df, x='campaign_name', y='cost',
                    title=f'Top {top_n} Campaigns by Cost',
                    labels={'cost': 'Cost ($)', 'campaign_name': 'Campaign'})
        fig.update_xaxes(tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
    
    # ROAS by campaign (if available)
    if 'ROAS' in campaign_df.columns:
        fig = px.bar(campaign_df, x='campaign_name', y='ROAS',
                    title=f'Top {top_n} Campaigns by ROAS',
                    labels={'ROAS': 'ROAS', 'campaign_name': 'Campaign'})
        fig.update_xaxes(tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)


def display_data_preview(df: pd.DataFrame, max_rows: int = 10):
    """Display preview of the uploaded data"""
    st.markdown("### Data Preview")
    st.dataframe(df.head(max_rows), use_container_width=True)
    st.caption(f"Total rows: {len(df)}")
