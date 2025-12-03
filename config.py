"""
Configuration for Ads Reporting Copilot
Defines metric formulas and standard column mappings
"""

# Standard column name mappings (case-insensitive, handles variations)
STANDARD_COLUMNS = {
    'impressions': ['impressions', 'impr', 'imp', 'views'],
    'clicks': ['clicks', 'click', 'clk'],
    'cost': ['cost', 'spend', 'spending', 'expense'],
    'conversions': ['conversions', 'conv', 'conversion', 'converted'],
    'revenue': ['revenue', 'rev', 'sales', 'income'],
    'campaign_name': ['campaign', 'campaign name', 'campaign_name', 'campaignname', 'ad group', 'adgroup'],
    'date': ['date', 'day', 'time', 'timestamp', 'period']
}

# Metric formulas using standard column names
METRIC_FORMULAS = {
    'CTR': {
        'formula': lambda clicks, impressions: (clicks / impressions * 100) if impressions > 0 else 0,
        'description': 'Click-Through Rate (%)',
        'required_columns': ['clicks', 'impressions']
    },
    'CPC': {
        'formula': lambda cost, clicks: (cost / clicks) if clicks > 0 else 0,
        'description': 'Cost Per Click',
        'required_columns': ['cost', 'clicks']
    },
    'CPA': {
        'formula': lambda cost, conversions: (cost / conversions) if conversions > 0 else 0,
        'description': 'Cost Per Acquisition',
        'required_columns': ['cost', 'conversions']
    },
    'ROAS': {
        'formula': lambda revenue, cost: (revenue / cost) if cost > 0 else 0,
        'description': 'Return on Ad Spend',
        'required_columns': ['revenue', 'cost']
    },
    'CVR': {
        'formula': lambda conversions, clicks: (conversions / clicks * 100) if clicks > 0 else 0,
        'description': 'Conversion Rate (%)',
        'required_columns': ['conversions', 'clicks']
    }
}

# Gemini API configuration
# Available models: gemini-2.5-pro, gemini-2.5-flash, gemini-pro-latest, gemini-flash-latest
GEMINI_MODEL = "gemini-2.5-flash"  # Fast and efficient. Use "gemini-2.5-pro" for better quality
