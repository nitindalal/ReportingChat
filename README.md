# Ads Reporting Copilot

A Streamlit application that processes campaign performance CSV data, computes key advertising metrics, and provides AI-powered insights via Google's Gemini LLM.

## Features

- **Flexible CSV Upload**: Automatically detects common column names (impressions, clicks, cost, conversions, revenue, etc.)
- **Metric Computation**: Calculates CTR, CPC, CPA, and ROAS automatically
- **Visualizations**: Interactive charts showing time series and campaign comparisons
- **AI-Powered Q&A**: Ask natural language questions about your campaign performance and get optimization suggestions
- **SQL Query Generation**: Automatically converts data retrieval questions to SQL for accurate, direct answers
- **Smart Query Routing**: Intelligently routes queries to SQL (for data retrieval) or LLM (for analysis and insights)

## Installation

1. Clone or download this repository

2. Install dependencies:
```bash
pip3 install -r requirements.txt
```

3. Set up Gemini API key:
   - Get your API key from [Google AI Studio](https://makersuite.google.com/app/apikey)
   - Option 1: Set environment variable:
     ```bash
     export GEMINI_API_KEY="your-api-key-here"
     ```
   - Option 2: Use Streamlit secrets (create `.streamlit/secrets.toml`):
     ```toml
     [gemini]
     api_key = "your-api-key-here"
     ```

## Usage

1. Run the Streamlit app:
```bash
streamlit run app.py
```

2. Upload a CSV file with campaign performance data using the sidebar

3. View metrics in the **Metrics Dashboard** tab

4. Ask questions in the **Ask Questions** tab to get AI-powered insights

## CSV Format

The app automatically detects common column name variations. Your CSV should include some combination of:

- **Impressions**: `impressions`, `impr`, `imp`, `views`
- **Clicks**: `clicks`, `click`, `clk`
- **Cost**: `cost`, `spend`, `spending`, `expense`
- **Conversions**: `conversions`, `conv`, `conversion`
- **Revenue**: `revenue`, `rev`, `sales`, `income`
- **Campaign Name**: `campaign`, `campaign name`, `ad group`, `adgroup`
- **Date**: `date`, `day`, `time`, `timestamp`

## Computed Metrics

- **CTR (Click-Through Rate)**: `(clicks / impressions) * 100`
- **CPC (Cost Per Click)**: `cost / clicks`
- **CPA (Cost Per Acquisition)**: `cost / conversions`
- **ROAS (Return on Ad Spend)**: `revenue / cost`

## Project Structure

```
ReportingChat/
├── app.py                 # Main Streamlit application
├── data_processor.py      # CSV parsing and column detection
├── metrics_calculator.py  # Metric computation logic
├── visualizer.py         # Chart generation
├── llm_client.py         # Gemini API integration and query routing
├── sql_query_generator.py # Natural language to SQL conversion
├── database.py           # SQLite database management
├── config.py             # Configuration and formulas
├── requirements.txt      # Python dependencies
└── README.md             # This file
```

## Data Storage

The app uses a **hybrid approach** for data storage and SQL execution:

1. **SQLite Database** (Primary): When you upload a CSV, the data is automatically stored in a SQLite database. This provides:
   - Persistent storage (survives app restarts within the session)
   - Better performance for SQL queries
   - Standard SQL execution environment

2. **Pandas DataFrame** (Fallback): The data is also kept in memory as a pandas DataFrame for:
   - Backward compatibility with existing functions
   - Fallback if database operations fail
   - Fast in-memory operations

SQL queries are executed against the SQLite database first, with automatic fallback to pandasql if needed.

## Query Types

The app intelligently routes your questions:

- **Data Retrieval Queries** (uses SQL):
  - "List all campaign names"
  - "What is the total cost?"
  - "How many campaigns are there?"
  - "Show campaigns with revenue > 1000"
  - "What is the average CTR?"

- **Analysis Queries** (uses LLM):
  - "Which campaign is performing best?"
  - "What should I optimize?"
  - "Explain the performance trends"
  - "Give me optimization suggestions"

## Configuration

You can customize metric formulas and column mappings in `config.py`:

- `STANDARD_COLUMNS`: Define column name variations
- `METRIC_FORMULAS`: Customize metric calculation formulas
- `GEMINI_MODEL`: Change the Gemini model version

## Troubleshooting

- **API Key Error**: Make sure your Gemini API key is set correctly
- **Column Detection Issues**: Check that your CSV has at least one recognizable column name
- **Missing Metrics**: Some metrics require specific columns (e.g., ROAS needs both revenue and cost)

## License

This project is provided as-is for educational and personal use.
