"""
Visualization Detection Service

Automatically detects the best visualization type based on:
- Question keywords (trend, compare, distribution, etc.)
- Data structure (time series, categorical, aggregates)
- Result characteristics (row count, column types)
"""

import re
from typing import Any

from app.logging import logger


# Keyword patterns for visualization detection

# LINE CHART: Best for temporal trends, time-series, and continuous data
LINE_CHART_PATTERNS = [
    # Temporal keywords - time periods
    r'\btrend\b', r'\bover\s+time\b', r'\bby\s+month\b', r'\bby\s+year\b',
    r'\bby\s+week\b', r'\bby\s+day\b', r'\bby\s+quarter\b', r'\bby\s+date\b',
    r'\bmonthly\b', r'\byearly\b', r'\bweekly\b', r'\bdaily\b', r'\bquarterly\b',
    r'\bhourly\b', r'\bper\s+month\b', r'\bper\s+year\b', r'\bper\s+day\b',
    # Trend analysis terms
    r'\btime\s+series\b', r'\bgrowth\b', r'\bdecline\b', r'\bincrease\b',
    r'\bdecrease\b', r'\brise\b', r'\bfall\b', r'\bchange\b',
    r'\bhistor(y|ical)\b', r'\bprogress\b', r'\bevolution\b',
    r'\bforecast\b', r'\bprojection\b', r'\btrajectory\b', r'\bmomentum\b',
    # Common question phrasings
    r'\bhow\s+has\b', r'\bhow\s+did\b', r'\bover\s+the\s+(last|past)\b',
    r'\bacross\s+months\b', r'\bacross\s+years\b', r'\bthrough\s+time\b',
    r'\bseasonal\b', r'\bcumulative\b', r'\brolling\b', r'\bmoving\s+average\b'
]

# BAR CHART: Best for comparisons, rankings, and categorical data
BAR_CHART_PATTERNS = [
    # Ranking patterns
    r'\btop\s+\d+\b', r'\bbottom\s+\d+\b', r'\bbest\s+\d+\b', r'\bworst\s+\d+\b',
    r'\branking\b', r'\brank\b', r'\bleaders\b', r'\blaggards\b',
    r'\bhighest\b', r'\blowest\b', r'\bmost\b', r'\bleast\b',
    # Comparison patterns
    r'\bcompare\b', r'\bcomparison\b', r'\bvs\b', r'\bversus\b',
    r'\bdifference\b', r'\bgap\b',
    # Grouping patterns
    r'\bby\s+region\b', r'\bby\s+category\b', r'\bby\s+product\b',
    r'\bby\s+customer\b', r'\bby\s+name\b', r'\bby\s+type\b',
    r'\bby\s+department\b', r'\bby\s+manager\b', r'\bby\s+team\b',
    r'\bby\s+city\b', r'\bby\s+country\b', r'\bby\s+state\b',
    # Common question patterns
    r'\bwhich\s+(regions?|products?|customers?)\b', r'\bwho\s+are\b',
    r'\blist\s+all\b', r'\bshow\s+all\b'
]

# PIE CHART: Best for proportions, distributions (2-7 categories ideal)
PIE_CHART_PATTERNS = [
    r'\bdistribution\b', r'\bbreakdown\b', r'\bpercentage\b', r'\bshare\b',
    r'\bproportion\b', r'\bcomposition\b', r'\bsplit\b',
    r'\bpie\s+chart\b', r'\bdoughnut\b',
    r'\b%\s+of\b', r'\bpercent\b', r'\bfraction\b',
    r'\bmakeup\b', r'\bcontribution\b', r'\bratio\b'
]

# Date-like column names
DATE_COLUMN_PATTERNS = [
    r'date', r'time', r'month', r'year', r'week', r'day', r'period',
    r'quarter', r'created', r'updated', r'timestamp'
]

# Currency column patterns (names suggesting money values)
CURRENCY_COLUMN_PATTERNS = [
    r'amount', r'sales', r'revenue', r'price', r'cost', r'profit',
    r'value', r'total', r'gross', r'net', r'margin', r'budget',
    r'income', r'expense', r'payment', r'invoice'
]

# Percentage column patterns
PERCENTAGE_COLUMN_PATTERNS = [
    r'percent', r'percentage', r'rate', r'ratio', r'share',
    r'proportion', r'pct', r'growth', r'change'
]

# Sequential time indicators (values that suggest time series)
SEQUENTIAL_PATTERNS = [
    r'^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)',
    r'^\d{4}[-/]\d{2}',  # 2024-01, 2024/01
    r'^q[1-4]',  # Q1, Q2, Q3, Q4
    r'^(fy\s*\d+|fiscal)',  # FY 2025, Fiscal Year
    r'^\d{4}$',  # Year only: 2024, 2025
]


def _matches_patterns(text: str, patterns: list) -> bool:
    """Check if text matches any of the regex patterns."""
    text_lower = text.lower()
    return any(re.search(p, text_lower) for p in patterns)


def _has_date_column(columns: list) -> bool:
    """Check if any column appears to be a date/time column."""
    if not columns:
        return False
    for col in columns:
        col_lower = col.lower()
        if any(re.search(p, col_lower) for p in DATE_COLUMN_PATTERNS):
            return True
    return False


def _is_numeric_column(data: list, column: str) -> bool:
    """Check if a column contains numeric values."""
    if not data:
        return False
    for row in data[:10]:  # Sample first 10 rows
        value = row.get(column)
        if value is not None and not isinstance(value, (int, float)):
            return False
    return True


def _get_category_count(data: list, column: str) -> int:
    """Get the number of unique values in a column."""
    if not data:
        return 0
    unique_values = set()
    for row in data:
        value = row.get(column)
        if value is not None:
            unique_values.add(str(value))
    return len(unique_values)


def _is_currency_column(column: str) -> bool:
    """Check if column name suggests currency/money values."""
    col_lower = column.lower()
    return any(re.search(p, col_lower) for p in CURRENCY_COLUMN_PATTERNS)


def _is_percentage_column(column: str, data: list = None) -> bool:
    """Check if column contains percentage values."""
    col_lower = column.lower()
    # Check column name
    if any(re.search(p, col_lower) for p in PERCENTAGE_COLUMN_PATTERNS):
        return True
    # Check if values are in 0-100 range (likely percentages)
    if data:
        values = [row.get(column) for row in data[:10] if row.get(column) is not None]
        if values and all(isinstance(v, (int, float)) for v in values):
            if all(0 <= v <= 100 for v in values):
                return True
    return False


def _is_sequential_data(data: list, column: str) -> bool:
    """Check if first column contains sequential/time-series values."""
    if not data or len(data) < 3:
        return False
    
    values = [str(row.get(column, '')).lower() for row in data[:10]]
    
    # Check if values match sequential patterns (months, quarters, years)
    for pattern in SEQUENTIAL_PATTERNS:
        matches = sum(1 for v in values if re.search(pattern, v))
        if matches >= len(values) * 0.5:  # At least 50% match
            return True
    
    return False


def _values_sum_to_100(data: list, column: str) -> bool:
    """Check if numeric values approximately sum to 100 (pie chart suitable)."""
    if not data:
        return False
    total = 0
    for row in data:
        val = row.get(column)
        if isinstance(val, (int, float)):
            total += val
    return 95 <= total <= 105  # Allow some tolerance


def detect_visualization_type(
    question: str,
    columns: list[str],
    data: list[dict[str, Any]]
) -> str:
    """
    Detect visualization type ONLY when user explicitly requests it.
    
    Args:
        question: The user's natural language question
        columns: List of column names in the result
        data: List of result rows as dictionaries
    
    Returns:
        Visualization type: 'bar', 'line', 'pie', 'table', or 'none'
    """
    if not data:
        return "none"
    
    question_lower = question.lower()
    row_count = len(data)
    
    # Only show visualizations when user EXPLICITLY asks for them
    
    # 1. Explicit PIE CHART request
    if any(x in question_lower for x in ['pie chart', 'pie graph', 'in pie', 'as pie']):
        if row_count >= 2:
            logger.debug("Explicit pie chart request")
            return "pie"
    
    # 2. Explicit LINE CHART request
    if any(x in question_lower for x in ['line chart', 'line graph', 'in line', 'as line']):
        if row_count >= 2:
            logger.debug("Explicit line chart request")
            return "line"
    
    # 3. Explicit BAR CHART request
    if any(x in question_lower for x in ['bar chart', 'bar graph', 'in bar', 'as bar']):
        if row_count >= 2:
            logger.debug("Explicit bar chart request")
            return "bar"
    
    # 4. Generic "graph" or "chart" request - default to bar
    if any(x in question_lower for x in [' graph', ' chart', 'visuali', 'visualise', 'visualize']):
        if row_count >= 2:
            logger.debug("Generic chart request -> bar chart")
            return "bar"
    
    # 5. Explicit TABLE request
    table_keywords = [
        'tabular', 'table format', 'in table', 'as table', 'show table',
        'list all', 'show all', 'all details', 'full list', 'complete list',
        'raw data', 'detailed view', 'spreadsheet', 'export', 'data view'
    ]
    if any(x in question_lower for x in table_keywords):
        logger.debug("Explicit table request")
        return "table"
    
    # 6. "Top N" requests where N > 10 should show as table
    import re
    top_n_match = re.search(r'top\s+(\d+)', question_lower)
    if top_n_match:
        requested_count = int(top_n_match.group(1))
        if requested_count > 10:
            logger.debug("Top %d request -> table for full visibility", requested_count)
            return "table"
    
    # Default: No visualization, just text answer
    logger.debug("No explicit visualization request -> text only")
    return "none"


def get_chart_config(
    viz_type: str,
    columns: list[str],
    data: list[dict[str, Any]],
    max_points: int = 50
) -> dict:
    """
    Generate chart configuration for the frontend.
    
    Args:
        viz_type: The visualization type
        columns: List of column names
        data: List of result rows
        max_points: Maximum data points to include
    
    Returns:
        Chart configuration dictionary
    """
    if not data or not columns:
        return {"type": viz_type, "labels": [], "datasets": []}
    
    # Limit data points for performance
    chart_data = data[:max_points]
    
    # Use first column as labels, second as values
    label_col = columns[0]
    value_col = columns[1] if len(columns) > 1 else columns[0]
    
    labels = [str(row.get(label_col, "")) for row in chart_data]
    values = []
    for row in chart_data:
        val = row.get(value_col, 0)
        values.append(float(val) if isinstance(val, (int, float)) else 0)
    
    return {
        "type": viz_type,
        "labels": labels,
        "datasets": [{
            "label": value_col,
            "data": values
        }]
    }
