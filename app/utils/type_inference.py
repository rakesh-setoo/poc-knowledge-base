"""
Smart column type inference for PostgreSQL.

Analyzes DataFrame columns to determine optimal PostgreSQL data types
by examining both column names and actual data values.
"""
import re
import warnings
from typing import Any
import pandas as pd
from sqlalchemy import BigInteger, Boolean, Date, DateTime, Numeric, String, Text

from app.logging import logger


# Column name patterns that suggest date types (use word boundaries to avoid false matches)
# These patterns are used as HINTS - data must still be validated as actual dates
DATE_COLUMN_PATTERNS = [
    r'\bdate\b', r'_dt$', r'_date$', r'^date_', r'^dt_',
    r'\bcreated\b', r'\bupdated\b', r'\bmodified\b',
    r'\btimestamp\b', r'_time$', r'_at$',
    r'\bdob\b', r'\bbirth\b', r'\bexpir',
    r'\bdeadline\b', r'\bdue_', r'^start_', r'^end_',
]

# Common date formats to try parsing
DATE_FORMATS = [
    '%Y-%m-%d',           # 2024-01-15
    '%d-%m-%Y',           # 15-01-2024
    '%d/%m/%Y',           # 15/01/2024
    '%m/%d/%Y',           # 01/15/2024
    '%Y/%m/%d',           # 2024/01/15
    '%d-%b-%Y',           # 15-Jan-2024
    '%d %b %Y',           # 15 Jan 2024
    '%b %d, %Y',          # Jan 15, 2024
    '%d-%m-%y',           # 15-01-24
    '%d/%m/%y',           # 15/01/24
]

DATETIME_FORMATS = [
    '%Y-%m-%d %H:%M:%S',  # 2024-01-15 14:30:00
    '%d-%m-%Y %H:%M:%S',  # 15-01-2024 14:30:00
    '%d/%m/%Y %H:%M:%S',  # 15/01/2024 14:30:00
    '%Y-%m-%dT%H:%M:%S',  # 2024-01-15T14:30:00 (ISO format)
]


def infer_column_types(df: pd.DataFrame) -> dict[str, Any]:
    """
    Analyze DataFrame and return SQLAlchemy type mapping for each column.
    
    Checks:
    1. Column name patterns (e.g., 'date', '_at', 'created')
    2. Actual data values in the column
    3. Data characteristics (length for strings, range for numbers)
    
    Returns:
        dict mapping column names to SQLAlchemy types
    """
    dtype_map = {}
    
    for column in df.columns:
        col_type = _infer_single_column_type(df[column], str(column))
        dtype_map[column] = col_type
        logger.debug(f"Column '{column}' inferred as: {col_type}")
    
    return dtype_map


def _infer_single_column_type(series: pd.Series, column_name: str) -> Any:
    """Infer the best PostgreSQL type for a single column."""
    
    # Drop null values for analysis
    non_null = series.dropna()
    
    if len(non_null) == 0:
        return Text()  # Empty column, default to TEXT
    
    # Check if already datetime
    if pd.api.types.is_datetime64_any_dtype(series):
        return DateTime()
    
    # Check if numeric
    if pd.api.types.is_integer_dtype(series):
        return BigInteger()
    
    if pd.api.types.is_float_dtype(series):
        return Numeric()
    
    # Check if boolean
    if pd.api.types.is_bool_dtype(series):
        return Boolean()
    
    # For object dtype (strings), do deeper analysis
    if series.dtype == 'object':
        # Note: Boolean string detection (Yes/No) is disabled - too error-prone
        # Values like 'Yes'/'No' will remain as VARCHAR
        
        # Check for date/datetime
        # Name hint alone is NOT enough - must verify data actually contains dates
        is_likely_date_by_name = _should_check_for_date(column_name)
        is_date_by_data = _is_date_column(non_null)
        
        # Only classify as date if data validation passes
        if is_date_by_data:
            if _has_time_component(non_null):
                return DateTime()
            return Date()
        
        # Check for numeric strings
        if _is_numeric_string_column(non_null):
            if _is_integer_string_column(non_null):
                return BigInteger()
            return Numeric()
        
        # String column - determine VARCHAR length or TEXT
        max_len = _get_max_string_length(non_null)
        if max_len <= 255:
            # Add 20% buffer, minimum 50
            buffer_len = max(50, int(max_len * 1.2))
            return String(min(buffer_len, 255))
        else:
            return Text()
    
    # Default fallback
    return Text()


def _should_check_for_date(column_name: str) -> bool:
    """Check if column name suggests it might be a date column."""
    column_lower = column_name.lower()
    return any(re.search(pattern, column_lower) for pattern in DATE_COLUMN_PATTERNS)


def _is_date_column(series: pd.Series) -> bool:
    """
    Check if a string column contains date values by sampling and parsing.
    Uses sampling for performance on large datasets.
    """
    # Sample up to 100 values for testing
    sample_size = min(100, len(series))
    sample = series.sample(n=sample_size, random_state=42) if len(series) > sample_size else series
    
    # Need at least 80% to parse as dates
    success_threshold = 0.8
    
    for fmt in DATE_FORMATS + DATETIME_FORMATS:
        success_count = 0
        for val in sample:
            try:
                str_val = str(val).strip()
                if str_val:
                    pd.to_datetime(str_val, format=fmt)
                    success_count += 1
            except (ValueError, TypeError):
                continue
        
        if success_count / sample_size >= success_threshold:
            return True
    
    # Try pandas' flexible date parsing as fallback
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            parsed = pd.to_datetime(sample, errors='coerce', dayfirst=True)
        valid_ratio = parsed.notna().sum() / sample_size
        return valid_ratio >= success_threshold
    except Exception:
        return False


def _has_time_component(series: pd.Series) -> bool:
    """Check if date values have time component (not just 00:00:00)."""
    sample = series.head(20)
    
    for val in sample:
        str_val = str(val).strip()
        # Check for time patterns
        if re.search(r'\d{1,2}:\d{2}(:\d{2})?', str_val):
            # Verify it's not just midnight
            if not re.search(r'00:00(:00)?$', str_val):
                return True
    
    return False


def _is_boolean_column(series: pd.Series) -> bool:
    """Check if column contains boolean-like values."""
    bool_values = {'true', 'false', 'yes', 'no', 'y', 'n', '1', '0', 't', 'f'}
    
    sample = series.head(100)
    unique_values = set(str(v).lower().strip() for v in sample)
    
    return len(unique_values) <= 3 and unique_values.issubset(bool_values)


def _is_numeric_string_column(series: pd.Series) -> bool:
    """
    Check if string column contains ONLY numeric values.
    Uses broader sampling to catch mixed alphanumeric columns.
    """
    # Use random sample from entire column for better coverage
    sample_size = min(200, len(series))
    sample = series.sample(n=sample_size, random_state=42) if len(series) > sample_size else series
    
    try:
        for val in sample:
            str_val = str(val).strip().replace(',', '')  # Handle comma separators
            if str_val:
                # Check for obvious non-numeric patterns first
                # (letters at start/end, common ID prefixes)
                if any(c.isalpha() for c in str_val):
                    return False
                float(str_val)
        return True
    except ValueError:
        return False


def _is_integer_string_column(series: pd.Series) -> bool:
    """Check if numeric string column contains only integers."""
    sample_size = min(200, len(series))
    sample = series.sample(n=sample_size, random_state=42) if len(series) > sample_size else series
    
    try:
        for val in sample:
            str_val = str(val).strip().replace(',', '')
            if str_val and '.' in str_val:
                return False
            if str_val:
                # Extra check for any letters
                if any(c.isalpha() for c in str_val):
                    return False
                int(float(str_val))
        return True
    except ValueError:
        return False


def _get_max_string_length(series: pd.Series) -> int:
    """Get maximum string length in the column."""
    return series.astype(str).str.len().max()


def convert_date_columns(df: pd.DataFrame, dtype_map: dict[str, Any]) -> pd.DataFrame:
    """
    Convert columns identified as Date/DateTime to proper pandas datetime.
    
    This ensures the data is in correct format before saving to database.
    """
    df = df.copy()
    
    for column, col_type in dtype_map.items():
        if isinstance(col_type, (Date, DateTime)):
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    df[column] = pd.to_datetime(df[column], errors='coerce', dayfirst=True)
                if isinstance(col_type, Date):
                    # Convert to date only (remove time component)
                    df[column] = df[column].dt.date
                logger.debug(f"Converted column '{column}' to datetime")
            except Exception as e:
                logger.warning(f"Failed to convert column '{column}' to datetime: {e}")
    
    return df
