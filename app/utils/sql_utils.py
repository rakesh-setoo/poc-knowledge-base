import re
import sqlglot
from sqlalchemy import text
from app.db import engine
from app.logging import SQLValidationError, SQLExecutionError


QUERY_TIMEOUT_SECONDS = 10

MAX_ROWS = 1000


def extract_sql(text_response: str) -> str:
    pattern = r'```(?:sql)?\s*(.*?)\s*```'
    match = re.search(pattern, text_response, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return text_response.strip()


def validate_sql(sql: str) -> str:
    sql = extract_sql(sql)
    
    try:
        parsed = sqlglot.parse_one(sql)
        
        if parsed.key != "select":
            raise SQLValidationError("Only SELECT queries are allowed")
        
        sql_upper = sql.upper()
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'TRUNCATE', 'ALTER', 'CREATE']
        for keyword in dangerous_keywords:
            if keyword in sql_upper and keyword != parsed.key.upper():
                raise SQLValidationError(f"Query contains forbidden keyword: {keyword}")
        
        return sql
        
    except sqlglot.errors.ParseError as e:
        raise SQLValidationError(f"Invalid SQL syntax: {str(e)}")


def run_sql(sql: str) -> tuple[list, list]:
    try:
        sql_upper = sql.upper()
        if 'LIMIT' not in sql_upper:
            sql = f"{sql.rstrip(';')} LIMIT {MAX_ROWS}"
        
        with engine.connect() as conn:
            conn.execute(text(f"SET statement_timeout = '{QUERY_TIMEOUT_SECONDS * 1000}'"))
            
            result = conn.execute(text(sql))
            rows = result.fetchall()
            columns = list(result.keys())
            
            return rows, columns
            
    except Exception as e:
        error_msg = str(e)
        if 'statement timeout' in error_msg.lower():
            raise SQLExecutionError(f"Query timed out. Please try a simpler question.")
        
        raise SQLExecutionError(f"Query execution failed: {error_msg}")
