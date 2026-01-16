import re
import sqlglot
from sqlalchemy import text
from app.db import engine
from app.logging import SQLValidationError, SQLExecutionError, logger


QUERY_TIMEOUT_SECONDS = 10

MAX_ROWS = 1000


def extract_sql(text_response: str) -> str:
    pattern = r'```(?:sql)?\s*(.*?)\s*```'
    match = re.search(pattern, text_response, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return text_response.strip()


def fix_group_by_aliases(sql: str) -> str:
    """
    Fix PostgreSQL GROUP BY alias issues by replacing aliases with positional
    references. PostgreSQL doesn't allow column aliases in GROUP BY clauses.
    """
    try:
        parsed = sqlglot.parse_one(sql, dialect='postgres')
        if not parsed:
            return sql
        
        # Find all SELECT expressions and their aliases
        select_node = parsed.find(sqlglot.exp.Select)
        if not select_node:
            return sql
        
        # Build mapping of alias -> position (1-indexed)
        alias_to_position = {}
        position = 1
        for expr in select_node.expressions:
            if hasattr(expr, 'alias') and expr.alias:
                alias_to_position[expr.alias.lower()] = position
            position += 1
        
        if not alias_to_position:
            return sql
        
        # Find GROUP BY clause
        group_by = parsed.find(sqlglot.exp.Group)
        if not group_by:
            return sql
        
        # Check if any GROUP BY items are aliases that need fixing
        modified = False
        for i, expr in enumerate(group_by.expressions):
            expr_str = expr.sql().lower().strip()
            # Check if this is a simple identifier matching an alias
            if expr_str in alias_to_position:
                # Replace with positional reference using sqlglot Literal
                pos = alias_to_position[expr_str]
                group_by.expressions[i] = sqlglot.exp.Literal.number(pos)
                modified = True
                logger.info("[SQL FIX] Replaced GROUP BY alias '%s' with position %d", expr_str, pos)
        
        if modified:
            fixed_sql = parsed.sql(dialect='postgres')
            logger.info("[SQL FIX] Fixed SQL generated")
            return fixed_sql
        
        return sql
    except Exception as e:
        logger.warning("[SQL FIX] Could not auto-fix GROUP BY: %s", e)
        return sql


def validate_sql(sql: str) -> str:
    sql = extract_sql(sql)
    
    # Auto-fix common GROUP BY alias issues
    sql = fix_group_by_aliases(sql)
    
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
            raise SQLExecutionError("Query timed out. Please try a simpler question.")
        
        raise SQLExecutionError(f"Query execution failed: {error_msg}")
