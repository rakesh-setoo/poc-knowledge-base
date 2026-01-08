import json
from sqlalchemy import text

from app.db import engine
from app.core.llm import llm_call
from app.logging import logger


def get_table_info(table_name: str) -> dict:
    with engine.connect() as conn:
        type_query = text("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = :table_name
            ORDER BY ordinal_position
        """)
        type_result = conn.execute(type_query, {"table_name": table_name})
        column_types = {row[0]: row[1] for row in type_result}
        
        sample_query = text(f'SELECT * FROM "{table_name}" LIMIT 5')
        sample_result = conn.execute(sample_query)
        sample_rows = [dict(row._mapping) for row in sample_result]
        
        distinct_values = _get_distinct_values(conn, table_name, column_types)
        
    return {
        "column_types": column_types, 
        "sample_data": sample_rows,
        "distinct_values": distinct_values
    }


def _get_distinct_values(conn, table_name: str, column_types: dict) -> dict:
    distinct_values = {}
    category_keywords = ['month', 'date', 'year', 'category', 'type', 'status', 'region', 'city']
    
    for col_name, col_type in column_types.items():
        if col_type in ('text', 'character varying', 'varchar'):
            if any(keyword in col_name.lower() for keyword in category_keywords):
                distinct_query = text(
                    f'SELECT DISTINCT "{col_name}" FROM "{table_name}" '
                    f'ORDER BY "{col_name}" LIMIT 20'
                )
                distinct_result = conn.execute(distinct_query)
                distinct_values[col_name] = [
                    row[0] for row in distinct_result if row[0] is not None
                ]
    
    return distinct_values


def build_sql_prompt(question: str, table: str, table_info: dict) -> str:
    distinct_section = ""
    if table_info.get('distinct_values'):
        distinct_section = f"\nKey column values: {json.dumps(table_info['distinct_values'])}"
    
    # Format columns with their types for better SQL generation
    column_types = table_info['column_types']
    columns_formatted = ", ".join([f"{col} ({dtype})" for col, dtype in column_types.items()])
    
    return f"""You are a PostgreSQL expert. Generate an accurate SQL query for this question.

TABLE: {table}
COLUMNS (with types): {columns_formatted}
SAMPLE DATA: {json.dumps(table_info['sample_data'][:5], default=str)}{distinct_section}

QUERY PATTERNS (use the appropriate pattern):

1. RANKING ("what rank is X", "position of X"):
   CRITICAL: Calculate rank for ALL rows first, then filter OUTSIDE the CTE!
   WITH ranked AS (
     SELECT entity, SUM(metric) as total, 
            ROW_NUMBER() OVER (ORDER BY SUM(metric) DESC) as rank
     FROM table 
     GROUP BY entity  -- NO WHERE clause here!
   ) 
   SELECT * FROM ranked WHERE entity ILIKE '%search%'  -- Filter AFTER ranking!

2. TOP N ("top 5", "best 10"):
   SELECT entity, SUM(metric) as total FROM table 
   GROUP BY entity ORDER BY total DESC LIMIT N

3. COMPARISON ("X vs Y", "compare"):
   SELECT entity, SUM(metric) as total FROM table 
   WHERE entity ILIKE '%X%' OR entity ILIKE '%Y%' GROUP BY entity

4. PERCENTAGE ("% of total", "share"):
   SELECT entity, SUM(metric) as value,
          ROUND((100.0 * SUM(metric) / (SELECT SUM(metric) FROM table))::numeric, 2) as percentage
   FROM table GROUP BY entity

5. FILTERING ("in region X", "where"):
   Use ILIKE '%value%' for text columns, = for exact matches

6. AGGREGATION ("total", "sum", "average", "count"):
   Use SUM(), AVG(), COUNT(), MIN(), MAX() with GROUP BY
   IMPORTANT: For numeric columns, use them directly - no casting needed!
   Example: AVG(numeric_column), not NULLIF(column,'')::numeric

7. TREND ("by month", "over time"):
   GROUP BY time_column ORDER BY time_column

IMPORTANT PostgreSQL Rules:
- ROUND with decimals MUST cast to numeric: ROUND(value::numeric, 2) NOT ROUND(value, 2)
- For already numeric columns, just use: ROUND(AVG(column)::numeric, 2)
- Do NOT use NULLIF or empty string checks on numeric columns - they already handle NULL properly
- Only use NULLIF for TEXT columns that might have empty strings

QUESTION: {question}

OUTPUT: Only the SQL query, nothing else."""


def build_answer_prompt(question: str, result_data: list) -> str:
    """Build the prompt for answer generation."""
    sample = result_data[:10]  # Reduced from 20 for faster processing
    
    # return f"""Answer concisely based on the data below.
    return f"""Answer the question in natural language based on the query results below.

Question: {question}

Data ({len(result_data)} rows):
{json.dumps(sample, default=str)}   

    RESPONSE GUIDELINES:
1. Start with a brief, friendly sentence answering the question directly
2. Present data as a simple numbered or bulleted list - DO NOT use markdown tables
3. Each list item should be clear and readable, like: "April: 87.23 days"
4. Format values for readability:
   - Currency/Sales/Revenue: Use Indian format - ₹38.85 Cr (crores), ₹19.49 L (lakhs)
   - Round decimals to 2 places
5. Keep the response concise and easy to scan
6. Do not use markdown table syntax (no | or --- characters)"""

# Rules:
# - Start with 1 sentence answering the question
# - List key data points briefly
# - Use ₹ Cr/L for currency (Indian format)
# - No markdown tables"""


def select_table(question: str, datasets: list, dataset_id: int = None) -> str:
    if dataset_id is not None:
        dataset = next((d for d in datasets if d.get("id") == dataset_id), None)
        if not dataset:
            logger.error(f"Dataset with ID {dataset_id} not found in {len(datasets)} datasets")
            raise ValueError(f"Dataset with ID {dataset_id} not found.")
        logger.info(f"Selected table '{dataset['table_name']}' for dataset_id {dataset_id}")
        return dataset["table_name"]
    
    if len(datasets) == 1:
        logger.info(f"Auto-selected single table: {datasets[0]['table_name']}")
        return datasets[0]["table_name"]
    
    logger.info("Auto-detecting table using LLM...")
    selected = _select_schema_with_llm(question, datasets)
    logger.info(f"LLM selected table: {selected}")
    return selected


def _select_schema_with_llm(question: str, datasets: list) -> str:
    metadata_summary = [
        {
            "table_name": d["table_name"],
            "columns": d["columns"]
        } for d in datasets
    ]
    
    prompt = f"""You are a data analyst. Select the best table for this question.

Available Tables:
{json.dumps(metadata_summary, indent=2)}

Question: "{question}"

Return ONLY the table_name in JSON format: {{"table_name": "..."}}"""

    try:
        response = llm_call(prompt)
        clean_response = response.replace("```json", "").replace("```", "").strip()
        result = json.loads(clean_response)
        return result["table_name"]
    except Exception as e:
        logger.error(f"Schema selection failed: {e}")
        return datasets[0]["table_name"]

