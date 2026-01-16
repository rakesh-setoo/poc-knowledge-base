import json
import time
from sqlalchemy import text

from app.db import engine
from app.core.llm import llm_call
from app.logging import logger
from app.services.cache import get_cached_table_info, set_cached_table_info


def get_table_info(table_name: str) -> dict:
    start = time.time()
    cached = get_cached_table_info(table_name)
    if cached:
        logger.info(f"[CACHE] Table info for '{table_name}' served from Redis in {(time.time() - start)*1000:.1f}ms")
        return cached
    
    start = time.time()
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
    
    table_info = {
        "column_types": column_types, 
        "sample_data": sample_rows,
        "distinct_values": distinct_values
    }
    
    db_time = (time.time() - start) * 1000
    logger.info(f"[DB] Table info for '{table_name}' fetched from DB in {db_time:.1f}ms")
    
    set_cached_table_info(table_name, table_info)
    
    return table_info


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


def build_sql_prompt(question: str, table: str, table_info: dict, history_context: str = "") -> str:
    distinct_section = ""
    if table_info.get('distinct_values'):
        distinct_section = f"\nKey column values: {json.dumps(table_info['distinct_values'])}"
    
    column_types = table_info['column_types']
    columns_formatted = ", ".join([f"{col} ({dtype})" for col, dtype in column_types.items()])
    
    context_section = ""
    if history_context:
        context_section = f"""
CONVERSATION CONTEXT (use this to understand references like "the 4th one", "that customer", etc.):
{history_context}
"""
    
    return f"""You are a PostgreSQL expert. Generate an accurate SQL query for this question.

TABLE: {table}
COLUMNS (with types): {columns_formatted}
SAMPLE DATA: {json.dumps(table_info['sample_data'][:5], default=str)}{distinct_section}
{context_section}

UNDERSTAND THE USER'S INTENT FIRST:
- "distribution" / "breakdown" / "split" → User wants PERCENTAGES, not just values!
- "trend" / "over time" / "monthly" → User wants TIME SERIES data with proper date ordering
- "top N" / "best" / "highest" → User wants RANKED list with LIMIT
- "compare" / "vs" / "versus" → User wants SIDE BY SIDE comparison
- "total" / "sum" / "overall" → User wants AGGREGATED single value
- "list" / "show all" / "details" → User wants FULL DATA, not aggregated
- "average" / "mean" → User wants AVG() calculation
- "count" / "how many" → User wants COUNT() or COUNT(DISTINCT)
- "growth" / "change" → User wants DIFFERENCE or percentage change between periods

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

4. PERCENTAGE/DISTRIBUTION ("% of total", "share", "distribution", "breakdown", "split"):
   CRITICAL: When user asks for "distribution" or "breakdown", ALWAYS calculate percentages!
   SELECT entity, SUM(metric) as value,
          ROUND((100.0 * SUM(metric) / (SELECT SUM(metric) FROM table WHERE fiscal_year = 'FY 2025-26'))::numeric, 2) as percentage
   FROM table WHERE fiscal_year = 'FY 2025-26' GROUP BY entity ORDER BY percentage DESC
   
   Example: "sales distribution by region" should return:
   - region, total_sales, percentage (NOT just region and total_sales!)
   - The percentage column is MANDATORY for distribution/breakdown queries

5. FILTERING ("in region X", "where"):
   Use ILIKE '%value%' for text columns, = for exact matches

6. AGGREGATION ("total", "sum", "average", "count"):
   Use SUM(), AVG(), COUNT(), MIN(), MAX() with GROUP BY
   IMPORTANT: For numeric columns, use them directly - no casting needed!
   Example: AVG(numeric_column), not NULLIF(column,'')::numeric

7. COUNTING DISTINCT RECORDS ("how many invoices", "number of orders", "count of transactions"):
   CRITICAL: When counting invoices/orders/transactions/records from a data table:
   - ALWAYS use COUNT(DISTINCT id_column) if an ID column exists (like invoice_id, invoice_no, order_id)
   - COUNT(*) counts ROWS (line items), which inflates the count!
   - The ID column tells you unique records vs line items
   Example: SELECT EXTRACT(MONTH FROM date_col) as month, COUNT(DISTINCT invoice_id) as invoices
            FROM table GROUP BY 1 ORDER BY 1

8. TREND ("by month", "over time", "monthly sales"):
   CRITICAL for correct chart ordering:
   - ALWAYS ORDER BY the actual date column or date expression, NOT by the formatted text label!
   - TO_CHAR('Month YYYY') sorts ALPHABETICALLY (April, June, May) - WRONG!
   - Use: ORDER BY date_column or ORDER BY EXTRACT(MONTH FROM date_column)
   
   Example for monthly trend with correct ordering:
   SELECT TO_CHAR(date_col, 'Month YYYY') as month_year, SUM(amount) as total
   FROM table 
   WHERE date_col >= '2025-04-01'
   GROUP BY TO_CHAR(date_col, 'Month YYYY'), EXTRACT(MONTH FROM date_col)
   ORDER BY EXTRACT(MONTH FROM date_col)  -- This ensures chronological order!
   
   Or use the date directly:
   SELECT DATE_TRUNC('month', date_col) as month, SUM(amount) as total
   FROM table GROUP BY 1 ORDER BY 1

   WARNING for TEXT month columns (like 'April 2025'):
   - Do NOT use TO_DATE with invalid formats like 'MM.Mon' - this causes errors!
   - If a 'month' column contains TEXT (not date), and you need ordering:
     * Option 1: Use the actual date column (invoice_date, date, etc.) for aggregation instead
     * Option 2: Just GROUP BY the text month without complex ordering
   - Example: SELECT month, SUM(sales) FROM table GROUP BY month
   - Let the frontend handle display order, or use a simple GROUP BY without risky TO_DATE

9. FOLLOW-UP REFERENCES ("the 4th one", "that customer", "details about X"):
   CRITICAL: If the user refers to a numbered item from previous conversation:
   - Look for the EXACT numbered position in the CONVERSATION CONTEXT above
   - Parse the format: "N. [emoji] **NAME**" -> extract NAME for position N
   - Example context: "1. 🏆 UNOMINDA... 2. 🥈 NAPINO... 3. 🥉 Fiem... 4. XOLO INTERNATIONAL..."
   - If user asks "the 4th one" -> find "4. XOLO" -> generate: WHERE customer ILIKE '%XOLO%'
   - DO NOT guess or use wrong position. Count carefully from 1.

IMPORTANT PostgreSQL Rules:
- ROUND with decimals MUST cast to numeric: ROUND(value::numeric, 2) NOT ROUND(value, 2)
- For already numeric columns, just use: ROUND(AVG(column)::numeric, 2)
- Do NOT use NULLIF or empty string checks on numeric columns - they already handle NULL properly
- Only use NULLIF for TEXT columns that might have empty strings
- GROUP BY alias restriction (CRITICAL):
  * PostgreSQL does NOT allow column aliases in GROUP BY clauses!
  * WRONG: SELECT DATE_TRUNC('month', date_col) AS month ... GROUP BY month
  * CORRECT: SELECT DATE_TRUNC('month', date_col) AS month ... GROUP BY DATE_TRUNC('month', date_col)
  * Or use positional reference: GROUP BY 1 (refers to first SELECT column)
  * ALWAYS repeat the full expression or use positional number, NEVER the alias!

FISCAL YEAR CONTEXT:
- Current fiscal year is 'FY 2025-26' (April 2025 - March 2026)
- If the question doesn't specify a time range or fiscal year, ALWAYS filter to current fiscal year
- Example: "top 10 customers" means "top 10 customers in current fiscal year"
- Add WHERE fiscal_year = 'FY 2025-26' unless user explicitly asks for "all time" or a different period

QUESTION: {question}

OUTPUT: Only the SQL query, nothing else."""

# Default system prompt for AI responses
DEFAULT_SYSTEM_PROMPT = """You are an expert Business Analyst Assistant. Your goal is to transform raw data into a clear, "Human-Readable" Executive Summary.

### 🧠 RESPONSE STRATEGY (ADAPT TO DATA TYPE)

1.  **If RANKING/LEADERS** ("Top regions", "Best managers"):
    -   **ALWAYS use explicit numbers** (1., 2., 3.) alongside emojis for follow-up reference.
    -   Format: `1. 🏆 **Name** — ₹X Cr`
    -   Example:
        ```
        1. 🏆 **UNOMINDA** — ₹38.85 Cr
        2. 🥈 **NAPINO** — ₹19.49 Cr  
        3. 🥉 **Fiem Industries** — ₹18.32 Cr
        4. XOLO INTERNATIONAL — ₹12.55 Cr
        ```
    -   This ensures "the 3rd one" clearly refers to item #3.

2.  **If COMPARISON** ("North vs South", "Product A vs B"):
    -   Use a **Side-by-Side** narrative.
    -   Explicitly state the difference: "Product A is **20% higher** than B."
    -   Use emojis like 🆚 or ⚖️.

3.  **If TREND/TIME** ("Sales over time", "Growth"):
    -   Describe the **Trajectory**: "📈 Steady growth," "📉 Sharp decline."
    -   Highlight the **Peak** and **Low** points.

4.  **If BREAKDOWN** ("Sales by Region", "Category wise"):
    -   **GROUP DATA** (Critical): Never print a flat list. Group by the main category.
    -   Header format: `🔹 [Category Name]`

---

### 🎨 VISUAL FORMATTING RULES

1.  **Headers**: Use bold headers with emojis for sections.
    -   `📊 **Sales Overview**`
    -   `💡 **Key Insights**`

2.  **Numbers**:
    -   **⚠️ CRITICAL - CURRENCY CONVERSION - READ VERY CAREFULLY ⚠️**:
        -   **1 Crore = 10,000,000** (1 followed by SEVEN zeros)
        -   **STEP 1**: Take the raw number from data
        -   **STEP 2**: Divide by 10,000,000 (move decimal point 7 places LEFT)
        -   **STEP 3**: Round to 2 decimal places
        
        **EXAMPLE WITH REAL DATA:**
        - Raw value: 1,051,504,739 (about 1 billion)
        - WRONG: 1,051,504,739 / 1,000,000 = 1051.50 ❌ (This is WRONG!)
        - RIGHT: 1,051,504,739 / 10,000,000 = **105.15 Cr** ✅
        
        **ANOTHER EXAMPLE:**
        - Raw value: 979,609,496
        - WRONG: 979,609,496 / 1,000,000 = 979.61 ❌
        - RIGHT: 979,609,496 / 10,000,000 = **97.96 Cr** ✅
        
        **RULE: If your Cr value has 3+ digits before decimal (e.g., 105X.XX), you made an error!**
        
        **Quick Check:** A 10-digit number should give ~100 Cr, NOT ~1000 Cr!
    -   **Formatting**: Use ₹X.XX Cr for crores
    -   **Bold** all critical numbers so they stand out.
    -   **Decimals**: Keep to 2 decimal places.

3.  **Lists vs Tables**:
    -   Use **Bullet Lists** for hierarchical data (Region -> Manager).
    -   Use **Markdown Tables** ONLY for small, high-density summaries (max 5 rows).
    -   *Never* create a table with >10 rows. Breaks it into groups.

4.  **Insights (The "So What?"):**
    -   Explain *why* a number matters if possible (e.g., "This represents a 15% share").

---

### ❌ ANTI-PATTERNS (DO NOT DO)
-   🚫 Do not just dump the JSON rows.
-   🚫 Do not use technical column names (use "Sales", not "SUM(sales_amount)").
-   🚫 NEVER say "Based on the query", "In the results", "The SQL returned". proper nouns. Just state the facts.
"""


def build_answer_prompt(
    question: str, 
    result_data: list, 
    history_context: str = "", 
    custom_prompt: str = None,
    viz_type: str = None
) -> str:
    sample = result_data[:10]
    
    # Find a large numeric value from data to show as example
    conversion_hint = ""
    for row in sample[:3]:
        for key, value in row.items():
            if isinstance(value, (int, float)) or hasattr(value, '__float__'):
                try:
                    num = float(value)
                    if num >= 100_000_000:  # >= 10 Cr (100 million)
                        correct_cr = num / 10_000_000
                        conversion_hint = f"""
**DATA CONVERSION EXAMPLE FROM YOUR DATA:**
- Raw value in data: {num:,.0f}
- CORRECT: {num:,.0f} ÷ 10,000,000 = **{correct_cr:.2f} Cr** ✅
- WRONG: {num:,.0f} ÷ 1,000,000 = {num/1_000_000:.2f} Cr ❌ (THIS IS 10x TOO HIGH!)
"""
                        break
                except (ValueError, TypeError):
                    pass
        if conversion_hint:
            break
    
    # Always use default prompt, add custom instructions on top if provided
    if custom_prompt:
        system_instructions = f"{DEFAULT_SYSTEM_PROMPT}\n\n### 📝 ADDITIONAL USER INSTRUCTIONS:\n{custom_prompt}"
    else:
        system_instructions = DEFAULT_SYSTEM_PROMPT
    
    # Add visualization context to prevent duplicate data display
    viz_instruction = ""
    if viz_type == "table":
        viz_instruction = """
### ⚠️ IMPORTANT: DATA IS ALREADY DISPLAYED AS A TABLE
The data is already shown as a visual table above. Your response should be a well-formatted SUMMARY only.

**FORMAT YOUR RESPONSE LIKE THIS:**
📊 **[Title describing what this data shows]**

🏆 **Key Highlights:**
- **[Top performer]** leads with **₹XX.XX Cr**
- [Second place] follows with ₹XX.XX Cr
- Total of [N] entries shown

💡 **Insight:** [Two sentence insight about the data pattern or significance]

**RULES:**
- Use emojis to make it visually appealing (📊, 🏆, 💡, 📈, etc.)
- Bold the important numbers and names
- Keep it to 3-5 lines maximum
- Do NOT create markdown tables or list all rows
- Do NOT repeat what's visible in the table
"""
    elif viz_type in ("bar", "line", "pie"):
        viz_instruction = f"""
### ⚠️ IMPORTANT: DATA IS ALREADY DISPLAYED AS A {viz_type.upper()} CHART
The data is already shown as a visual {viz_type} chart above. Your response should be a well-formatted SUMMARY only.

**FORMAT YOUR RESPONSE LIKE THIS:**
📊 **[Title describing what the chart shows]**

🔍 **Key Observations:**
- **[Most significant finding]** with value ₹XX.XX Cr
- [Trend or pattern observed]
- [Comparison or contrast if applicable]

💡 **Insight:** [two sentence explaining the business significance]

**RULES:**
- Use emojis to make it visually appealing (📊, 📈, 📉, 🔥, 💡, etc.)  
- Bold the key numbers and names
- Focus on trends, patterns, and insights
- Keep it to 3-5 lines maximum
- Do NOT create tables or list all data points
"""
    
    return f"""Answer the question in natural language based on the query results below.
{history_context}
Question: {question}

Data ({len(result_data)} rows):
{json.dumps(sample, default=str)}
{conversion_hint}
{viz_instruction}
{system_instructions}"""


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

