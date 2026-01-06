"""
Schema selector for multi-table queries.
Determines the best table and columns to use for a given question.
"""
import json
from app.llm import llm_call
from app.exceptions import LLMError


def select_schema(question: str, metadata: list[dict]) -> dict:
    """
    Select the appropriate schema (table and columns) for a question.
    
    Args:
        question: Natural language question
        metadata: List of available dataset metadata
        
    Returns:
        Dict with table_name and columns
        
    Raises:
        LLMError: If schema selection fails
    """
    prompt = f"""
You are a data analyst.

Available tables:
{json.dumps(metadata, indent=2)}

User question:
"{question}"

Return ONLY JSON:
{{
  "table_name": "...",
  "columns": ["col1", "col2"]
}}
"""
    
    try:
        response = llm_call(prompt)
        return json.loads(response)
        
    except json.JSONDecodeError:
        # Fallback to first available table
        if metadata:
            return {
                "table_name": metadata[0]["table_name"],
                "columns": metadata[0]["columns"]
            }
        raise LLMError("Failed to select schema and no tables available")
