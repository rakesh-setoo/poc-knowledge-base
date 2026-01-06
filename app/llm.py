"""
LLM operations using OpenAI API.
Provides SQL generation and answer explanation with error handling.
"""
from openai import OpenAI, OpenAIError
from app.config import settings
from app.exceptions import LLMError


# Initialize OpenAI client
client = OpenAI(api_key=settings.openai_api_key)


def llm_call(prompt: str, temperature: float = 0, max_tokens: int = 2000) -> str:
    """
    Make a call to the OpenAI API.
    
    Args:
        prompt: The prompt to send
        temperature: Randomness of response (0-1)
        max_tokens: Maximum tokens in response
        
    Returns:
        The LLM response text
        
    Raises:
        LLMError: If the API call fails
    """
    try:
        response = client.chat.completions.create(
            model=settings.openai_model,
            messages=[
                {"role": "system", "content": "You are a helpful data assistant"},
                {"role": "user", "content": prompt}
            ],
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=30  # 30 second timeout for fast response
        )
        
        return response.choices[0].message.content.strip()
        
    except OpenAIError as e:
        raise LLMError(f"LLM call failed: {str(e)}")
    except Exception as e:
        raise LLMError(f"Unexpected error: {str(e)}")