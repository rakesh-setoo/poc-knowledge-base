from openai import OpenAI, OpenAIError
from app.config import settings
from app.exceptions import LLMError


client = OpenAI(api_key=settings.openai_api_key)


def llm_call(
    prompt: str, 
    temperature: float = 0, 
    max_tokens: int = 2000,
    system_prompt: str = "You are a helpful data assistant"
) -> str:
    """LLM call using streaming, returns the complete response."""
    try:
        stream = client.chat.completions.create(
            model=settings.openai_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ],
            temperature=temperature,
            max_completion_tokens=max_tokens,
            stream=True,
            timeout=60
        )
        
        result = []
        for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                result.append(chunk.choices[0].delta.content)
        
        return "".join(result).strip()
                
    except OpenAIError as e:
        raise LLMError(f"LLM call failed: {str(e)}")
    except Exception as e:
        raise LLMError(f"Unexpected error: {str(e)}")