from openai import OpenAI, OpenAIError
from app.core.config import settings
from app.logging.exceptions import LLMError
from typing import Generator


client = OpenAI(api_key=settings.openai_api_key)


def llm_call(
    prompt: str, 
    temperature: float = 0, 
    max_tokens: int = 500,
    system_prompt: str = "You are a helpful data assistant"
) -> str:
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


def llm_call_stream(
    prompt: str, 
    temperature: float = 0, 
    max_tokens: int = 500,
    system_prompt: str = "You are a helpful data assistant"
) -> Generator[str, None, None]:
    """Generator that yields tokens as they arrive from the LLM."""
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
        
        for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content
                
    except OpenAIError as e:
        raise LLMError(f"LLM stream failed: {str(e)}")
    except Exception as e:
        raise LLMError(f"Unexpected error: {str(e)}")
