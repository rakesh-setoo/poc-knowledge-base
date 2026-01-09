from fastapi import APIRouter, Body, HTTPException

from app.services.settings import get_global_system_prompt, set_global_system_prompt
from app.logging import logger


router = APIRouter(prefix="/settings", tags=["Settings"])


@router.get("/global-prompt")
def get_global_prompt():
    prompt = get_global_system_prompt()
    return {"global_prompt": prompt or ""}


@router.put("/global-prompt")
def update_global_prompt(prompt: str = Body(..., embed=True)):
    success = set_global_system_prompt(prompt)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to update global prompt")
    return {"message": "Global prompt updated", "global_prompt": prompt}
