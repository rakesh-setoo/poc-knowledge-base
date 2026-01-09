from fastapi import APIRouter, Body, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional

from app.services.chat import (
    create_chat, get_chats, get_chat, delete_chat,
    get_messages, add_message, auto_generate_title, update_system_prompt
)
from app.logging import logger


router = APIRouter(prefix="/chats", tags=["Chats"])


@router.post("")
def create_new_chat(
    dataset_id: Optional[int] = Body(None),
    title: str = Body("New Chat"),
    system_prompt: Optional[str] = Body(None)
):
    """Create a new chat with optional custom system prompt."""
    try:
        chat = create_chat(dataset_id=dataset_id, title=title, system_prompt=system_prompt)
        return chat
    except Exception as e:
        logger.error(f"Failed to create chat: {e}")
        raise HTTPException(status_code=500, detail="Failed to create chat")


@router.get("")
def list_chats(limit: int = 50):
    chats = get_chats(limit=limit)
    return {"chats": chats, "count": len(chats)}


@router.get("/{chat_id}")
def get_single_chat(chat_id: int):
    chat = get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    return chat


@router.delete("/{chat_id}")
def delete_single_chat(chat_id: int):
    success = delete_chat(chat_id)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete chat")
    return {"message": "Chat deleted", "chat_id": chat_id}


@router.patch("/{chat_id}/system-prompt")
def update_chat_system_prompt(
    chat_id: int,
    system_prompt: str = Body(..., embed=True)
):
    chat = get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    
    success = update_system_prompt(chat_id, system_prompt)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to update system prompt")
    
    return {"message": "System prompt updated", "chat_id": chat_id}


@router.get("/{chat_id}/messages")
def get_chat_messages(chat_id: int, limit: int = 100):
    chat = get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    
    messages = get_messages(chat_id, limit=limit)
    return {"messages": messages, "count": len(messages), "chat": chat}
