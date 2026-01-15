/**
 * Chat Management Module
 * Handles chat history, switching, and message display
 */

// Current active chat
let currentChatId = null;

/**
 * Load chat history from API
 */
async function loadChatHistory() {
  try {
    const response = await fetch(`${API_BASE}/chats`);
    const data = await response.json();
    renderChatHistory(data.chats || []);
  } catch (error) {
    console.error('Failed to load chat history:', error);
  }
}

/**
 * Render chat history in sidebar
 */
function renderChatHistory(chats) {
  const container = document.getElementById('chatHistory');
  const emptyState = document.getElementById('chatHistoryEmpty');

  if (!chats || chats.length === 0) {
    emptyState.style.display = 'flex';
    return;
  }

  emptyState.style.display = 'none';

  // Clear existing items (except empty state)
  container.querySelectorAll('.chat-history-item').forEach(el => el.remove());

  chats.forEach(chat => {
    const item = document.createElement('div');
    item.className = `chat-history-item ${chat.id === currentChatId ? 'active' : ''}`;
    item.dataset.chatId = chat.id;
    item.innerHTML = `
      <span class="icon">💬</span>
      <span class="title">${escapeHtml(chat.title)}</span>
      <button class="delete-btn" onclick="deleteChat(${chat.id}, event)" title="Delete">🗑️</button>
    `;
    item.onclick = (e) => {
      if (!e.target.classList.contains('delete-btn')) {
        switchToChat(chat.id);
      }
    };
    container.insertBefore(item, emptyState);
  });
}

/**
 * Start a new chat
 */
function startNewChat() {
  currentChatId = null;
  currentCustomPrompt = null;

  // Update UI
  document.querySelectorAll('.chat-history-item').forEach(el => el.classList.remove('active'));
  document.getElementById('welcomeScreen').style.display = 'flex';
  document.getElementById('messagesContainer').style.display = 'none';
  document.getElementById('messagesContainer').innerHTML = '';
  document.getElementById('questionInput').value = '';
  document.getElementById('questionInput').focus();

  // Clear prompt indicator
  updatePromptIndicator();
}

/**
 * Switch to an existing chat
 */
async function switchToChat(chatId) {
  currentChatId = chatId;

  // Update sidebar active state
  document.querySelectorAll('.chat-history-item').forEach(el => {
    el.classList.toggle('active', el.dataset.chatId == chatId);
  });

  // Hide welcome, show messages
  document.getElementById('welcomeScreen').style.display = 'none';
  document.getElementById('messagesContainer').style.display = 'flex';

  // Load messages
  try {
    const response = await fetch(`${API_BASE}/chats/${chatId}/messages`);
    const data = await response.json();
    renderMessages(data.messages || []);

    // Update dataset selector if chat has dataset_id
    if (data.chat && data.chat.dataset_id) {
      document.getElementById('datasetSelect').value = data.chat.dataset_id;
    }

    // Load system prompt for this chat
    currentCustomPrompt = data.chat?.system_prompt || null;
    updatePromptIndicator();
  } catch (error) {
    console.error('Failed to load chat messages:', error);
  }
}

/**
 * Render messages in the chat area
 */
function renderMessages(messages) {
  const container = document.getElementById('messagesContainer');
  container.innerHTML = '';

  messages.forEach(msg => {
    const messageEl = createMessageElement(msg.role, msg.content, msg.metadata);
    container.appendChild(messageEl);
  });

  // Scroll to bottom
  scrollToBottom();
}

/**
 * Create a message element
 */
function createMessageElement(role, content, metadata = null) {
  const div = document.createElement('div');
  div.className = `message ${role}`;

  const avatar = role === 'user' ? '👤' : '🤖';

  // For assistant messages with metadata, structure for viz FIRST
  if (role === 'assistant' && metadata && metadata.data && metadata.data.length > 0) {
    div.innerHTML = `
      <div class="message-avatar">${avatar}</div>
      <div class="message-content">
        <div class="message-bubble">
          <div class="viz-placeholder"></div>
          <p class="answer-text">${formatAnswer(content)}</p>
        </div>
      </div>
    `;

    // Render visualization in placeholder
    const vizType = metadata.viz_type;
    const columns = metadata.columns;
    const data = metadata.data;

    setTimeout(() => {
      const vizPlaceholder = div.querySelector('.viz-placeholder');

      if (shouldRenderChart(vizType, data)) {
        const vizId = `viz-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
        vizPlaceholder.innerHTML = `
          <div class="viz-wrapper viz-inline">
            <div class="viz-chart-container" id="${vizId}"></div>
          </div>
        `;
        setTimeout(() => {
          renderVisualization(vizId, vizType, columns, data);
        }, 50);
      } else {
        vizPlaceholder.innerHTML = `
          <div class="message-data-table viz-inline">
            ${renderDataTable(columns, data)}
          </div>
        `;
      }
    }, 10);
  } else {
    // Standard message (user or assistant without data)
    div.innerHTML = `
      <div class="message-avatar">${avatar}</div>
      <div class="message-content">
        <div class="message-bubble">
          <p>${role === 'assistant' ? formatAnswer(content) : escapeHtml(content)}</p>
        </div>
      </div>
    `;
  }

  return div;
}

/**
 * Add a message to the current chat display
 */
function addMessageToChat(role, content, metadata = null) {
  const container = document.getElementById('messagesContainer');

  // Show messages container if hidden
  document.getElementById('welcomeScreen').style.display = 'none';
  container.style.display = 'flex';

  const messageEl = createMessageElement(role, content, metadata);
  container.appendChild(messageEl);

  scrollToBottom();
  return messageEl;
}

/**
 * Update the last assistant message (for streaming)
 */
function updateLastAssistantMessage(content) {
  const container = document.getElementById('messagesContainer');
  const lastMessage = container.querySelector('.message.assistant:last-child');

  if (lastMessage) {
    const bubble = lastMessage.querySelector('.message-bubble p');
    if (bubble) {
      bubble.innerHTML = formatAnswer(content) + '<span class="streaming-cursor">▊</span>';
    }
  }

  scrollToBottom();
}

/**
 * Finalize the last assistant message (remove cursor)
 */
function finalizeLastAssistantMessage(content) {
  const container = document.getElementById('messagesContainer');
  const lastMessage = container.querySelector('.message.assistant:last-child');

  if (lastMessage) {
    const bubble = lastMessage.querySelector('.message-bubble p');
    if (bubble) {
      bubble.innerHTML = formatAnswer(content);
    }
  }
}

/**
 * Delete a chat
 */
async function deleteChat(chatId, event) {
  event.stopPropagation();

  if (!confirm('Delete this conversation?')) return;

  try {
    await fetch(`${API_BASE}/chats/${chatId}`, { method: 'DELETE' });

    // If deleted current chat, start new chat
    if (chatId === currentChatId) {
      startNewChat();
    }

    // Reload chat history
    loadChatHistory();
    showToast('Chat deleted', 'success');
  } catch (error) {
    console.error('Failed to delete chat:', error);
    showToast('Failed to delete chat', 'error');
  }
}

/**
 * Use a suggestion chip
 */
function useSuggestion(text) {
  document.getElementById('questionInput').value = text;
  askQuestion();
}

/**
 * Toggle upload panel
 */
function toggleUploadPanel() {
  const panel = document.getElementById('uploadPanel');
  panel.style.display = panel.style.display === 'none' ? 'flex' : 'none';
}

/**
 * Scroll chat to bottom
 */
function scrollToBottom() {
  const container = document.getElementById('chatMessages');
  container.scrollTop = container.scrollHeight;
}

/**
 * Escape HTML to prevent XSS
 */
function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

// Current custom prompt for the chat and global
let currentCustomPrompt = null;
let currentGlobalPrompt = null;
let activePromptTab = 'global'; // 'global' or 'chat'

/**
 * Toggle the custom prompt modal
 */
async function togglePromptModal() {
  const modal = document.getElementById('promptModal');
  const isVisible = modal.style.display !== 'none';

  if (isVisible) {
    modal.style.display = 'none';
  } else {
    // Load prompts
    if (!currentGlobalPrompt) {
      // Fetch global prompt if not loaded
      try {
        const response = await fetch(`${API_BASE}/settings/global-prompt`);
        const data = await response.json();
        currentGlobalPrompt = data.global_prompt || '';
      } catch (error) {
        console.error('Failed to load global prompt:', error);
      }
    }

    // Set values
    document.getElementById('globalPromptInput').value = currentGlobalPrompt || '';
    document.getElementById('customPromptInput').value = currentCustomPrompt || '';

    // Switch to active tab (default global)
    switchPromptTab(activePromptTab);

    modal.style.display = 'flex';
  }
}

/**
 * Switch prompt tab
 */
function switchPromptTab(tab) {
  activePromptTab = tab;

  // Update buttons
  document.querySelectorAll('.prompt-tab').forEach(btn => btn.classList.remove('active'));
  document.getElementById(tab === 'global' ? 'tabGlobal' : 'tabChat').classList.add('active');

  // Update content
  document.getElementById('contentGlobal').style.display = tab === 'global' ? 'block' : 'none';
  document.getElementById('contentChat').style.display = tab === 'chat' ? 'block' : 'none';
}

/**
 * Save custom prompt (handles both tabs)
 */
async function saveCustomPrompt() {
  const promises = [];

  // Save Global Prompt
  const globalPromptInput = document.getElementById('globalPromptInput');
  const globalPrompt = globalPromptInput.value.trim();

  if (globalPrompt !== currentGlobalPrompt) {
    currentGlobalPrompt = globalPrompt;
    promises.push(
      fetch(`${API_BASE}/settings/global-prompt`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt: globalPrompt })
      })
    );
  }

  // Save Chat Prompt
  const chatPromptInput = document.getElementById('customPromptInput');
  const chatPrompt = chatPromptInput.value.trim();

  // Only update chat prompt if we have an active chat
  if (currentChatId) {
    if (chatPrompt !== currentCustomPrompt) {
      currentCustomPrompt = chatPrompt || null;
      promises.push(
        fetch(`${API_BASE}/chats/${currentChatId}/system-prompt`, {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ system_prompt: chatPrompt })
        })
      );
    }
  } else {
    // No active chat, just update local state for when chat is created
    currentCustomPrompt = chatPrompt || null;
  }

  try {
    await Promise.all(promises);
    showToast('Instructions saved successfully', 'success');
    activePromptTab = 'chat'; // Remember last used tab logic could be here, or just reset

    // Update UI indicator (only for chat specific prompt)
    updatePromptIndicator();

  } catch (error) {
    console.error('Failed to save instructions:', error);
    showToast('Failed to save instructions', 'error');
  }

  // Close modal
  document.getElementById('promptModal').style.display = 'none';
}

/**
 * Clear custom prompt (only clears active tab's input)
 */
async function clearCustomPrompt() {
  if (activePromptTab === 'chat') {
    document.getElementById('customPromptInput').value = '';
    currentCustomPrompt = null;

    // If we have a current chat, clear on backend
    if (currentChatId) {
      try {
        await fetch(`${API_BASE}/chats/${currentChatId}/system-prompt`, {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ system_prompt: '' })
        });
      } catch (error) {
        console.error('Failed to clear system prompt:', error);
      }
    }
    showToast('Chat instructions cleared', 'success');

  } else {
    // Global tab
    document.getElementById('globalPromptInput').value = '';
    currentGlobalPrompt = '';

    try {
      await fetch(`${API_BASE}/settings/global-prompt`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt: '' })
      });
    } catch (error) {
      console.error('Failed to clear global prompt:', error);
    }
    showToast('Global instructions cleared', 'success');
  }

  updatePromptIndicator();
}

/**
 * Update the prompt indicator visibility (shows only if chat-specific prompt is active)
 */
function updatePromptIndicator() {
  const indicator = document.getElementById('promptIndicator');
  const plusBtn = document.getElementById('plusBtn');

  // Only show indicator for chat-specific prompts, as global applies invisibly to all
  if (currentCustomPrompt) {
    indicator.style.display = 'flex';
    plusBtn.classList.add('active');
  } else {
    indicator.style.display = 'none';
    plusBtn.classList.remove('active');
  }
}

/**
 * Load system prompt when switching chats
 */
async function loadChatSystemPrompt(chatId) {
  try {
    const response = await fetch(`${API_BASE}/chats/${chatId}`);
    const chat = await response.json();
    currentCustomPrompt = chat.system_prompt || null;
    updatePromptIndicator();
  } catch (error) {
    console.error('Failed to load system prompt:', error);
  }
}
