/*
 * Query Module
 * Handles question asking with streaming and integrates with chat system
 */

/**
 * Ask a question about the data (with streaming response)
 */
async function askQuestion() {
  const input = document.getElementById('questionInput');
  const question = input.value.trim();
  const datasetSelect = document.getElementById('datasetSelect');
  const selectedValue = datasetSelect.value;

  // Validation
  if (selectedValue === '' || selectedValue === null) {
    showToast('Please select a dataset first', 'error');
    return;
  }

  if (!question) {
    showToast('Please enter a question', 'error');
    return;
  }

  const selectedDatasetId = parseInt(selectedValue);
  const loading = document.getElementById('loading');
  const askBtn = document.getElementById('askBtn');

  // Clear input immediately
  input.value = '';

  // Add user message to chat
  addMessageToChat('user', question);

  // Show loading state
  loading.classList.add('show');
  askBtn.disabled = true;

  // Variables to hold assistant message element (created after metadata)
  let assistantMessage = null;
  let bubbleP = null;

  try {
    const response = await fetch(`${API_BASE}/ask-stream`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        question: question,
        dataset_id: selectedDatasetId,
        chat_id: currentChatId
      })
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let streamedAnswer = '';
    let metadataReceived = false;
    let resultData = null;
    let buffer = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });

      const lines = buffer.split('\n');
      buffer = lines.pop() || '';

      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;

        try {
          const data = JSON.parse(line.slice(6));

          // Handle error
          if (data.error) {
            loading.classList.remove('show');
            // Create error message
            const errorMsg = addMessageToChat('assistant', '');
            errorMsg.querySelector('.message-bubble p').innerHTML = `<span class="error-text">⚠️ ${data.error}</span>`;
            askBtn.disabled = false;
            return;
          }

          // Handle metadata - create assistant message and visualization FIRST
          if (data.type === 'metadata') {
            metadataReceived = true;
            resultData = data;

            // Update currentChatId if this is a new chat
            if (data.chat_id && !currentChatId) {
              currentChatId = data.chat_id;
            }

            loading.classList.remove('show');

            // Create assistant message with structure for viz FIRST, then text
            assistantMessage = document.createElement('div');
            assistantMessage.className = 'message assistant';
            assistantMessage.innerHTML = `
              <div class="message-avatar">🤖</div>
              <div class="message-content">
                <div class="message-bubble">
                  <div class="viz-placeholder"></div>
                  <p class="answer-text"></p>
                </div>
              </div>
            `;

            // Show messages container
            document.getElementById('welcomeScreen').style.display = 'none';
            const container = document.getElementById('messagesContainer');
            container.style.display = 'flex';
            container.appendChild(assistantMessage);

            // Get reference to text paragraph
            bubbleP = assistantMessage.querySelector('.answer-text');

            // Render visualization IMMEDIATELY (ChatGPT style - viz appears first)
            if (resultData.data && resultData.data.length > 0) {
              const vizType = resultData.viz_type || 'table';
              const columns = resultData.columns;
              const vizData = resultData.data;
              const vizPlaceholder = assistantMessage.querySelector('.viz-placeholder');

              if (shouldRenderChart(vizType, vizData)) {
                // Create chart container directly (no toggle tabs)
                const vizId = `viz-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
                vizPlaceholder.innerHTML = `
                  <div class="viz-wrapper viz-inline">
                    <div class="viz-chart-container" id="${vizId}"></div>
                  </div>
                `;
                // Render chart after DOM update
                setTimeout(() => {
                  renderVisualization(vizId, vizType, columns, vizData);
                }, 50);
              } else {
                // Render table directly
                vizPlaceholder.innerHTML = `
                  <div class="message-data-table viz-inline">
                    ${renderDataTable(columns, vizData)}
                  </div>
                `;
              }
            }

            scrollToBottom();
          }

          // Handle streaming tokens
          if (data.type === 'token' && metadataReceived) {
            streamedAnswer += data.content;
            bubbleP.innerHTML = formatAnswer(streamedAnswer) + '<span class="streaming-cursor">▊</span>';
            scrollToBottom();
          }

          // Handle completion
          if (data.type === 'done') {
            // Finalize message (remove cursor)
            bubbleP.innerHTML = formatAnswer(streamedAnswer);

            // Reload chat history to show new/updated chat
            loadChatHistory();
            scrollToBottom();
          }
        } catch (e) {
          console.debug('JSON parse error:', e.message);
        }
      }
    }

  } catch (error) {
    loading.classList.remove('show');
    console.error('Connection error:', error);
    bubbleP.innerHTML = '<span class="error-text">⚠️ Something went wrong. Please try again.</span>';
  }

  askBtn.disabled = false;
}

/**
 * Render data table HTML
 */
function renderDataTable(columns, data) {
  if (!columns || !data || data.length === 0) return '';

  // Limit display to 10 rows with pagination
  const displayData = data.slice(0, 10);
  const hasMore = data.length > 10;

  // Wrap in details/summary for default collapsed state
  let html = `
  <details class="data-details">
    <summary class="data-summary">
      <span class="icon">📋</span>
      <span>View Table</span>
    </summary>
    <div class="data-table-wrapper">
      <table class="data-table"><thead><tr>`;

  columns.forEach(col => {
    html += `<th>${escapeHtml(col)}</th>`;
  });

  html += '</tr></thead><tbody>';

  displayData.forEach(row => {
    html += '<tr>';
    columns.forEach(col => {
      const value = row[col];
      html += `<td>${formatCellValue(value)}</td>`;
    });
    html += '</tr>';
  });

  html += '</tbody></table>';

  if (hasMore) {
    html += `<div class="table-more">... and ${data.length - 10} more rows</div>`;
  }

  html += '</div></details>';

  return html;
}

/**
 * Format cell value for display
 */
function formatCellValue(value) {
  if (value === null || value === undefined) return '<span class="null-value">-</span>';
  if (typeof value === 'number') {
    // Format numbers with commas
    return value.toLocaleString();
  }
  return escapeHtml(String(value));
}

/**
 * Format answer text with markdown-like formatting
 */
function formatAnswer(text) {
  if (!text) return '';

  return text
    // Bold
    .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
    // Italic
    .replace(/\*(.*?)\*/g, '<em>$1</em>')
    // Line breaks
    .replace(/\n/g, '<br>')
    // Numbered lists
    .replace(/^(\d+)\.\s/gm, '<span class="list-number">$1.</span> ');
}

/**
 * Render error message
 */
function renderError(container, data) {
  container.innerHTML = `
    <div class="result-error">
      <strong>⚠️ Oops!</strong> ${data.error}
    </div>
  `;
}

// Legacy functions kept for compatibility
function renderStreamingResults(container, data, answer) {
  // Now handled by chat message system
}

function updateStreamingAnswer(answer) {
  // Now handled inline in askQuestion
}

function renderResults(container, data) {
  // Now handled by chat message system
}
