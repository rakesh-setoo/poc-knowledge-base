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
              } else if (vizType === 'table') {
                // Render table only when explicitly requested
                vizPlaceholder.innerHTML = `
                  <div class="message-data-table viz-inline">
                    ${renderDataTable(columns, vizData, true)}
                  </div>
                `;
              } else {
                // viz_type is 'none' - don't show any visualization
                vizPlaceholder.innerHTML = '';
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
 * @param {array} columns - Column names
 * @param {array} data - Data rows
 * @param {boolean} autoExpand - If true, show table directly without collapse
 */
function renderDataTable(columns, data, autoExpand = false) {
  if (!columns || !data || data.length === 0) return '';

  const previewSize = 10;
  const pageSize = 20;
  const totalRows = data.length;
  const hasMore = totalRows > previewSize;
  const tableId = `table-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

  // Store data for expansion/pagination
  if (hasMore) {
    window._tableData = window._tableData || {};
    window._tableData[tableId] = { columns, data, pageSize, currentPage: 1, expanded: false };
  }

  let html = '';

  // If autoExpand, show table directly without details wrapper
  if (autoExpand) {
    html = `<div class="data-table-container" id="${tableId}-container">`;
  } else {
    html = `
    <details class="data-details">
      <summary class="data-summary">
        <span class="icon">📋</span>
        <span>View Table (${totalRows} rows)</span>
      </summary>
      <div class="data-table-container" id="${tableId}-container">`;
  }

  // Build initial preview (first 10 rows)
  html += buildTableRows(tableId, columns, data.slice(0, previewSize));

  // Add "View More" button if there's more data
  if (hasMore) {
    html += `
      <div class="table-actions" id="${tableId}-actions">
        <button class="view-more-btn" onclick="expandTable('${tableId}')">
          View All ${totalRows} Rows →
        </button>
      </div>`;
  }

  // Close the appropriate wrapper
  if (autoExpand) {
    html += '</div>';
  } else {
    html += '</div></details>';
  }

  return html;
}

/**
 * Build table HTML with rows
 */
function buildTableRows(tableId, columns, rows) {
  let html = `<div class="data-table-wrapper" id="${tableId}-body"><table class="data-table"><thead><tr>`;

  columns.forEach(col => {
    html += `<th>${escapeHtml(formatColumnHeader(col))}</th>`;
  });

  html += '</tr></thead><tbody>';

  rows.forEach(row => {
    html += '<tr>';
    columns.forEach(col => {
      html += `<td>${formatCellValue(row[col])}</td>`;
    });
    html += '</tr>';
  });

  html += '</tbody></table></div>';
  return html;
}

/**
 * Expand table to show all data with pagination
 */
function expandTable(tableId) {
  const tableData = window._tableData[tableId];
  if (!tableData) return;

  const { columns, data, pageSize } = tableData;
  const totalPages = Math.ceil(data.length / pageSize);
  tableData.expanded = true;
  tableData.currentPage = 1;

  // Build full paginated view
  let html = buildTableRows(tableId, columns, data.slice(0, pageSize));

  // Add pagination controls
  if (totalPages > 1) {
    html += `
      <div class="table-pagination" id="${tableId}-pagination">
        <button class="pagination-btn" onclick="changePage('${tableId}', -1)" id="${tableId}-prev" disabled>← Prev</button>
        <span class="pagination-info">Page <span id="${tableId}-page">1</span> of ${totalPages}</span>
        <button class="pagination-btn" onclick="changePage('${tableId}', 1)" id="${tableId}-next">Next →</button>
      </div>`;
  }

  // Replace content
  const container = document.getElementById(`${tableId}-container`);
  if (container) {
    container.innerHTML = html;
  }
}

/**
 * Change table page
 */
function changePage(tableId, direction) {
  const tableData = window._tableData[tableId];
  if (!tableData) return;

  const { columns, data, pageSize } = tableData;
  const totalPages = Math.ceil(data.length / pageSize);

  tableData.currentPage += direction;
  tableData.currentPage = Math.max(1, Math.min(tableData.currentPage, totalPages));

  const startIdx = (tableData.currentPage - 1) * pageSize;
  const pageData = data.slice(startIdx, startIdx + pageSize);

  // Update table body
  const bodyContainer = document.getElementById(`${tableId}-body`);
  if (bodyContainer) {
    bodyContainer.outerHTML = buildTableRows(tableId, columns, pageData);
  }

  // Update pagination state
  document.getElementById(`${tableId}-page`).textContent = tableData.currentPage;
  document.getElementById(`${tableId}-prev`).disabled = tableData.currentPage === 1;
  document.getElementById(`${tableId}-next`).disabled = tableData.currentPage === totalPages;
}

/**
 * Format column header for display (snake_case -> Title Case)
 */
function formatColumnHeader(col) {
  if (!col) return '';
  return col
    .replace(/_/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase())
    .replace(/Inr/g, '(INR)')
    .replace(/Pct/g, '%')
    .replace(/Qty/g, 'Quantity');
}

/**
 * Format cell value for display
 */
function formatCellValue(value) {
  if (value === null || value === undefined) return '<span class="null-value">-</span>';

  // Handle numbers
  if (typeof value === 'number' || (typeof value === 'string' && !isNaN(parseFloat(value)) && value.match(/^-?\d+\.?\d*$/))) {
    const num = parseFloat(value);

    // Format large numbers as Crores
    if (Math.abs(num) >= 10000000) {
      return `₹${(num / 10000000).toFixed(2)} Cr`;
    }
    // Format medium numbers as Lakhs
    if (Math.abs(num) >= 100000) {
      return `₹${(num / 100000).toFixed(2)} L`;
    }
    // Format with commas for smaller numbers
    if (Number.isInteger(num)) {
      return num.toLocaleString('en-IN');
    }
    // Decimal numbers - check if it looks like a percentage
    if (Math.abs(num) <= 100 && num !== Math.floor(num)) {
      return `${num.toFixed(2)}%`;
    }
    return num.toLocaleString('en-IN', { maximumFractionDigits: 2 });
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
