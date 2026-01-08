/*
 * Query Module
 * Handles question asking and result rendering with streaming
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
  const results = document.getElementById('results');
  const askBtn = document.getElementById('askBtn');
  const responseTimeBadge = document.getElementById('responseTimeBadge');
  const responseTimeEl = document.getElementById('responseTime');

  // Hide response time badge and show loading state
  responseTimeBadge.style.display = 'none';
  loading.classList.add('show');
  results.classList.remove('show');
  askBtn.disabled = true;

  const startTime = performance.now();

  try {
    const response = await fetch(`${API_BASE}/ask-stream`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question: question, dataset_id: selectedDatasetId })
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let streamedAnswer = '';
    let metadataReceived = false;
    let resultData = null;
    let buffer = '';  // Buffer to accumulate incomplete chunks

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      // Add new data to buffer
      buffer += decoder.decode(value, { stream: true });

      // Process complete lines from buffer
      const lines = buffer.split('\n');
      // Keep the last incomplete line in buffer
      buffer = lines.pop() || '';

      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;

        try {
          const data = JSON.parse(line.slice(6));

          // Handle error
          if (data.error) {
            loading.classList.remove('show');
            results.classList.add('show');
            renderError(results, data);
            askBtn.disabled = false;
            return;
          }

          // Handle metadata (table, SQL, data) - render immediately
          if (data.type === 'metadata') {
            metadataReceived = true;
            resultData = data;
            loading.classList.remove('show');
            results.classList.add('show');
            renderStreamingResults(results, data, '');
          }

          // Handle streaming tokens
          if (data.type === 'token' && metadataReceived) {
            streamedAnswer += data.content;
            updateStreamingAnswer(streamedAnswer);
          }

          // Handle completion
          if (data.type === 'done') {
            responseTimeEl.textContent = data.elapsed.toFixed(2);
            responseTimeBadge.style.display = 'block';
            // Remove cursor when done
            const answerEl = document.getElementById('streamingAnswer');
            if (answerEl && streamedAnswer) {
              answerEl.innerHTML = formatAnswer(streamedAnswer);
            }
          }
        } catch (e) {
          // Skip malformed JSON - may be incomplete chunk
          console.debug('JSON parse error (may be incomplete chunk):', e.message);
        }
      }
    }

  } catch (error) {
    const endTime = performance.now();
    const elapsedSeconds = ((endTime - startTime) / 1000).toFixed(2);
    responseTimeEl.textContent = elapsedSeconds;
    responseTimeBadge.style.display = 'block';

    loading.classList.remove('show');
    results.classList.add('show');
    console.error('Connection error:', error);
    results.innerHTML = `
      <div class="result-error">
        <strong>⚠️ Oops!</strong> Something went wrong. Please try again.
      </div>
    `;
  }

  askBtn.disabled = false;
}

/**
 * Render results with placeholder for streaming answer
 */
function renderStreamingResults(container, data, answer) {
  container.innerHTML = `
    <div class="result-card">
      <div class="result-insight">
        <span class="insight-icon">💡</span>
        <span class="insight-text" id="streamingAnswer">${answer ? formatAnswer(answer) : '<span class="streaming-cursor">▊</span>'}</span>
      </div>
    </div>
    
    <div class="result-card">
      <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.75rem;">
        <div class="result-label" style="margin-bottom: 0;">Results</div>
        <span class="row-count-badge">📊 ${data.row_count} rows</span>
      </div>
      ${renderDataTable(data.columns, data.data)}
    </div>
    
    <div class="result-card">
      <div class="result-label">Generated SQL</div>
      <div class="result-sql">${escapeHtml(data.generated_sql)}</div>
      <div class="result-meta" style="margin-top: 1rem;">
        <span>📋 Table: <strong>${data.table_used}</strong></span>
      </div>
    </div>
  `;
}

/**
 * Update the streaming answer in place
 */
function updateStreamingAnswer(answer) {
  const answerEl = document.getElementById('streamingAnswer');
  if (answerEl) {
    answerEl.innerHTML = formatAnswer(answer) + '<span class="streaming-cursor">▊</span>';
  }
}

/**
 * Render error response
 */
function renderError(container, data) {
  // Show generic error message to user (detailed error is logged in backend terminal)
  console.error('Query error:', data.error);
  container.innerHTML = `
    <div class="result-error">
      <strong>⚠️ Oops!</strong> ${data.error}
    </div>
  `;
}

/**
 * Render successful query results
 */
function renderResults(container, data) {
  container.innerHTML = `
    <div class="result-card">
      <div class="result-insight">
        <span class="insight-icon">💡</span>
        <span class="insight-text">${formatAnswer(data.answer)}</span>
      </div>
    </div>
    
    <div class="result-card">
      <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.75rem;">
        <div class="result-label" style="margin-bottom: 0;">Results</div>
        <span class="row-count-badge">📊 ${data.row_count} rows</span>
      </div>
      ${renderDataTable(data.columns, data.data)}
    </div>
    
    <div class="result-card">
      <div class="result-label">Generated SQL</div>
      <div class="result-sql">${escapeHtml(data.generated_sql)}</div>
      <div class="result-meta" style="margin-top: 1rem;">
        <span>📋 Table: <strong>${data.table_used}</strong></span>
      </div>
    </div>
  `;
}

// Pagination state
let paginationState = {
  columns: [],
  data: [],
  currentPage: 1,
  rowsPerPage: 50
};

/**
 * Render data table with pagination
 */
function renderDataTable(columns, data, page = 1) {
  if (!data || data.length === 0) {
    return '<p style="color: var(--text-muted); text-align: center; padding: 1rem;">No results found</p>';
  }

  // Store state for pagination
  paginationState.columns = columns;
  paginationState.data = data;
  paginationState.currentPage = page;

  const totalRows = data.length;
  const totalPages = Math.ceil(totalRows / paginationState.rowsPerPage);
  const startIndex = (page - 1) * paginationState.rowsPerPage;
  const endIndex = Math.min(startIndex + paginationState.rowsPerPage, totalRows);
  const pageData = data.slice(startIndex, endIndex);

  // Determine if each column is numeric by checking first row values
  const headers = columns.map(col => {
    const firstRowValue = data[0][col];
    const headerClass = isNumeric(firstRowValue) ? 'number-header' : '';
    return `<th class="${headerClass}">${escapeHtml(formatColumnName(col))}</th>`;
  }).join('');

  const rows = pageData.map((row, index) => {
    const cells = columns.map(col => {
      const value = row[col];
      const cellClass = isNumeric(value) ? 'number-cell' : 'name-cell';
      const formattedValue = formatCellValue(value, col);
      return `<td class="${cellClass}">${formattedValue}</td>`;
    }).join('');
    return `<tr><td class="rank-cell">${startIndex + index + 1}</td>${cells}</tr>`;
  }).join('');

  // Pagination controls (only show if more than 50 rows)
  const paginationHtml = totalRows > paginationState.rowsPerPage ? `
    <div class="pagination-controls">
      <button class="pagination-btn" onclick="goToPage(${page - 1})" ${page === 1 ? 'disabled' : ''}>
        ← Previous
      </button>
      <span class="pagination-info">
        Page ${page} of ${totalPages} (${startIndex + 1}-${endIndex} of ${totalRows})
      </span>
      <button class="pagination-btn" onclick="goToPage(${page + 1})" ${page === totalPages ? 'disabled' : ''}>
        Next →
      </button>
    </div>
  ` : '';

  return `
    <div class="result-table-wrapper">
      <table class="result-table">
        <thead>
          <tr><th>#</th>${headers}</tr>
        </thead>
        <tbody>
          ${rows}
        </tbody>
      </table>
    </div>
    ${paginationHtml}
  `;
}

/**
 * Go to a specific page
 */
function goToPage(page) {
  if (page < 1 || page > Math.ceil(paginationState.data.length / paginationState.rowsPerPage)) {
    return;
  }

  const tableContainer = document.querySelector('.result-table-wrapper')?.parentElement;
  if (tableContainer) {
    const rowCountBadge = tableContainer.querySelector('.row-count-badge');
    const rowCountHtml = rowCountBadge ? rowCountBadge.outerHTML : '';
    const labelHtml = '<div class="result-label" style="margin-bottom: 0;">Results</div>';

    tableContainer.innerHTML = `
      <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.75rem;">
        ${labelHtml}
        ${rowCountHtml}
      </div>
      ${renderDataTable(paginationState.columns, paginationState.data, page)}
    `;
  }
}

