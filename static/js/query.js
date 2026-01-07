/*
 * Query Module
 * Handles question asking and result rendering
 */

/**
 * Ask a question about the data
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

  console.log('Dataset Selection Debug:', { selectedValue, parsed: parseInt(selectedValue) });

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
    const data = await askQuestionAPI(question, selectedDatasetId);

    const endTime = performance.now();
    const elapsedSeconds = ((endTime - startTime) / 1000).toFixed(2);

    // Show response time
    responseTimeEl.textContent = elapsedSeconds;
    responseTimeBadge.style.display = 'block';

    loading.classList.remove('show');
    results.classList.add('show');

    if (data.error) {
      renderError(results, data);
    } else {
      renderResults(results, data);
    }
  } catch (error) {
    const endTime = performance.now();
    const elapsedSeconds = ((endTime - startTime) / 1000).toFixed(2);
    responseTimeEl.textContent = elapsedSeconds;
    responseTimeBadge.style.display = 'block';

    loading.classList.remove('show');
    results.classList.add('show');
    results.innerHTML = `
      <div class="result-error">
        <strong>Connection Error:</strong> ${error.message}
      </div>
    `;
  }

  askBtn.disabled = false;
}

/**
 * Render error response
 */
function renderError(container, data) {
  container.innerHTML = `
    <div class="result-error">
      <strong>Error:</strong> ${data.error}
      ${data.generated_sql ? `<div class="result-sql" style="margin-top: 1rem;">${data.generated_sql}</div>` : ''}
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

/**
 * Render data table
 */
function renderDataTable(columns, data) {
  if (!data || data.length === 0) {
    return '<p style="color: var(--text-muted); text-align: center; padding: 1rem;">No results found</p>';
  }

  const headers = columns.map(col => `<th>${escapeHtml(formatColumnName(col))}</th>`).join('');

  const rows = data.map((row, index) => {
    const cells = columns.map(col => {
      const value = row[col];
      const cellClass = isNumeric(value) ? 'number-cell' : 'name-cell';
      const formattedValue = formatCellValue(value, col);
      return `<td class="${cellClass}">${formattedValue}</td>`;
    }).join('');
    return `<tr><td class="rank-cell">${index + 1}</td>${cells}</tr>`;
  }).join('');

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
  `;
}
