// API Configuration
const API_BASE = 'http://localhost:8005';

// ============================================
// Toast Notifications
// ============================================
function showToast(message, type = 'success') {
  const container = document.getElementById('toastContainer');
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerHTML = `
    <span>${type === 'success' ? '✅' : '❌'}</span>
    <span>${message}</span>
  `;
  container.appendChild(toast);
  setTimeout(() => toast.remove(), 4000);
}

// ============================================
// File Upload
// ============================================
const uploadZone = document.getElementById('uploadZone');
const fileInput = document.getElementById('fileInput');

uploadZone.addEventListener('dragover', (e) => {
  e.preventDefault();
  uploadZone.classList.add('dragover');
});

uploadZone.addEventListener('dragleave', () => {
  uploadZone.classList.remove('dragover');
});

uploadZone.addEventListener('drop', (e) => {
  e.preventDefault();
  uploadZone.classList.remove('dragover');
  const file = e.dataTransfer.files[0];
  if (file) uploadFile(file);
});

fileInput.addEventListener('change', (e) => {
  const file = e.target.files[0];
  if (file) uploadFile(file);
});

async function uploadFile(file) {
  const formData = new FormData();
  formData.append('file', file);

  // Get progress elements
  const uploadProgress = document.getElementById('uploadProgress');
  const uploadFileName = document.getElementById('uploadFileName');
  const uploadPercent = document.getElementById('uploadPercent');
  const uploadProgressFill = document.getElementById('uploadProgressFill');
  const uploadStatus = document.getElementById('uploadStatus');
  const uploadZoneEl = document.getElementById('uploadZone');

  // Show progress bar and prepare UI
  uploadFileName.textContent = file.name;
  uploadPercent.textContent = '0%';
  uploadProgressFill.style.width = '0%';
  uploadStatus.className = 'upload-progress-status';
  uploadStatus.innerHTML = '<span class="spinner" style="width: 14px; height: 14px; border-width: 2px;"></span><span>Starting upload...</span>';
  uploadProgress.classList.add('show');
  uploadZoneEl.style.pointerEvents = 'none';
  uploadZoneEl.style.opacity = '0.6';

  try {
    const response = await fetch(`${API_BASE}/upload-excel`, {
      method: 'POST',
      body: formData
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    // Read the SSE stream
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const { done, value } = await reader.read();

      if (done) break;

      buffer += decoder.decode(value, { stream: true });

      // Process complete SSE events in buffer
      const lines = buffer.split('\n\n');
      buffer = lines.pop(); // Keep incomplete event in buffer

      for (const line of lines) {
        if (line.startsWith('data: ')) {
          try {
            const eventData = JSON.parse(line.substring(6));

            // Update progress UI
            uploadPercent.textContent = eventData.progress + '%';
            uploadProgressFill.style.width = eventData.progress + '%';
            uploadStatus.innerHTML = '<span class="spinner" style="width: 14px; height: 14px; border-width: 2px;"></span><span>' + eventData.status + '</span>';

            // Check for completion or error
            if (eventData.error) {
              uploadStatus.className = 'upload-progress-status error';
              uploadStatus.innerHTML = '❌ ' + eventData.error;
              showToast(eventData.error, 'error');

              setTimeout(() => {
                uploadProgress.classList.remove('show');
                uploadZoneEl.style.pointerEvents = '';
                uploadZoneEl.style.opacity = '';
              }, 3000);
              return;
            }

            if (eventData.progress === 100 && eventData.result) {
              uploadStatus.className = 'upload-progress-status success';
              uploadStatus.innerHTML = '✅ Upload complete!';
              showToast(`Uploaded: ${eventData.result.file_name} (${eventData.result.row_count} rows)`, 'success');
              loadDatasets();

              setTimeout(() => {
                uploadProgress.classList.remove('show');
                uploadZoneEl.style.pointerEvents = '';
                uploadZoneEl.style.opacity = '';
              }, 1500);
            }
          } catch (parseError) {
            console.error('Failed to parse SSE event:', parseError);
          }
        }
      }
    }
  } catch (error) {
    console.error('Upload error:', error);
    uploadStatus.className = 'upload-progress-status error';
    uploadStatus.innerHTML = '❌ ' + error.message;
    showToast('Upload failed: ' + error.message, 'error');

    setTimeout(() => {
      uploadProgress.classList.remove('show');
      uploadZoneEl.style.pointerEvents = '';
      uploadZoneEl.style.opacity = '';
    }, 3000);
  }

  fileInput.value = '';
}

// ============================================
// Datasets Management
// ============================================
async function loadDatasets() {
  try {
    const response = await fetch(`${API_BASE}/datasets`);
    const data = await response.json();
    renderDatasets(data.datasets);
  } catch (error) {
    console.error('Failed to load datasets:', error);
  }
}

function renderDatasets(datasets) {
  const container = document.getElementById('datasetsList');
  const dropdown = document.getElementById('datasetSelect');

  // Populate dropdown selector
  dropdown.innerHTML = '<option value="" style="background: #1a1a2e; color: #ffffff;">-- Select a dataset --</option>';
  if (datasets && datasets.length > 0) {
    datasets.forEach(d => {
      const option = document.createElement('option');
      option.value = d.id;
      option.textContent = `${d.file_name} (${d.row_count.toLocaleString()} rows)`;
      option.style.background = '#1a1a2e';
      option.style.color = '#ffffff';
      dropdown.appendChild(option);
    });
    // Auto-select if only one dataset
    if (datasets.length === 1) {
      dropdown.value = datasets[0].id;
    }
  }

  if (!datasets || datasets.length === 0) {
    container.innerHTML = `
      <div class="empty-state">
        <div class="icon">📭</div>
        <p>No datasets yet. Upload an Excel file or sync existing tables.</p>
      </div>
    `;
    return;
  }

  container.innerHTML = datasets.map(d => `
    <div class="dataset-item">
      <div class="dataset-info">
        <h4>${d.file_type === 'csv' ? '📄' : '📊'} ${d.file_name}</h4>
        <div class="dataset-meta">
          <span>📋 ${d.columns.length} columns</span>
          <span>📝 ${d.row_count.toLocaleString()} rows</span>
        </div>
      </div>
      <button class="btn btn-danger btn-sm" onclick="deleteDataset('${d.table_name}')">
        🗑️
      </button>
    </div>
  `).join('');
}

async function syncDatasets() {
  try {
    const response = await fetch(`${API_BASE}/sync-datasets`, { method: 'POST' });
    const data = await response.json();
    showToast(`Synced ${data.synced.length} new dataset(s)`, 'success');
    loadDatasets();
  } catch (error) {
    showToast('Sync failed: ' + error.message, 'error');
  }
}

async function deleteDataset(tableName) {
  if (!confirm('Delete this dataset?')) return;

  try {
    const response = await fetch(`${API_BASE}/datasets/${tableName}`, {
      method: 'DELETE'
    });
    const data = await response.json();

    if (data.error) {
      showToast(data.error, 'error');
    } else {
      showToast('Dataset deleted', 'success');
      loadDatasets();
    }
  } catch (error) {
    showToast('Delete failed: ' + error.message, 'error');
  }
}

// ============================================
// Ask Question
// ============================================
async function askQuestion() {
  const input = document.getElementById('questionInput');
  const question = input.value.trim();
  const datasetSelect = document.getElementById('datasetSelect');
  const selectedValue = datasetSelect.value;

  if (selectedValue === '' || selectedValue === null) {
    showToast('Please select a dataset first', 'error');
    return;
  }

  const selectedDatasetId = parseInt(selectedValue);

  if (!question) {
    showToast('Please enter a question', 'error');
    return;
  }

  const loading = document.getElementById('loading');
  const results = document.getElementById('results');
  const askBtn = document.getElementById('askBtn');

  loading.classList.add('show');
  results.classList.remove('show');
  askBtn.disabled = true;

  try {
    const response = await fetch(`${API_BASE}/ask`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question: question, dataset_id: selectedDatasetId })
    });
    const data = await response.json();

    loading.classList.remove('show');
    results.classList.add('show');

    if (data.error) {
      results.innerHTML = `
        <div class="result-error">
          <strong>Error:</strong> ${data.error}
          ${data.generated_sql ? `<div class="result-sql" style="margin-top: 1rem;">${data.generated_sql}</div>` : ''}
        </div>
      `;
    } else {
      results.innerHTML = `
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
  } catch (error) {
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

// ============================================
// Data Table Rendering
// ============================================
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

// ============================================
// Utility Functions
// ============================================
function formatColumnName(name) {
  return name
    .replace(/_/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase());
}

function isNumeric(value) {
  return typeof value === 'number' || (typeof value === 'string' && !isNaN(parseFloat(value)) && isFinite(value));
}

// Intelligent column type detection
function getColumnType(colName) {
  const lower = colName.toLowerCase();

  // Currency columns
  if (['value', 'amount', 'sales', 'revenue', 'profit', 'cost', 'price', 'total'].some(kw => lower.includes(kw))) {
    return 'currency';
  }
  // ID/Code columns
  if (['id', 'code', 'number', 'no', 'invoice'].some(kw => lower.includes(kw))) {
    return 'id';
  }
  // Percentage columns
  if (['percent', 'pct', 'rate', 'ratio'].some(kw => lower.includes(kw))) {
    return 'percent';
  }
  // Count columns
  if (['count', 'qty', 'quantity'].some(kw => lower.includes(kw))) {
    return 'count';
  }
  return 'default';
}

function formatCellValue(value, colName = '') {
  if (value === null || value === undefined) return '-';

  if (typeof value === 'object') {
    return escapeHtml(JSON.stringify(value));
  }

  if (typeof value === 'number') {
    const colType = getColumnType(colName);

    if (colType === 'currency') {
      // Format as Indian currency
      if (Math.abs(value) >= 10000000) {
        return '₹' + (value / 10000000).toFixed(2) + ' Cr';
      } else if (Math.abs(value) >= 100000) {
        return '₹' + (value / 100000).toFixed(2) + ' L';
      }
      return '₹' + value.toLocaleString('en-IN', { maximumFractionDigits: 2 });
    }

    if (colType === 'id') {
      // Plain number for IDs
      return String(Math.round(value));
    }

    if (colType === 'percent') {
      return value.toFixed(2) + '%';
    }

    if (colType === 'count') {
      return Math.round(value).toLocaleString('en-IN');
    }

    // Default: format with commas
    return value.toLocaleString('en-IN', { maximumFractionDigits: 2 });
  }

  return escapeHtml(String(value));
}

function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

function formatAnswer(text) {
  // Convert markdown-style formatting
  return escapeHtml(text)
    .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
    .replace(/\n/g, '<br>');
}

// ============================================
// Initialize
// ============================================
document.addEventListener('DOMContentLoaded', () => {
  loadDatasets();
});
