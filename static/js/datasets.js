/*
 * Datasets Module
 * Manages dataset list, sync, and delete operations
 */

/**
 * Load and render datasets
 */
async function loadDatasets() {
  try {
    const datasets = await fetchDatasets();
    renderDatasets(datasets);
  } catch (error) {
    console.error('Failed to load datasets:', error);
  }
}

/**
 * Render datasets in UI
 */
function renderDatasets(datasets) {
  const container = document.getElementById('datasetsList');
  const dropdown = document.getElementById('datasetSelect');

  // Populate dropdown selector
  console.log('Rendering datasets:', datasets);
  dropdown.innerHTML = '<option value="">-- Select a dataset --</option>';
  if (datasets && datasets.length > 0) {
    datasets.forEach(d => {
      const option = document.createElement('option');
      if (d.id === undefined) console.warn('Dataset missing ID:', d);
      option.value = d.id;
      option.textContent = `${d.file_name} (${d.row_count.toLocaleString()} rows)`;
      dropdown.appendChild(option);
    });
    // Auto-select if only one dataset
    if (datasets.length === 1) {
      dropdown.value = datasets[0].id;
    }
  }

  // Render list
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

/**
 * Sync datasets with database
 */
async function syncDatasets() {
  try {
    const data = await syncDatasetsAPI();
    showToast(`Synced ${data.synced.length} new dataset(s)`, 'success');
    loadDatasets();
  } catch (error) {
    showToast('Sync failed: ' + error.message, 'error');
  }
}

/**
 * Delete a dataset
 */
async function deleteDataset(tableName) {
  if (!confirm('Delete this dataset?')) return;

  try {
    const data = await deleteDatasetAPI(tableName);
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
