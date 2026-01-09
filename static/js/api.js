/*
 * API Functions
 * Wrapper functions for backend API calls
 */

/**
 * Fetch all datasets
 */
async function fetchDatasets() {
  const response = await fetch(`${API_BASE}/datasets`);
  const data = await response.json();
  return data.datasets;
}

/**
 * Sync datasets with database
 */
async function syncDatasetsAPI() {
  const response = await fetch(`${API_BASE}/datasets/sync`, { method: 'POST' });
  return response.json();
}

/**
 * Delete a dataset
 */
async function deleteDatasetAPI(tableName) {
  const response = await fetch(`${API_BASE}/datasets/${tableName}`, {
    method: 'DELETE'
  });
  return response.json();
}

/**
   * Upload file with SSE progress
   * Returns the Response object for SSE streaming
   */
async function uploadFileAPI(formData) {
  const response = await fetch(`${API_BASE}/upload-excel`, {
    method: 'POST',
    body: formData
  });

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  return response;
}
