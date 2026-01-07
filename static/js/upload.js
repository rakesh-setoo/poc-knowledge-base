/*
 * File Upload Module
 * Handles drag-drop, file selection, and progress streaming
 */

// DOM Elements
const uploadZone = document.getElementById('uploadZone');
const fileInput = document.getElementById('fileInput');

/**
 * Initialize upload handlers
 */
function initUpload() {
  uploadZone.addEventListener('dragover', handleDragOver);
  uploadZone.addEventListener('dragleave', handleDragLeave);
  uploadZone.addEventListener('drop', handleDrop);
  fileInput.addEventListener('change', handleFileSelect);
}

function handleDragOver(e) {
  e.preventDefault();
  uploadZone.classList.add('dragover');
}

function handleDragLeave() {
  uploadZone.classList.remove('dragover');
}

function handleDrop(e) {
  e.preventDefault();
  uploadZone.classList.remove('dragover');
  const file = e.dataTransfer.files[0];
  if (file) uploadFile(file);
}

function handleFileSelect(e) {
  const file = e.target.files[0];
  if (file) uploadFile(file);
}

/**
 * Upload file with SSE progress tracking
 */
async function uploadFile(file) {
  const formData = new FormData();
  formData.append('file', file);

  // Get progress elements
  const uploadProgress = document.getElementById('uploadProgress');
  const uploadFileName = document.getElementById('uploadFileName');
  const uploadPercent = document.getElementById('uploadPercent');
  const uploadProgressFill = document.getElementById('uploadProgressFill');
  const uploadStatus = document.getElementById('uploadStatus');

  // Show progress bar and prepare UI
  uploadFileName.textContent = file.name;
  uploadPercent.textContent = '0%';
  uploadProgressFill.style.width = '0%';
  uploadStatus.className = 'upload-progress-status';
  uploadStatus.innerHTML = '<span class="spinner" style="width: 14px; height: 14px; border-width: 2px;"></span><span>Starting upload...</span>';
  uploadProgress.classList.add('show');
  uploadZone.style.pointerEvents = 'none';
  uploadZone.style.opacity = '0.6';

  try {
    const response = await uploadFileAPI(formData);

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
      buffer = lines.pop();

      for (const line of lines) {
        if (line.startsWith('data: ')) {
          try {
            const eventData = JSON.parse(line.substring(6));
            updateProgressUI(eventData, uploadPercent, uploadProgressFill, uploadStatus, uploadProgress);
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

    setTimeout(() => resetUploadUI(uploadProgress), 3000);
  }

  fileInput.value = '';
}

/**
 * Update progress UI based on SSE event
 */
function updateProgressUI(eventData, uploadPercent, uploadProgressFill, uploadStatus, uploadProgress) {
  uploadPercent.textContent = eventData.progress + '%';
  uploadProgressFill.style.width = eventData.progress + '%';
  uploadStatus.innerHTML = '<span class="spinner" style="width: 14px; height: 14px; border-width: 2px;"></span><span>' + eventData.status + '</span>';

  if (eventData.error) {
    uploadStatus.className = 'upload-progress-status error';
    uploadStatus.innerHTML = '❌ ' + eventData.error;
    showToast(eventData.error, 'error');
    setTimeout(() => resetUploadUI(uploadProgress), 3000);
    return;
  }

  if (eventData.progress === 100 && eventData.result) {
    uploadStatus.className = 'upload-progress-status success';
    uploadStatus.innerHTML = '✅ Upload complete!';
    showToast(`Uploaded: ${eventData.result.file_name} (${eventData.result.row_count} rows)`, 'success');
    loadDatasets();
    setTimeout(() => resetUploadUI(uploadProgress), 1500);
  }
}

/**
 * Reset upload zone UI
 */
function resetUploadUI(uploadProgress) {
  uploadProgress.classList.remove('show');
  uploadZone.style.pointerEvents = '';
  uploadZone.style.opacity = '';
}
