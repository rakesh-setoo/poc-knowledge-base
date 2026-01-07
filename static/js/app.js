/*
 * App Initialization
 * Main entry point for the frontend application
 */

/**
 * Initialize the application
 */
function initApp() {
  // Initialize upload handlers
  initUpload();

  // Load initial datasets
  loadDatasets();
}

// Run on DOM ready
document.addEventListener('DOMContentLoaded', initApp);
