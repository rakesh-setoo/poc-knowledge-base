/*
 * Utility Functions
 * Formatting, escaping, and helper functions
 */

/**
 * Escape HTML to prevent XSS
 */
function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

/**
 * Format column name from snake_case to Title Case
 */
function formatColumnName(name) {
  return name
    .replace(/_/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase());
}

/**
 * Check if a value is numeric
 */
function isNumeric(value) {
  return typeof value === 'number' ||
    (typeof value === 'string' && !isNaN(parseFloat(value)) && isFinite(value));
}

/**
 * Detect column type for smart formatting
 */
function getColumnType(colName) {
  const lower = colName.toLowerCase();

  // Check for rank/position columns first (should be plain numbers, not currency)
  if (['rank', 'position', 'row', 'index'].some(kw => lower.includes(kw))) {
    return 'id';
  }

  // Currency columns - includes 'inr' for Indian Rupee columns
  if (['value', 'amount', 'sales', 'revenue', 'profit', 'cost', 'price', 'total', 'inr'].some(kw => lower.includes(kw))) {
    // Exclude if it's a rank/percentage column that happens to have these words
    if (['rank', 'percent', 'pct', '%'].some(kw => lower.includes(kw))) {
      return 'default';
    }
    return 'currency';
  }
  if (['id', 'code', 'number', 'no', 'invoice'].some(kw => lower.includes(kw))) {
    return 'id';
  }
  if (['percent', 'pct', 'rate', 'ratio', 'percentage'].some(kw => lower.includes(kw))) {
    return 'percent';
  }
  if (['count', 'qty', 'quantity'].some(kw => lower.includes(kw))) {
    return 'count';
  }
  return 'default';
}

/**
 * Format cell value based on column type
 */
function formatCellValue(value, colName = '') {
  if (value === null || value === undefined) return '-';

  if (typeof value === 'object') {
    return escapeHtml(JSON.stringify(value));
  }

  if (typeof value === 'number') {
    const colType = getColumnType(colName);

    if (colType === 'currency') {
      if (Math.abs(value) >= 10000000) {
        return '₹' + (value / 10000000).toFixed(2) + ' Cr';
      } else if (Math.abs(value) >= 100000) {
        return '₹' + (value / 100000).toFixed(2) + ' L';
      }
      return '₹' + value.toLocaleString('en-IN', { maximumFractionDigits: 2 });
    }

    if (colType === 'id') {
      return String(Math.round(value));
    }

    if (colType === 'percent') {
      return value.toFixed(2) + '%';
    }

    if (colType === 'count') {
      return Math.round(value).toLocaleString('en-IN');
    }

    return value.toLocaleString('en-IN', { maximumFractionDigits: 2 });
  }

  return escapeHtml(String(value));
}

/**
 * Format answer text with markdown-style formatting
 */
function formatAnswer(text) {
  return escapeHtml(text)
    .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
    .replace(/\n/g, '<br>');
}

/**
 * Show toast notification
 */
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
