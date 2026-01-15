/**
 * Charts Module
 * Renders charts using Chart.js library
 */

// Chart color palette (matches dark theme)
const CHART_COLORS = {
  primary: 'rgba(99, 102, 241, 0.8)',
  primaryBg: 'rgba(99, 102, 241, 0.2)',
  secondary: 'rgba(139, 92, 246, 0.8)',
  success: 'rgba(16, 185, 129, 0.8)',
  warning: 'rgba(245, 158, 11, 0.8)',
  error: 'rgba(239, 68, 68, 0.8)',
  palette: [
    'rgba(99, 102, 241, 0.8)',
    'rgba(139, 92, 246, 0.8)',
    'rgba(16, 185, 129, 0.8)',
    'rgba(245, 158, 11, 0.8)',
    'rgba(236, 72, 153, 0.8)',
    'rgba(6, 182, 212, 0.8)',
    'rgba(251, 146, 60, 0.8)'
  ]
};

// Chart instances cache (for cleanup)
const chartInstances = new Map();

/**
 * Check if chart rendering is appropriate
 */
function shouldRenderChart(vizType, data) {
  if (!vizType || vizType === 'table' || vizType === 'single_value') {
    return false;
  }
  if (!data || data.length < 2) {
    return false;
  }
  return ['bar', 'line', 'pie'].includes(vizType);
}

/**
 * Render a chart or visualization based on type
 */
function renderVisualization(containerId, vizType, columns, data) {
  const container = document.getElementById(containerId);
  if (!container) return null;

  // Clean up existing chart if any
  destroyChart(containerId);

  if (vizType === 'single_value') {
    return renderSingleValue(container, columns, data);
  }

  if (!shouldRenderChart(vizType, data)) {
    return null;
  }

  // Create chart canvas
  const canvas = document.createElement('canvas');
  canvas.id = `${containerId}-canvas`;
  canvas.className = 'chart-canvas';
  container.appendChild(canvas);

  const ctx = canvas.getContext('2d');
  let chart = null;

  switch (vizType) {
    case 'bar':
      chart = renderBarChart(ctx, columns, data);
      break;
    case 'line':
      chart = renderLineChart(ctx, columns, data);
      break;
    case 'pie':
      chart = renderPieChart(ctx, columns, data);
      break;
  }

  if (chart) {
    chartInstances.set(containerId, chart);
  }

  return chart;
}

/**
 * Render a bar chart
 */
function renderBarChart(ctx, columns, data) {
  const labels = data.slice(0, 20).map(row => truncateLabel(row[columns[0]]));
  const values = data.slice(0, 20).map(row => parseNumeric(row[columns[1]]));

  return new Chart(ctx, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [{
        label: columns[1] || 'Value',
        data: values,
        backgroundColor: CHART_COLORS.palette,
        borderColor: CHART_COLORS.palette.map(c => c.replace('0.8', '1')),
        borderWidth: 1,
        borderRadius: 6
      }]
    },
    options: getChartOptions('bar', columns[1], columns[0])
  });
}

/**
 * Render a line chart
 */
function renderLineChart(ctx, columns, data) {
  const labels = data.slice(0, 50).map(row => truncateLabel(row[columns[0]]));
  const values = data.slice(0, 50).map(row => parseNumeric(row[columns[1]]));

  return new Chart(ctx, {
    type: 'line',
    data: {
      labels: labels,
      datasets: [{
        label: columns[1] || 'Value',
        data: values,
        borderColor: CHART_COLORS.primary,
        backgroundColor: CHART_COLORS.primaryBg,
        fill: true,
        tension: 0,  // Straight lines like ChatGPT
        pointRadius: 6,
        pointHoverRadius: 8,
        pointBackgroundColor: CHART_COLORS.primary,
        pointBorderColor: '#fff',
        pointBorderWidth: 2,
        borderWidth: 2
      }]
    },
    options: getChartOptions('line', columns[1], columns[0])
  });
}

/**
 * Render a pie chart
 */
function renderPieChart(ctx, columns, data) {
  const labels = data.slice(0, 7).map(row => truncateLabel(row[columns[0]]));
  const values = data.slice(0, 7).map(row => parseNumeric(row[columns[1]]));

  return new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: labels,
      datasets: [{
        data: values,
        backgroundColor: CHART_COLORS.palette,
        borderColor: 'rgba(15, 23, 42, 0.8)',
        borderWidth: 2
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: 'right',
          labels: {
            color: 'rgba(255, 255, 255, 0.8)',
            padding: 15,
            font: { size: 12 }
          }
        },
        tooltip: getTooltipConfig()
      }
    }
  });
}

/**
 * Render single value display
 */
function renderSingleValue(container, columns, data) {
  if (!data || data.length === 0) return null;

  const row = data[0];
  const value = columns.length > 1 ? row[columns[1]] : row[columns[0]];
  const label = columns.length > 1 ? columns[1] : columns[0];

  container.innerHTML = `
    <div class="single-value-display">
      <div class="single-value-number">${formatValue(value)}</div>
      <div class="single-value-label">${escapeHtml(label)}</div>
    </div>
  `;
  return true;
}

/**
 * Get chart options based on type
 */
function getChartOptions(type, valueLabel, categoryLabel) {
  const baseOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: false
      },
      tooltip: getTooltipConfig()
    },
    scales: {
      x: {
        ticks: { color: 'rgba(255, 255, 255, 0.6)', maxRotation: 45 },
        grid: { color: 'rgba(255, 255, 255, 0.05)' },
        title: {
          display: true,
          text: categoryLabel || 'Category',
          color: 'rgba(255, 255, 255, 0.5)',
          font: { size: 11 }
        }
      },
      y: {
        ticks: {
          color: 'rgba(255, 255, 255, 0.6)',
          callback: function (value) {
            // Indian currency format: 1 Crore = 10 Million
            if (value >= 10000000) {
              // Values in Crores (10 million+)
              const crores = value / 10000000;
              if (crores >= 100) return crores.toFixed(0) + ' Cr';
              return crores.toFixed(1) + ' Cr';
            }
            if (value >= 100000) {
              // Values in Lakhs (100,000+)
              const lakhs = value / 100000;
              return lakhs.toFixed(1) + ' L';
            }
            // Show full numbers with commas for smaller values
            return value.toLocaleString('en-IN');
          }
        },
        grid: { color: 'rgba(255, 255, 255, 0.05)' },
        // Line charts: don't start at zero for better trend visibility
        beginAtZero: type !== 'line',
        title: {
          display: true,
          text: valueLabel || 'Value',
          color: 'rgba(255, 255, 255, 0.5)',
          font: { size: 11 }
        }
      }
    }
  };

  return baseOptions;
}

/**
 * Get tooltip configuration
 */
function getTooltipConfig() {
  return {
    backgroundColor: 'rgba(15, 23, 42, 0.95)',
    titleColor: 'rgba(255, 255, 255, 0.9)',
    bodyColor: 'rgba(255, 255, 255, 0.8)',
    borderColor: 'rgba(99, 102, 241, 0.5)',
    borderWidth: 1,
    padding: 12,
    cornerRadius: 8
  };
}

/**
 * Create visualization container with toggle - renders INSIDE the message bubble
 */
function createVizContainer(messageEl, vizType, columns, data) {
  // Render inside the message bubble, after the text
  const bubbleEl = messageEl.querySelector('.message-bubble');
  if (!bubbleEl) return null;

  const vizId = `viz-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

  const wrapper = document.createElement('div');
  wrapper.className = 'viz-wrapper viz-inline';
  wrapper.innerHTML = `
    <div class="viz-toggle">
      <button class="viz-toggle-btn active" data-view="chart" onclick="toggleVizView('${vizId}', 'chart')">
        📊 Chart
      </button>
      <button class="viz-toggle-btn" data-view="table" onclick="toggleVizView('${vizId}', 'table')">
        📋 Table
      </button>
    </div>
    <div class="viz-chart-container" id="${vizId}"></div>
    <div class="viz-table-container" id="${vizId}-table" style="display: none;">
      ${renderDataTable(columns, data)}
    </div>
  `;

  bubbleEl.appendChild(wrapper);

  // Render chart
  setTimeout(() => {
    renderVisualization(vizId, vizType, columns, data);
  }, 50);

  return vizId;
}

/**
 * Toggle between chart and table views
 */
function toggleVizView(vizId, view) {
  const chartContainer = document.getElementById(vizId);
  const tableContainer = document.getElementById(`${vizId}-table`);
  const wrapper = chartContainer?.closest('.viz-wrapper');

  if (!chartContainer || !tableContainer || !wrapper) return;

  // Update buttons
  wrapper.querySelectorAll('.viz-toggle-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.view === view);
  });

  // Toggle visibility
  chartContainer.style.display = view === 'chart' ? 'block' : 'none';
  tableContainer.style.display = view === 'table' ? 'block' : 'none';
}

/**
 * Destroy a chart instance
 */
function destroyChart(containerId) {
  const chart = chartInstances.get(containerId);
  if (chart) {
    chart.destroy();
    chartInstances.delete(containerId);
  }
}

/**
 * Helper: Format label for charts (with date detection)
 */
function truncateLabel(value, maxLen = 15) {
  if (value === null || value === undefined) return '';

  const str = String(value);

  // Detect ISO date format (2024-04-01 or 2024-04-01T00:00:00)
  const isoDateMatch = str.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (isoDateMatch) {
    const [, year, month, day] = isoDateMatch;
    const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const monthName = monthNames[parseInt(month, 10) - 1];
    // If day is 01, just show month (likely monthly aggregation)
    if (day === '01') {
      return `${monthName} ${year}`;
    }
    return `${day} ${monthName}`;
  }

  // Detect month number (1-12) - common in GROUP BY month queries
  if (/^(1|2|3|4|5|6|7|8|9|10|11|12)$/.test(str)) {
    const monthNum = parseInt(str, 10);
    const monthNames = ['January', 'February', 'March', 'April', 'May', 'June',
      'July', 'August', 'September', 'October', 'November', 'December'];
    return monthNames[monthNum - 1] || str;
  }

  // Detect "YYYY-MM" format (year-month)
  const yearMonthMatch = str.match(/^(\d{4})-(\d{2})$/);
  if (yearMonthMatch) {
    const [, year, month] = yearMonthMatch;
    const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    return `${monthNames[parseInt(month, 10) - 1]} ${year}`;
  }

  // Truncate long strings
  return str.length > maxLen ? str.substring(0, maxLen) + '...' : str;
}

/**
 * Helper: Parse numeric value
 */
function parseNumeric(value) {
  if (typeof value === 'number') return value;
  const num = parseFloat(value);
  return isNaN(num) ? 0 : num;
}

/**
 * Helper: Format value for display
 */
function formatValue(value) {
  if (value === null || value === undefined) return '-';
  if (typeof value === 'number') {
    return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
  }
  return String(value);
}
