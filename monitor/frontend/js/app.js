// app.js - Basic Frontend Logic for News Agent Monitor

const MAX_CHART_POINTS = 180; // Max data points to display on charts (e.g., 30 minutes if 1 point per 10s)
const MAX_LOG_LINES = 200;

// DOM Elements
const wsStatusElem = document.getElementById('ws-status'); // Add this element to HTML if needed
const systemStatusElem = document.getElementById('system-status');
const activeLocationElem = document.getElementById('active-location');
const logPanel = document.getElementById('log-panel');
const errorPanel = document.getElementById('error-panel');
const funnelDisplay = document.getElementById('funnel-display');
const agentStatusTableBody = document.getElementById('agent-status-table')?.querySelector('tbody');
const llmMetricsTableBody = document.getElementById('llm-metrics-table')?.querySelector('tbody');

// Chart instances (initialize after fetching initial data)
let resourceChart = null;
let rateChart = null;
// Add more chart variables

// --- WebSocket Connection ---
let socket = null;

function connectWebSocket() {
    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${wsProtocol}//${window.location.host}/ws`;
    console.log(`Connecting to WebSocket: ${wsUrl}`);

    socket = new WebSocket(wsUrl);

    socket.onopen = (event) => {
        console.log("WebSocket connected.");
        // wsStatusElem.textContent = 'Connected';
        // wsStatusElem.style.color = 'green';
        fetchInitialData(); // Fetch historical data on connect
    };

    socket.onmessage = (event) => {
        try {
            const message = JSON.parse(event.data);
            handleWebSocketMessage(message);
        } catch (e) {
            console.error("Failed to parse WebSocket message:", e, event.data);
        }
    };

    socket.onerror = (event) => {
        console.error("WebSocket error:", event);
        // wsStatusElem.textContent = 'Error';
        // wsStatusElem.style.color = 'red';
    };

    socket.onclose = (event) => {
        console.log("WebSocket closed. Attempting reconnect...", event.reason);
        // wsStatusElem.textContent = 'Closed';
        // wsStatusElem.style.color = 'orange';
        // Simple exponential backoff reconnect
        setTimeout(connectWebSocket, Math.min(1000 * (2 ** reconnectAttempt), 30000));
        reconnectAttempt++;
    };
}
let reconnectAttempt = 0;


// --- Initial Data Fetch ---
async function fetchInitialData() {
    try {
        const response = await fetch('/api/initial_data');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        console.log("Received initial data:", data);
        initializeDashboard(data);
        reconnectAttempt = 0; // Reset on successful connect/fetch
    } catch (e) {
        console.error("Failed to fetch initial data:", e);
        // Maybe display an error message on the page
    }
}

// --- Dashboard Initialization ---
function initializeDashboard(data) {
    // Initialize KPIs
    updateOverview(data.system_status);

    // Initialize Charts
    setupResourceChart(data.resources);
    setupRateChart(data.rates);
    // Initialize other charts...

    // Initialize Funnel
    updateFunnel(data.funnel_counts);

    // Initialize Tables
    updateAgentStatusTable(data.agent_status);
    updateLlmMetricsTable(data.llm_metrics); // Pass the array directly

    // Initialize Logs/Errors
    logPanel.innerHTML = ''; // Clear panels
    errorPanel.innerHTML = '';
    data.logs.forEach(log => addLogEntry(log, logPanel));
    data.errors.forEach(err => addLogEntry(err, errorPanel));
}

// --- Chart Setup Functions --- (Using Chart.js examples)
function setupResourceChart(resourceData) {
    const ctx = document.getElementById('resourceChart')?.getContext('2d');
    if (!ctx) return;

    const initialTimestamps = resourceData?.cpu?.timestamps?.map(ts => ts * 1000) || [];
    const initialCpuValues = resourceData?.cpu?.values || [];
    const initialMemValues = resourceData?.memory?.values || [];


    resourceChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: initialTimestamps, // Use timestamps as labels
            datasets: [
                {
                    label: 'CPU (%)',
                    data: initialCpuValues,
                    borderColor: 'rgb(75, 192, 192)',
                    tension: 0.1,
                    yAxisID: 'y',
                },
                {
                    label: 'Memory (%)',
                    data: initialMemValues,
                    borderColor: 'rgb(255, 99, 132)',
                    tension: 0.1,
                    yAxisID: 'y',
                }
            ]
        },
        options: {
            scales: {
                x: {
                    type: 'time',
                    time: { unit: 'minute' },
                    title: { display: true, text: 'Time' }
                },
                y: {
                    beginAtZero: true,
                    max: 100, // Percentage
                    title: { display: true, text: 'Usage (%)' }
                }
            },
            animation: false // Faster updates potentially
        }
    });
}

function setupRateChart(rateData) {
     const ctx = document.getElementById('rateChart')?.getContext('2d');
     if (!ctx) return;

     const initialTimestamps = rateData?.articles?.timestamps?.map(ts => ts * 1000) || [];
     const initialArticleRates = rateData?.articles?.values || [];
     const initialErrorRates = rateData?.errors?.values || [];

     rateChart = new Chart(ctx, {
         type: 'line',
         data: {
             labels: initialTimestamps,
             datasets: [
                 {
                     label: 'Articles/sec',
                     data: initialArticleRates,
                     borderColor: 'rgb(54, 162, 235)',
                     tension: 0.1,
                     yAxisID: 'yArticles',
                 },
                 {
                     label: 'Errors/sec',
                     data: initialErrorRates,
                     borderColor: 'rgb(255, 159, 64)',
                     tension: 0.1,
                     yAxisID: 'yErrors',
                 }
             ]
         },
         options: {
             scales: {
                 x: { type: 'time', time: { unit: 'minute' }, title: { display: true, text: 'Time' } },
                 yArticles: { position: 'left', beginAtZero: true, title: { display: true, text: 'Articles/sec' } },
                 yErrors: { position: 'right', beginAtZero: true, title: { display: true, text: 'Errors/sec' }, grid: { drawOnChartArea: false } } // Only show grid for one axis
             },
             animation: false
         }
     });
}
// Add setup functions for other charts...


// --- WebSocket Message Handling ---
function handleWebSocketMessage(message) {
    // console.log("WS Message:", message); // Log received messages if needed
    switch (message.type) {
        case 'system_status':
            updateOverview(message.payload);
            break;
        case 'resource_update':
            updateChartData(resourceChart, message.payload.timestamp * 1000, [message.payload.cpu_percent, message.payload.memory_percent]);
            break;
        case 'rate_update':
             updateChartData(rateChart, message.payload.timestamp * 1000, [message.payload.articles_per_sec, message.payload.errors_per_sec]);
             break;
        case 'log':
            addLogEntry(message.payload, logPanel);
            break;
        case 'error':
             addLogEntry(message.payload, errorPanel);
             // Maybe update system status visually
             break;
        case 'funnel_update':
            updateFunnel(message.payload);
            break;
        case 'agent_status':
            updateAgentStatusEntry(message.payload.agent_id, message.payload.status);
            break;
        case 'llm_metric':
             updateLlmMetricEntry(message.payload);
             break;
         case 'processing_time':
              // Add to processing time chart data if implemented
              console.log(`Processing time recorded: ${message.payload.duration.toFixed(2)}s`);
              break;
         case 'stage_timing':
              // Add to stage timing chart data if implemented
              console.log(`Stage ${message.payload.stage} time: ${message.payload.duration.toFixed(2)}s`);
              break;

        default:
            console.warn("Received unknown WS message type:", message.type);
    }
}

// --- UI Update Functions ---
function updateOverview(payload) {
    if (!payload) return;
    systemStatusElem.textContent = payload.status || 'N/A';
    activeLocationElem.textContent = payload.active_location || 'None';
    // Update status color based on text
    if (payload.status === 'OK') systemStatusElem.style.color = 'green';
    else if (payload.status === 'Warning') systemStatusElem.style.color = 'orange';
    else if (payload.status === 'Error') systemStatusElem.style.color = 'red';
    else systemStatusElem.style.color = 'grey';
}

function updateChartData(chart, timestamp, values) {
    if (!chart || !values || values.length === 0) return;

    chart.data.labels.push(timestamp);
    values.forEach((value, index) => {
        if (chart.data.datasets[index]) {
            chart.data.datasets[index].data.push(value);
        }
    });

    // Limit data points
    if (chart.data.labels.length > MAX_CHART_POINTS) {
        chart.data.labels.shift();
        chart.data.datasets.forEach(dataset => {
            dataset.data.shift();
        });
    }
    chart.update('none'); // Use 'none' for no animation, potentially faster
}


function addLogEntry(log, panel) {
    if (!panel || !log) return;
    const entry = document.createElement('div');
    entry.classList.add('log-entry', `log-${log.level}`); // Add level class for styling
    const ts = new Date(log.timestamp * 1000).toLocaleTimeString();
    entry.textContent = `[${ts}] [${log.level}] [${log.name}] ${log.message}`;
    panel.appendChild(entry);

    // Auto-scroll and limit lines
    while (panel.children.length > MAX_LOG_LINES) {
        panel.removeChild(panel.firstChild);
    }
    panel.scrollTop = panel.scrollHeight; // Scroll to bottom
}

function updateFunnel(counts) {
     if (!funnelDisplay || !counts) return;
     // Simple text display for now, could be bars/Sankey later
     const order = ["sources_discovered", "sources_verified", "articles_fetched", "articles_validated", "articles_analyzed", "articles_stored"];
     let html = '<ul class="list">';
     order.forEach(key => {
          const count = counts[key] || 0;
          html += `<li class="list-item">${key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}: <strong>${count}</strong></li>`;
     });
     html += '</ul>';
     funnelDisplay.innerHTML = html;
}

function updateAgentStatusTable(allStatus) {
     if (!agentStatusTableBody || !allStatus) return;
     agentStatusTableBody.innerHTML = ''; // Clear existing rows
     Object.entries(allStatus).forEach(([agentId, status]) => {
          updateAgentStatusEntry(agentId, status); // Reuse single entry update logic
     });
}

function updateAgentStatusEntry(agentId, status) {
    if (!agentStatusTableBody || !status) return;
    let row = agentStatusTableBody.querySelector(`tr[data-agent-id="${agentId}"]`);
    if (!row) {
        row = document.createElement('tr');
        row.setAttribute('data-agent-id', agentId);
        row.innerHTML = `<td>${agentId.substring(0, 8)}...</td><td></td><td></td><td></td><td></td>`; // Create cells
        agentStatusTableBody.appendChild(row);
    }
    // Update cell content
    const cells = row.querySelectorAll('td');
    if (cells.length >= 5) {
        cells[1].textContent = status.name || 'N/A';
        cells[2].textContent = status.task || 'N/A';
        cells[3].textContent = status.context || '';
        cells[4].textContent = (status.duration !== undefined) ? status.duration.toFixed(1) : 'N/A';
    }
}

function updateLlmMetricsTable(allMetrics) {
     if (!llmMetricsTableBody || !allMetrics) return;
     llmMetricsTableBody.innerHTML = ''; // Clear
     allMetrics.forEach(metric => updateLlmMetricEntry(metric));
}

function updateLlmMetricEntry(metric) {
     if (!llmMetricsTableBody || !metric) return;
     const key = `${metric.provider}/${metric.model}`;
     let row = llmMetricsTableBody.querySelector(`tr[data-llm-key="${key}"]`);
     if (!row) {
          row = document.createElement('tr');
          row.setAttribute('data-llm-key', key);
          row.innerHTML = `<td>${key}</td><td></td><td></td><td></td><td></td>`;
          llmMetricsTableBody.appendChild(row);
     }
     const cells = row.querySelectorAll('td');
     if (cells.length >= 5) {
         cells[1].textContent = metric.calls || 0;
         cells[2].textContent = metric.errors || 0;
         cells[3].textContent = (metric.avg_latency_ms !== null && metric.avg_latency_ms !== undefined) ? metric.avg_latency_ms.toFixed(0) : 'N/A';
         cells[4].textContent = (metric.error_rate_pct !== null && metric.error_rate_pct !== undefined) ? metric.error_rate_pct.toFixed(1) + '%' : 'N/A';
     }
}


// --- Initialization ---
document.addEventListener('DOMContentLoaded', () => {
    connectWebSocket();
});