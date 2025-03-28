// app.js - Basic Frontend Logic for News Agent Monitor

const MAX_CHART_POINTS = 180; // Max data points to display on charts (e.g., 30 minutes if 1 point per 10s)
const MAX_LOG_LINES = 200;
const MAX_ARTICLES = 20; // Maximum number of articles to show in the list

// DOM Elements
const wsStatusElem = document.getElementById('ws-status'); // Add this element to HTML if needed
const systemStatusElem = document.getElementById('system-status');
const activeLocationElem = document.getElementById('active-location');
const logPanel = document.getElementById('log-panel');
const errorPanel = document.getElementById('error-panel');
const funnelDisplay = document.getElementById('funnel-display');
const agentStatusTableBody = document.getElementById('agent-status-table')?.querySelector('tbody');
const llmMetricsTableBody = document.getElementById('llm-metrics-table')?.querySelector('tbody');
const articlesDisplay = document.getElementById('articles-display');
const locationInput = document.getElementById('location-input');
const locationSubmit = document.getElementById('location-submit');

// Articles storage
let currentArticles = [];

// Chart instances (initialize after fetching initial data)
let resourceChart = null;
let rateChart = null;
// Add more chart variables

// --- WebSocket Connection ---
let socket = null;
let reconnectAttempt = 0;
let reconnectTimer = null;

function connectWebSocket() {
    // Clear any existing reconnect timer
    if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
    }

    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${wsProtocol}//${window.location.host}/ws`;
    console.log(`Connecting to WebSocket: ${wsUrl}`);

    socket = new WebSocket(wsUrl);

    socket.onopen = (event) => {
        console.log("WebSocket connected.");
        // wsStatusElem.textContent = 'Connected';
        // wsStatusElem.style.color = 'green';
        fetchInitialData(); // Fetch historical data on connect
        reconnectAttempt = 0; // Reset on successful connect
    };

    socket.onmessage = (event) => {
        try {
            const message = JSON.parse(event.data);
            // Debug logs can help troubleshooting
            if (message.type === 'agent_status') {
                console.debug("Agent status update received:", message.data);
            }
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

        // Simple exponential backoff reconnect with max delay
        const delay = Math.min(1000 * (2 ** reconnectAttempt), 30000);
        console.log(`Will reconnect in ${delay / 1000}s`);

        reconnectTimer = setTimeout(connectWebSocket, delay);
        reconnectAttempt++;
    };
}

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

    // Initialize Articles if any
    if (data.articles && data.articles.length > 0) {
        updateArticles(data.articles);
    }
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

    // Data should be in message.data, with type in message.type
    const type = message.type;
    const payload = message.data;

    if (!type || !payload) {
        console.warn("Received invalid message format:", message);
        return;
    }

    switch (type) {
        case 'system_status':
            updateOverview(payload);
            break;

        case 'resource_update':
            updateChartData(resourceChart, payload.timestamp * 1000, [payload.cpu_percent, payload.memory_percent]);
            break;

        case 'rate_update':
            updateChartData(rateChart, payload.timestamp * 1000, [payload.articles_per_sec, payload.errors_per_sec]);
            break;

        case 'log':
            addLogEntry(payload, logPanel);
            break;

        case 'error':
            addLogEntry(payload, errorPanel);
            break;

        case 'funnel_update':
            updateFunnel(payload);
            break;

        case 'agent_status':
            // Check if we have both agent_id and status properties
            if (payload.agent_id && payload.status) {
                updateAgentStatusEntry(payload.agent_id, payload.status);
            } else {
                console.warn("Invalid agent status update format:", payload);
            }
            break;

        case 'llm_metric':
            updateLlmMetricEntry(payload);
            break;

        case 'article_update':
            if (payload.article) {
                updateSingleArticle(payload.article);
            }
            break;

        case 'articles_update':
            if (payload.articles) {
                updateArticles(payload.articles);
            }
            break;

        case 'processing_time':
            // Add to processing time chart data if implemented
            console.log(`Processing time recorded: ${payload.duration.toFixed(2)}s`);
            break;

        case 'stage_timing':
            // Add to stage timing chart data if implemented
            console.log(`Stage ${payload.stage} time: ${payload.duration.toFixed(2)}s`);
            break;

        default:
            console.warn("Received unknown WS message type:", type);
    }
}

// Function to handle a single article update
function updateSingleArticle(article) {
    if (!articlesDisplay || !article) return;

    console.log("Received single article update:", article.title);

    // Check if article with same URL already exists
    let existingIndex = -1;
    for (let i = 0; i < currentArticles.length; i++) {
        if (currentArticles[i].url === article.url) {
            existingIndex = i;
            break;
        }
    }

    if (existingIndex !== -1) {
        // Update existing article
        currentArticles[existingIndex] = article;
    } else {
        // Add new article to the list
        currentArticles.push(article);
    }

    // Re-sort and update the display
    updateArticles(currentArticles);
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

    // Convert the status object to an array of [agentId, statusObj] entries 
    // for easier iteration and updating
    Object.entries(allStatus).forEach(([agentId, status]) => {
        updateAgentStatusEntry(agentId, status); // Reuse single entry update logic
    });
}

function updateAgentStatusEntry(agentId, status) {
    if (!agentStatusTableBody || !status) return;

    // Logging to help debug
    console.debug(`Updating agent status for ${agentId}:`, status);

    // Find existing row or create new one
    let row = agentStatusTableBody.querySelector(`tr[data-agent-id="${agentId}"]`);

    if (!row) {
        row = document.createElement('tr');
        row.setAttribute('data-agent-id', agentId);
        // Create table row with cells
        row.innerHTML = `<td>${agentId}</td><td></td><td></td><td></td><td></td>`;
        agentStatusTableBody.appendChild(row);
    }

    // Update cell content
    const cells = row.querySelectorAll('td');
    if (cells.length >= 5) {
        cells[0].textContent = agentId; // Full agent ID
        cells[1].textContent = status.name || 'N/A';
        cells[2].textContent = status.task || 'N/A';
        cells[3].textContent = status.context || '';

        // Calculate duration from current time if not provided
        let duration = status.duration;
        if (duration === undefined || duration === null) {
            if (status.timestamp) {
                duration = (Date.now() / 1000) - status.timestamp;
            } else {
                duration = 0;
            }
        }

        cells[4].textContent = duration.toFixed(1) + 's';

        // Add visual indicator for active/idle status
        if (status.task && status.task.toLowerCase() !== 'idle') {
            row.classList.add('is-active');
            cells[2].classList.add('has-text-weight-bold');
        } else {
            row.classList.remove('is-active');
            cells[2].classList.remove('has-text-weight-bold');
        }
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

// --- Article Display Functions ---
function updateArticles(articles) {
    if (!articlesDisplay || !articles) return;

    // Store articles in our cache
    currentArticles = articles;

    // Sort articles by importance and recency
    const importanceOrder = {
        'Critical': 0,
        'High': 1,
        'Medium': 2,
        'Low': 3
    };

    articles.sort((a, b) => {
        // First sort by importance
        const importanceA = importanceOrder[a.importance] || 4;
        const importanceB = importanceOrder[b.importance] || 4;

        if (importanceA !== importanceB) {
            return importanceA - importanceB;
        }

        // Then by recency (fetched_at or published_at)
        const dateA = new Date(a.fetched_at || a.published_at || 0);
        const dateB = new Date(b.fetched_at || b.published_at || 0);
        return dateB - dateA; // Newer first
    });

    // Clear and rebuild the display
    articlesDisplay.innerHTML = '';

    if (articles.length === 0) {
        articlesDisplay.innerHTML = '<div class="has-text-centered">No articles to display yet</div>';
        return;
    }

    // Take only the most recent MAX_ARTICLES
    const displayArticles = articles.slice(0, MAX_ARTICLES);

    displayArticles.forEach(article => {
        const card = createArticleCard(article);
        articlesDisplay.appendChild(card);
    });

    // Optionally animate new articles
    const allCards = articlesDisplay.querySelectorAll('.article-card');
    if (allCards.length > 0) {
        const newestCard = allCards[0]; // First card is newest due to sort
        newestCard.classList.add('new-article');
        setTimeout(() => {
            newestCard.classList.remove('new-article');
        }, 1500);
    }

    // Auto-scroll to top
    articlesDisplay.scrollTop = 0;
}

// Add a CSS class for new article animation
function addArticleAnimationStyles() {
    const styleEl = document.createElement('style');
    styleEl.textContent = `
        @keyframes highlightNew {
            0% { background-color: rgba(121, 187, 255, 0.3); }
            100% { background-color: transparent; }
        }
        .article-card.new-article {
            animation: highlightNew 1.5s ease-out;
        }
        .article-status-processing {
            color: orange;
            font-style: italic;
        }
        .article-status-complete {
            color: green;
        }
    `;
    document.head.appendChild(styleEl);
}

// Enhanced article card creation with status indicators
function createArticleCard(article) {
    const card = document.createElement('div');
    card.className = 'article-card';
    card.setAttribute('data-url', article.url);

    // Format dates
    const publishedDate = article.published_at ? formatDate(article.published_at) : 'Unknown';
    const fetchedDate = article.fetched_at ? formatDate(article.fetched_at) : 'Unknown';

    // Determine if article has a complete analysis
    const hasFullAnalysis = article.summary && article.summary !== 'Content unavailable.' && 
                           !article.summary.includes('Summary generation failed');

    // Status indicator
    let statusHtml = '';
    if (!hasFullAnalysis) {
        statusHtml = '<span class="article-status-processing">Processing...</span>';
    } else {
        statusHtml = '<span class="article-status-complete">✓</span>';
    }

    // Create HTML content
    card.innerHTML = `
        <h3 class="article-title">${article.title || 'Untitled Article'} ${statusHtml}</h3>
        <div class="article-meta">
            <span>Source: ${article.source_name || 'Unknown'} | Importance: <span class="importance-${article.importance}">${article.importance || 'Unknown'}</span> | Category: ${article.category || 'Uncategorized'}</span>
        </div>
        <div class="article-meta">
            <span>Published: ${publishedDate} | Fetched: ${fetchedDate} UTC</span>
        </div>
        <div class="article-summary">
            <strong>Summary:</strong> ${article.summary || 'Processing article content...'}
        </div>
        <div class="article-meta mt-2">
            <a href="${article.url}" target="_blank">View Source</a>
        </div>
    `;

    return card;
}

// Helper function to format dates nicely
function formatDate(dateString) {
    if (!dateString) return 'Unknown';

    try {
        const date = new Date(dateString);
        return date.toISOString().split('T')[0] + ' ' +
            date.toTimeString().split(' ')[0].substring(0, 5);
    } catch (e) {
        return dateString;
    }
}

// --- Location Submission ---
async function processLocation(location) {
    if (!location || location.trim() === '') return;

    try {
        // Show "processing" state
        systemStatusElem.textContent = 'Processing';
        systemStatusElem.style.color = 'orange';

        // Clear existing articles
        articlesDisplay.innerHTML = '<div class="has-text-centered">Processing location, please wait...</div>';

        // Send the location to process
        const response = await fetch('/api/process_location', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ location: location.trim() })
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const result = await response.json();
        console.log('Location processing initiated:', result);

        // Rest of the updates will come through WebSocket

    } catch (e) {
        console.error('Error processing location:', e);
        articlesDisplay.innerHTML = `<div class="has-text-centered has-text-danger">Error processing location: ${e.message}</div>`;
        systemStatusElem.textContent = 'Error';
        systemStatusElem.style.color = 'red';
    }
}

// --- Initialization ---
document.addEventListener('DOMContentLoaded', () => {
    // Add animation styles
    addArticleAnimationStyles();

    connectWebSocket();

    // Set up location submission handlers
    if (locationInput && locationSubmit) {
        // Submit on button click
        locationSubmit.addEventListener('click', () => {
            processLocation(locationInput.value);
        });

        // Submit on Enter key
        locationInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                processLocation(locationInput.value);
            }
        });
    }
});