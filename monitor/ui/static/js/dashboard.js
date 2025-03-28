// Add this function to fetch and display agent statuses

async function updateAgentStatus() {
    try {
        const response = await fetch('/api/agent-status');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        
        // Update the UI with agent statuses
        const statusContainer = document.getElementById('agent-status-container');
        if (!statusContainer) return;
        
        let statusHTML = '<h3>Agent Status</h3><div class="agent-status-grid">';
        
        for (const [agent, status] of Object.entries(data)) {
            const statusClass = status.status.toLowerCase() === 'idle' ? 'status-idle' : 'status-active';
            const lastUpdateTime = new Date(status.last_update * 1000).toLocaleTimeString();
            
            statusHTML += `
                <div class="agent-status-card ${statusClass}">
                    <h4>${agent.charAt(0).toUpperCase() + agent.slice(1)}</h4>
                    <p>Status: ${status.status}</p>
                    <p>Last Update: ${lastUpdateTime}</p>
                    ${status.details ? `<p>Details: ${JSON.stringify(status.details)}</p>` : ''}
                </div>
            `;
        }
        
        statusHTML += '</div>';
        statusContainer.innerHTML = statusHTML;
    } catch (error) {
        console.error('Error updating agent status:', error);
    }
}

// Add to your existing refresh cycle
setInterval(updateAgentStatus, 5000);  // Update every 5 seconds

// Call on page load
document.addEventListener('DOMContentLoaded', function() {
    updateAgentStatus();
    // ...existing init code...
});
