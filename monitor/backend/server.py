import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import asyncio

# Import the global metrics collector instance
from monitor.metrics import metrics_collector

logger = logging.getLogger(__name__)

# Create FastAPI app instance
app = FastAPI(title="News Agent Monitor")

# --- WebSocket Endpoint ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    await metrics_collector.register_websocket(websocket)
    logger.info("Monitor WebSocket client connected.")
    try:
        while True:
            # Keep connection alive, optionally handle client messages
            await websocket.receive_text() # Or receive_bytes, etc.
            # We primarily use server-push, so receiving might not be needed
    except WebSocketDisconnect:
        logger.info("Monitor WebSocket client disconnected.")
        metrics_collector.unregister_websocket(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}", exc_info=True)
        metrics_collector.unregister_websocket(websocket) # Clean up on error

# --- API Endpoint for Initial Data ---
@app.get("/api/initial_data")
async def get_initial_data():
    logger.debug("Serving initial data request.")
    return metrics_collector.get_initial_data()

# --- Serve Frontend Files ---
# Mount static files (JS, CSS)
app.mount("/js", StaticFiles(directory="monitor/frontend/js"), name="js")
# Serve index.html
@app.get("/")
async def get_index():
    return FileResponse("monitor/frontend/index.html")

# --- Lifespan Events (Start/Stop Background Tasks) ---
@app.on_event("startup")
async def startup_event():
    logger.info("Monitor backend starting up...")
    # Start background tasks from the metrics collector
    await metrics_collector.start_background_tasks()
    logger.info("Monitor background tasks started.")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Monitor backend shutting down...")
    # Stop background tasks
    await metrics_collector.stop_background_tasks()
    logger.info("Monitor background tasks stopped.")

# Optional: Add other API endpoints if needed later