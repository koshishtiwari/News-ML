import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
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

# --- Location Processing API Endpoint ---
class LocationRequest(BaseModel):
    location: str

@app.post("/api/process_location")
async def process_location(request: LocationRequest):
    """Process a location and start gathering news for it."""
    if not request.location or not request.location.strip():
        raise HTTPException(status_code=400, detail="Location cannot be empty")
    
    try:
        logger.info(f"Received request to process location: {request.location}")
        
        # Check if we have a reference to the news system
        if not hasattr(metrics_collector, 'news_system') or not metrics_collector.news_system:
            raise HTTPException(status_code=503, detail="News processing system not available")
            
        # Process the location asynchronously
        asyncio.create_task(metrics_collector.process_news_location(request.location))
        
        return {
            "status": "processing",
            "location": request.location,
            "message": f"Started processing location: {request.location}"
        }
    except Exception as e:
        logger.error(f"Error processing location '{request.location}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error processing location: {str(e)}")

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
    try:
        # Set the event loop for the metrics collector
        metrics_collector.set_loop(asyncio.get_running_loop())
        # Start background tasks from the metrics collector
        await metrics_collector.start_background_tasks()
        logger.info("Monitor background tasks started.")
    except Exception as e:
        logger.error(f"Failed to set up metrics_collector or start background tasks: {e}", exc_info=True)

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Monitor backend shutting down...")
    # Stop background tasks
    await metrics_collector.stop_background_tasks()
    logger.info("Monitor background tasks stopped.")

# Optional: Add other API endpoints if needed later