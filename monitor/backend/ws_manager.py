import logging
from typing import Set, Any
from weakref import WeakSet

logger = logging.getLogger(__name__)

# Note: In a simple setup, the MetricsCollector directly holds the WeakSet.
# This manager might be useful if connection logic becomes more complex later.
# For now, we can let MetricsCollector manage the set directly.
# This file can be removed or kept as a placeholder if preferred.

# class WebSocketManager:
#     def __init__(self):
#         self.active_connections: WeakSet = WeakSet()
#         logger.info("WebSocketManager initialized.")

#     async def connect(self, websocket: Any):
#         await websocket.accept()
#         self.active_connections.add(websocket)
#         logger.info(f"WebSocket connected. Total: {len(self.active_connections)}")

#     def disconnect(self, websocket: Any):
#         self.active_connections.discard(websocket)
#         logger.info(f"WebSocket disconnected. Total: {len(self.active_connections)}")

#     async def broadcast(self, message: str):
#         # asyncio.gather(*(ws.send_text(message) for ws in self.active_connections))
#         # Use the broadcast method in MetricsCollector instead to keep connections centralized
#         pass