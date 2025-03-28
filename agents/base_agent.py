import logging
import asyncio
from typing import Dict, Any
from monitor.metrics import metrics_collector

logger = logging.getLogger(__name__)

class BaseAgent:
    """Base class for all agents with status tracking"""
    
    def __init__(self, agent_type: str):
        self.agent_type = agent_type
        self.update_status("Initialized")
    
    def update_status(self, status: str, details: Dict[str, Any] = None):
        """Update this agent's status in the metrics collector - sync version"""
        try:
            # If we're in an event loop, use it
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(self._update_status_async(status, details))
            else:
                # Otherwise run a new event loop just for this task
                asyncio.run(self._update_status_async(status, details))
        except RuntimeError:
            # If there's no event loop, log it but don't fail
            logger.debug(f"{self.__class__.__name__} status update queued: {status}")
            # We can't directly create a task without a running loop
            pass
    
    async def update_status_async(self, status: str, details: Dict[str, Any] = None):
        """Async version of status update that can be awaited"""
        await self._update_status_async(status, details)
    
    async def _update_status_async(self, status: str, details: Dict[str, Any] = None):
        """Async implementation of status update"""
        await metrics_collector.update_agent_status(self.agent_type, status, details)
        logger.debug(f"{self.__class__.__name__} status updated: {status}")
