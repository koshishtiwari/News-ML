import logging
import asyncio
import time
from typing import Dict, Any, Optional, List, Set
from monitor.metrics import metrics_collector
from enum import Enum
import uuid

logger = logging.getLogger(__name__)

class AgentState(str, Enum):
    """Standard states for all agents"""
    INITIALIZING = "initializing"
    IDLE = "idle"
    WORKING = "working"
    ERROR = "error"
    RECOVERING = "recovering"
    SHUTTING_DOWN = "shutting_down"

class TaskPriority(int, Enum):
    """Priority levels for agent tasks"""
    CRITICAL = 0
    HIGH = 1
    MEDIUM = 2
    LOW = 3

class Task:
    """Represents a task that can be assigned to an agent"""
    def __init__(self, 
                 task_id: str, 
                 task_type: str, 
                 priority: TaskPriority = TaskPriority.MEDIUM,
                 data: Dict[str, Any] = None):
        self.task_id = task_id
        self.task_type = task_type
        self.priority = priority
        self.data = data or {}
        self.created_at = time.monotonic()
        self.started_at: Optional[float] = None
        self.completed_at: Optional[float] = None
        self.result: Any = None
        self.error: Optional[Exception] = None
        self.dependencies: Set[str] = set()  # IDs of tasks this task depends on
        self.dependents: Set[str] = set()    # IDs of tasks that depend on this task

    def start(self):
        """Mark task as started"""
        self.started_at = time.monotonic()
        
    def complete(self, result: Any = None):
        """Mark task as completed with result"""
        self.completed_at = time.monotonic()
        self.result = result
        
    def fail(self, error: Exception):
        """Mark task as failed with error"""
        self.completed_at = time.monotonic()
        self.error = error
        
    @property
    def is_completed(self) -> bool:
        return self.completed_at is not None and self.error is None
        
    @property
    def is_failed(self) -> bool:
        return self.error is not None
        
    @property
    def duration(self) -> Optional[float]:
        """Duration in seconds or None if not completed"""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return None
    
    @property
    def waiting_time(self) -> float:
        """Time waiting to be processed in seconds"""
        start = self.started_at or time.monotonic()
        return start - self.created_at


class BaseAgent:
    """Base class for all micro-agents with improved lifecycle management"""
    
    def __init__(self, agent_type: str):
        self.agent_id = f"{agent_type}-{uuid.uuid4().hex[:8]}"
        self.agent_type = agent_type
        self.state = AgentState.INITIALIZING
        self.current_task: Optional[Task] = None
        self.task_history: List[Task] = []
        self.last_error: Optional[Exception] = None
        self.metrics: Dict[str, Any] = {
            "tasks_completed": 0,
            "tasks_failed": 0,
            "total_processing_time": 0.0,
        }
        self.ready_event = asyncio.Event()
        self.shutdown_event = asyncio.Event()
        logger.info(f"Agent {self.agent_id} initialized")
        
    async def initialize(self):
        """Initialize agent resources asynchronously"""
        try:
            await self._initialize_resources()
            self.state = AgentState.IDLE
            self.ready_event.set()
            await self.update_status("Ready")
        except Exception as e:
            self.state = AgentState.ERROR
            self.last_error = e
            logger.error(f"Failed to initialize {self.agent_id}: {str(e)}", exc_info=True)
            await self.update_status("Initialization failed", {"error": str(e)})
            
    async def _initialize_resources(self):
        """Override this method to initialize specific resources"""
        pass
        
    async def shutdown(self):
        """Cleanup agent resources"""
        self.state = AgentState.SHUTTING_DOWN
        await self.update_status("Shutting down")
        try:
            await self._cleanup_resources()
            self.shutdown_event.set()
            await self.update_status("Shut down")
        except Exception as e:
            logger.error(f"Error during shutdown of {self.agent_id}: {str(e)}", exc_info=True)
            await self.update_status("Shutdown error", {"error": str(e)})

    async def _cleanup_resources(self):
        """Override this method to clean up specific resources"""
        pass
        
    async def process_task(self, task: Task) -> Any:
        """Process a task and return the result"""
        if self.state not in [AgentState.IDLE, AgentState.WORKING]:
            raise RuntimeError(f"Agent {self.agent_id} cannot accept tasks in state {self.state}")
            
        self.current_task = task
        task.start()
        self.state = AgentState.WORKING
        
        await self.update_status(f"Processing {task.task_type}", 
                               {"task_id": task.task_id, 
                                "priority": task.priority.name})
        
        try:
            result = await self._process_task_internal(task)
            task.complete(result)
            self.metrics["tasks_completed"] += 1
            self.metrics["total_processing_time"] += task.duration or 0
            self.state = AgentState.IDLE
            await self.update_status("Task completed", 
                                   {"task_id": task.task_id, 
                                    "duration": f"{task.duration:.2f}s" if task.duration else "unknown"})
            return result
        except Exception as e:
            task.fail(e)
            self.metrics["tasks_failed"] += 1
            self.last_error = e
            self.state = AgentState.ERROR
            logger.error(f"Error in {self.agent_id} processing task {task.task_id}: {str(e)}", exc_info=True)
            await self.update_status("Task failed", 
                                   {"task_id": task.task_id, 
                                    "error": str(e)})
            # Try to recover
            await self._try_recovery()
            raise
        finally:
            self.task_history.append(task)
            self.current_task = None
            
    async def _process_task_internal(self, task: Task) -> Any:
        """Override this method to implement task processing logic"""
        raise NotImplementedError("Subclasses must implement _process_task_internal")
        
    async def _try_recovery(self):
        """Attempt to recover from error state"""
        if self.state != AgentState.ERROR:
            return
            
        self.state = AgentState.RECOVERING
        await self.update_status("Attempting recovery")
        
        try:
            await self._recovery_procedure()
            self.state = AgentState.IDLE
            await self.update_status("Recovered")
        except Exception as e:
            logger.error(f"Recovery failed for {self.agent_id}: {str(e)}", exc_info=True)
            self.state = AgentState.ERROR
            await self.update_status("Recovery failed", {"error": str(e)})
            
    async def _recovery_procedure(self):
        """Override this to implement specific recovery logic"""
        # Default implementation resets resources
        try:
            await self._cleanup_resources()
        except:
            pass
        await self._initialize_resources()
        
    async def update_status(self, status: str, details: Dict[str, Any] = None):
        """Update this agent's status in the metrics collector"""
        details = details or {}
        details["agent_id"] = self.agent_id
        details["state"] = self.state.value
        
        await metrics_collector.update_agent_status(
            self.agent_type,
            status,
            details
        )
        logger.debug(f"{self.agent_id} status updated: {status} {details if details else ''}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get agent statistics"""
        return {
            "agent_id": self.agent_id,
            "agent_type": self.agent_type,
            "state": self.state.value,
            "current_task": self.current_task.task_id if self.current_task else None,
            "metrics": self.metrics,
            "last_error": str(self.last_error) if self.last_error else None,
        }
