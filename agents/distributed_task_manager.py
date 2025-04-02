import logging
import asyncio
import time
import json
import uuid
import datetime
from typing import Dict, List, Any, Optional, Set, Callable, Awaitable, Union, TypeVar, DefaultDict
from collections import defaultdict
import heapq
import redis.asyncio as redis
from dataclasses import dataclass, field, asdict

from .base_agent import BaseAgent, Task, TaskPriority, AgentState

logger = logging.getLogger(__name__)

T = TypeVar('T')

@dataclass
class TaskEvent:
    """Event related to a task for cross-agent communication"""
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    task_id: str = ""
    event_type: str = ""  # created, updated, completed, failed, etc.
    data: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TaskEvent':
        return cls(**data)

class DistributedTaskQueue:
    """
    Enhanced task queue with Redis persistence and real-time task updates
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self._tasks: Dict[str, Task] = {}
        self._blocking: DefaultDict[str, Set[str]] = defaultdict(set)
        self._priority_queue: List[Tuple[int, float, str]] = []  # (priority, timestamp, task_id)
        self._task_types: DefaultDict[str, Set[str]] = defaultdict(set)
        self._agent_task_counts: DefaultDict[str, int] = defaultdict(int)
        self._redis_url = redis_url
        self._redis: Optional[redis.Redis] = None
        self._pubsub_task: Optional[asyncio.Task] = None
        self._running = False
        self._event_handlers: Dict[str, List[Callable[[TaskEvent], Awaitable[None]]]] = defaultdict(list)
        
    async def initialize(self):
        """Initialize Redis connection and restore state"""
        self._redis = redis.Redis.from_url(self._redis_url)
        
        # Check connection
        try:
            await self._redis.ping()
            logger.info("Connected to Redis successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
            
        # Restore tasks from Redis
        await self._restore_state()
        
        # Start listening for events
        self._running = True
        self._pubsub_task = asyncio.create_task(self._listen_for_events())
        
    async def _restore_state(self):
        """Restore task state from Redis"""
        try:
            # Get all task IDs
            task_ids = await self._redis.smembers("tasks:all")
            
            restored_count = 0
            for task_id in task_ids:
                task_id = task_id.decode('utf-8')
                task_data = await self._redis.get(f"tasks:{task_id}")
                if task_data:
                    task_dict = json.loads(task_data)
                    
                    # Convert back to Task object
                    task = Task(
                        task_id=task_dict["task_id"],
                        task_type=task_dict["task_type"],
                        priority=TaskPriority(task_dict["priority"]),
                        data=task_dict["data"]
                    )
                    
                    # Restore task state
                    if task_dict.get("started_at"):
                        task.started_at = task_dict["started_at"]
                    if task_dict.get("completed_at"):
                        task.completed_at = task_dict["completed_at"]
                    if task_dict.get("result"):
                        task.result = task_dict["result"]
                    if task_dict.get("error"):
                        task.error = Exception(task_dict["error"])
                        
                    # Add to our in-memory structures
                    self._tasks[task_id] = task
                    
                    # Only add to priority queue if not completed/failed
                    if not task.is_completed and not task.is_failed:
                        created_at = task_dict.get("created_at", time.monotonic())
                        heapq.heappush(
                            self._priority_queue, 
                            (task.priority.value, created_at, task_id)
                        )
                        
                    # Update task type tracking
                    self._task_types[task.task_type].add(task_id)
                    
                    # Restore dependencies
                    for dep_id in task_dict.get("dependencies", []):
                        task.dependencies.add(dep_id)
                        self._blocking[dep_id].add(task_id)
                        
                    restored_count += 1
            
            logger.info(f"Restored {restored_count} tasks from Redis")
            
        except Exception as e:
            logger.error(f"Error restoring task state from Redis: {e}", exc_info=True)
    
    async def shutdown(self):
        """Shut down the queue and Redis connection"""
        self._running = False
        
        # Cancel the pubsub task
        if self._pubsub_task:
            self._pubsub_task.cancel()
            try:
                await self._pubsub_task
            except asyncio.CancelledError:
                pass
        
        # Close Redis connection
        if self._redis:
            await self._redis.close()
    
    async def _persist_task(self, task: Task):
        """Save task to Redis"""
        if not self._redis:
            logger.warning("Cannot persist task: Redis not initialized")
            return
            
        try:
            task_dict = {
                "task_id": task.task_id,
                "task_type": task.task_type,
                "priority": task.priority.value,
                "data": task.data,
                "created_at": task.created_at,
                "started_at": task.started_at,
                "completed_at": task.completed_at,
                "result": task.result,
                "error": str(task.error) if task.error else None,
                "dependencies": list(task.dependencies),
                "dependents": list(task.dependents),
            }
            
            # Save task data
            await self._redis.set(f"tasks:{task.task_id}", json.dumps(task_dict))
            
            # Add to all tasks set
            await self._redis.sadd("tasks:all", task.task_id)
            
            # Add to type index
            await self._redis.sadd(f"tasks:type:{task.task_type}", task.task_id)
            
            # Add to status indexes
            if task.is_completed:
                await self._redis.sadd("tasks:completed", task.task_id)
            elif task.is_failed:
                await self._redis.sadd("tasks:failed", task.task_id)
            else:
                await self._redis.sadd("tasks:pending", task.task_id)
                
        except Exception as e:
            logger.error(f"Error persisting task {task.task_id}: {e}", exc_info=True)
    
    async def _listen_for_events(self):
        """Listen for task events in Redis Pub/Sub"""
        if not self._redis:
            return
            
        try:
            pubsub = self._redis.pubsub()
            await pubsub.subscribe("task_events")
            
            while self._running:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message:
                    try:
                        event_data = json.loads(message["data"])
                        event = TaskEvent.from_dict(event_data)
                        
                        # Process event locally
                        await self._process_task_event(event)
                        
                    except json.JSONDecodeError:
                        logger.warning(f"Received invalid event data: {message['data']}")
                    except Exception as e:
                        logger.error(f"Error processing task event: {e}", exc_info=True)
                        
        except asyncio.CancelledError:
            logger.info("Event listener cancelled")
        except Exception as e:
            logger.error(f"Error in event listener: {e}", exc_info=True)
        finally:
            await pubsub.unsubscribe()
    
    async def _process_task_event(self, event: TaskEvent):
        """Process a task event and update local state"""
        task_id = event.task_id
        event_type = event.event_type
        
        if event_type == "created":
            # Task was created in another instance
            if task_id not in self._tasks:
                task_data = event.data.get("task")
                if task_data:
                    task = Task(
                        task_id=task_data["task_id"],
                        task_type=task_data["task_type"],
                        priority=TaskPriority(task_data["priority"]),
                        data=task_data["data"]
                    )
                    self._tasks[task_id] = task
                    heapq.heappush(
                        self._priority_queue, 
                        (task.priority.value, task.created_at, task_id)
                    )
                    self._task_types[task.task_type].add(task_id)
                    
        elif event_type == "completed":
            # Task was completed in another instance
            if task_id in self._tasks:
                task = self._tasks[task_id]
                task.complete(event.data.get("result"))
                
                # Handle dependent tasks
                await self._update_dependent_tasks(task_id)
                
        elif event_type == "failed":
            # Task failed in another instance
            if task_id in self._tasks:
                task = self._tasks[task_id]
                error_msg = event.data.get("error", "Unknown error")
                task.fail(Exception(error_msg))
                
                # Handle dependent tasks
                await self._process_task_failure(task_id, error_msg)
        
        # Trigger event handlers
        for handler in self._event_handlers[event_type]:
            try:
                await handler(event)
            except Exception as e:
                logger.error(f"Error in event handler for {event_type}: {e}", exc_info=True)
    
    async def _publish_event(self, event: TaskEvent):
        """Publish a task event to Redis Pub/Sub"""
        if not self._redis:
            return
            
        try:
            event_json = json.dumps(event.to_dict())
            await self._redis.publish("task_events", event_json)
        except Exception as e:
            logger.error(f"Error publishing event: {e}", exc_info=True)
    
    def add_task(self, task: Task, agent_id: Optional[str] = None) -> str:
        """Add a task to the queue"""
        task_id = task.task_id
        self._tasks[task_id] = task
        
        # Add to priority queue
        heapq.heappush(
            self._priority_queue, 
            (task.priority.value, task.created_at, task_id)
        )
        
        # Track by task type
        self._task_types[task.task_type].add(task_id)
        
        # Track assigned agent
        if agent_id:
            self._agent_task_counts[agent_id] += 1
            
        # Save to Redis
        asyncio.create_task(self._persist_task(task))
        
        # Publish event
        event = TaskEvent(
            task_id=task_id,
            event_type="created",
            data={"task": {
                "task_id": task.task_id,
                "task_type": task.task_type,
                "priority": task.priority.value,
                "data": task.data
            }}
        )
        asyncio.create_task(self._publish_event(event))
        
        return task_id
    
    def add_dependency(self, task_id: str, depends_on_id: str) -> bool:
        """Add a dependency between tasks"""
        if task_id not in self._tasks or depends_on_id not in self._tasks:
            return False
            
        # Add dependency
        self._tasks[task_id].dependencies.add(depends_on_id)
        self._tasks[depends_on_id].dependents.add(task_id)
        self._blocking[depends_on_id].add(task_id)
        
        # Update Redis
        asyncio.create_task(self._persist_task(self._tasks[task_id]))
        asyncio.create_task(self._persist_task(self._tasks[depends_on_id]))
        
        return True
    
    def get_next_task(self) -> Optional[Task]:
        """Get the next task based on priority and dependencies"""
        if not self._priority_queue:
            return None
            
        # Try to find a task without unmet dependencies
        candidates = []
        while self._priority_queue and len(candidates) < 5:  # Check up to 5 tasks
            priority, created_at, task_id = heapq.heappop(self._priority_queue)
            
            if task_id not in self._tasks:
                continue  # Task was removed
                
            task = self._tasks[task_id]
            if task.is_completed or task.is_failed:
                continue  # Task is already done
                
            # Check dependencies
            has_pending_dependencies = False
            for dep_id in task.dependencies:
                if dep_id in self._tasks and not self._tasks[dep_id].is_completed:
                    has_pending_dependencies = True
                    break
                    
            if not has_pending_dependencies:
                return task
                
            # If has dependencies, save for later
            candidates.append((priority, created_at, task_id))
            
        # Put candidates back in the queue
        for candidate in candidates:
            heapq.heappush(self._priority_queue, candidate)
            
        return None
    
    def mark_task_complete(self, task_id: str, result: Any = None, agent_id: Optional[str] = None):
        """Mark a task as completed and handle dependents"""
        if task_id not in self._tasks:
            logger.warning(f"Task {task_id} not found in queue")
            return
            
        task = self._tasks[task_id]
        task.complete(result)
        
        # Update agent task count
        if agent_id and agent_id in self._agent_task_counts:
            self._agent_task_counts[agent_id] -= 1
            
        # Update Redis
        asyncio.create_task(self._persist_task(task))
        
        # Handle dependent tasks
        asyncio.create_task(self._update_dependent_tasks(task_id))
        
        # Publish event
        event = TaskEvent(
            task_id=task_id,
            event_type="completed",
            data={"result": result}
        )
        asyncio.create_task(self._publish_event(event))
    
    async def _update_dependent_tasks(self, task_id: str):
        """Update tasks that depend on the completed task"""
        dependent_ids = list(self._blocking.get(task_id, set()))
        
        # Remove this task from blocking
        if task_id in self._blocking:
            del self._blocking[task_id]
            
        # For each dependent, check if all dependencies are met
        for dependent_id in dependent_ids:
            if dependent_id not in self._tasks:
                continue
                
            dependent_task = self._tasks[dependent_id]
            if dependent_task.is_completed or dependent_task.is_failed:
                continue
                
            # Check if all dependencies are now met
            all_deps_met = True
            for dep_id in dependent_task.dependencies:
                if dep_id in self._tasks and not self._tasks[dep_id].is_completed:
                    all_deps_met = False
                    break
                    
            if all_deps_met:
                # This dependent task is now ready to run
                # It's already in the priority queue, so it will be picked up
                pass
    
    def mark_task_failed(self, task_id: str, error: Exception, agent_id: Optional[str] = None):
        """Mark a task as failed and cascade failure to dependents if configured"""
        if task_id not in self._tasks:
            logger.warning(f"Task {task_id} not found in queue")
            return
            
        task = self._tasks[task_id]
        task.fail(error)
        
        # Track agent stats if provided
        if agent_id and agent_id in self._agent_task_counts:
            self._agent_task_counts[agent_id] -= 1
            
        # Update Redis
        asyncio.create_task(self._persist_task(task))
        
        # Process failure and handle dependents
        asyncio.create_task(self._process_task_failure(task_id, str(error)))
        
        # Publish event
        event = TaskEvent(
            task_id=task_id,
            event_type="failed",
            data={"error": str(error)}
        )
        asyncio.create_task(self._publish_event(event))
    
    async def _process_task_failure(self, task_id: str, error_msg: str):
        """Handle a task failure and its impact on dependent tasks"""
        # Get all dependents
        dependent_ids = list(self._blocking.get(task_id, set()))
        
        # Handle each dependent
        for dependent_id in dependent_ids:
            if dependent_id not in self._tasks:
                continue
                
            dependent_task = self._tasks[dependent_id]
            if not dependent_task.is_failed:
                # Mark dependent as failed too (cascading failure)
                dependent_task.fail(Exception(f"Required dependency {task_id} failed: {error_msg}"))
                
                # Update Redis
                await self._persist_task(dependent_task)
                
                # Recursively handle dependents of this task
                await self._process_task_failure(dependent_id, f"Cascading failure from {task_id}")
                
        # Clean up blocking tracking
        if task_id in self._blocking:
            del self._blocking[task_id]
    
    def register_event_handler(self, event_type: str, handler: Callable[[TaskEvent], Awaitable[None]]):
        """Register a handler for task events"""
        self._event_handlers[event_type].append(handler)
        
    def get_task(self, task_id: str) -> Optional[Task]:
        """Get a task by ID"""
        return self._tasks.get(task_id)
        
    def get_tasks_by_type(self, task_type: str) -> List[Task]:
        """Get all tasks of a specific type"""
        task_ids = self._task_types.get(task_type, set())
        return [self._tasks[tid] for tid in task_ids if tid in self._tasks]

class DistributedTaskManager:
    """
    Enhanced task manager with distributed capabilities using Redis for persistence
    and real-time coordination between instances.
    """
    
    def __init__(self, 
                redis_url: str = "redis://localhost:6379/0",
                max_concurrent_tasks: int = 10,
                max_retries: int = 2):
        # Core task management
        self.max_concurrent_tasks = max_concurrent_tasks
        self.task_queue = DistributedTaskQueue(redis_url=redis_url)
        self.running = False
        self._workers: List[asyncio.Task] = []
        self._worker_semaphore = asyncio.Semaphore(max_concurrent_tasks)
        
        # Agent management
        self._task_type_to_agents: Dict[str, List[BaseAgent]] = defaultdict(list)
        self._agent_to_task_types: Dict[str, List[str]] = defaultdict(list)
        self._all_agents: Dict[str, BaseAgent] = {}
        
        # Completion tracking
        self._task_results: Dict[str, Any] = {}
        self._task_errors: Dict[str, Exception] = {}
        self._task_completion_events: Dict[str, asyncio.Event] = {}
        
        # Task failure handling
        self._failed_task_retry_counts: Dict[str, int] = defaultdict(int)
        self._max_retries = max_retries
        
        # Advanced scheduling
        self._agent_health_scores: Dict[str, float] = {}  # 0.0 to 1.0, higher is healthier
        self._agent_performance_metrics: Dict[str, Dict[str, Any]] = defaultdict(dict)
        
        # Load balancing
        self._round_robin_indexes: Dict[str, int] = defaultdict(int)  # task_type -> current index
        
        # For monitoring
        self._task_latencies: List[float] = []  # track task execution times
        
        # Instance ID for distributed coordination
        self.instance_id = str(uuid.uuid4())
        
        logger.info(f"Distributed Task Manager initialized with {max_concurrent_tasks} max concurrent tasks")
    
    async def initialize(self):
        """Initialize the task manager and restore state"""
        # Initialize Redis task queue
        await self.task_queue.initialize()
        
        # Register event handlers
        self.task_queue.register_event_handler("completed", self._handle_task_completion_event)
        self.task_queue.register_event_handler("failed", self._handle_task_failure_event)
        
        # Start worker tasks
        self.running = True
        self._workers = [
            asyncio.create_task(self._worker_loop())
            for _ in range(self.max_concurrent_tasks)
        ]
        
        logger.info(f"Task Manager {self.instance_id} started with {len(self._workers)} workers")
    
    async def _handle_task_completion_event(self, event: TaskEvent):
        """Handle task completion events from other instances"""
        task_id = event.task_id
        
        # If we have a completion event for this task, notify waiters
        if task_id in self._task_completion_events:
            self._task_results[task_id] = event.data.get("result")
            self._task_completion_events[task_id].set()
    
    async def _handle_task_failure_event(self, event: TaskEvent):
        """Handle task failure events from other instances"""
        task_id = event.task_id
        
        # If we have a completion event for this task, notify waiters of failure
        if task_id in self._task_completion_events:
            error_msg = event.data.get("error", "Unknown error")
            self._task_errors[task_id] = Exception(error_msg)
            self._task_completion_events[task_id].set()
    
    async def shutdown(self):
        """Stop all worker tasks and clean up"""
        logger.info(f"Task Manager {self.instance_id} shutting down...")
        self.running = False
        
        # Stop workers
        if self._workers:
            for worker in self._workers:
                worker.cancel()
            await asyncio.gather(*self._workers, return_exceptions=True)
            
        # Shut down task queue
        await self.task_queue.shutdown()
        
        logger.info(f"Task Manager {self.instance_id} shut down complete")
    
    def register_agent_for_task_types(self, agent: BaseAgent, task_types: List[str]):
        """Register an agent to handle specific task types"""
        agent_id = agent.agent_id
        
        # Store the agent
        self._all_agents[agent_id] = agent
        
        # Initialize health score
        self._agent_health_scores[agent_id] = 1.0
        
        # Map agent to task types
        for task_type in task_types:
            if agent not in self._task_type_to_agents[task_type]:
                self._task_type_to_agents[task_type].append(agent)
                
        # Map task types to agent
        self._agent_to_task_types[agent_id] = task_types
        
        logger.info(f"Agent {agent_id} registered for task types: {', '.join(task_types)}")
    
    async def create_task(self, 
                       task_type: str, 
                       data: Dict[str, Any] = None, 
                       priority: TaskPriority = TaskPriority.MEDIUM,
                       dependencies: List[str] = None) -> str:
        """
        Create a new task and add it to the queue.
        Returns the task ID.
        """
        task_id = str(uuid.uuid4())
        task = Task(task_id, task_type, priority, data or {})
        
        # Add to queue
        self.task_queue.add_task(task)
        
        # Set up completion event
        self._task_completion_events[task_id] = asyncio.Event()
        
        # Add dependencies if provided
        if dependencies:
            for dep_id in dependencies:
                self.task_queue.add_dependency(task_id, dep_id)
        
        logger.info(f"Created task {task_id} of type {task_type} with priority {priority.name}")
        return task_id
    
    async def wait_for_task_completion(self, task_id: str, timeout: Optional[float] = None) -> bool:
        """
        Wait for a task to complete. Returns True if completed, False if timed out.
        If the task is already completed, returns immediately.
        """
        if task_id not in self._task_completion_events:
            task = self.task_queue.get_task(task_id)
            if not task:
                raise ValueError(f"Task {task_id} not found")
                
            if task.is_completed or task.is_failed:
                # Task already done
                if task.is_completed:
                    self._task_results[task_id] = task.result
                else:
                    self._task_errors[task_id] = task.error
                return True
                
            # Set up event
            self._task_completion_events[task_id] = asyncio.Event()
        
        # Wait for completion
        try:
            await asyncio.wait_for(self._task_completion_events[task_id].wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False
    
    async def get_task_result(self, task_id: str, timeout: Optional[float] = None) -> Any:
        """
        Get the result of a completed task. If the task is not completed yet,
        waits for it to complete up to the specified timeout.
        Raises an exception if the task failed or timed out.
        """
        # Wait for task to complete
        completed = await self.wait_for_task_completion(task_id, timeout)
        if not completed:
            raise asyncio.TimeoutError(f"Task {task_id} did not complete within timeout")
            
        # Check for errors
        if task_id in self._task_errors:
            raise self._task_errors[task_id]
            
        # Return result
        if task_id in self._task_results:
            return self._task_results[task_id]
            
        # If no result stored but task completed, fetch from queue
        task = self.task_queue.get_task(task_id)
        if task and task.is_completed:
            return task.result
            
        # Should never happen, but just in case
        raise Exception(f"Task {task_id} completed but has no result")
    
    async def _worker_loop(self):
        """Worker loop that processes tasks"""
        try:
            while self.running:
                # Try to acquire a semaphore
                async with self._worker_semaphore:
                    # Get next task
                    task = self.task_queue.get_next_task()
                    
                    if not task:
                        # No tasks available, wait a bit
                        await asyncio.sleep(0.1)
                        continue
                        
                    # Process the task
                    asyncio.create_task(self._process_task(task))
                    
                # Small sleep to prevent tight loop
                await asyncio.sleep(0.01)
                
        except asyncio.CancelledError:
            logger.debug("Worker task cancelled")
        except Exception as e:
            logger.error(f"Error in worker loop: {e}", exc_info=True)
    
    async def _process_task(self, task: Task) -> None:
        """Process a task by finding an appropriate agent and executing it"""
        task_id = task.task_id
        task_type = task.task_type
        
        logger.info(f"Processing task {task_id} of type {task_type}")
        
        start_time = time.monotonic()
        
        try:
            # Select an agent for this task
            agent = await self._select_agent_for_task(task)
            
            if not agent:
                logger.warning(f"No agent available for task {task_id} ({task_type})")
                await asyncio.sleep(1)  # Wait a bit before retrying
                return  # Task remains in queue
                
            agent_id = agent.agent_id
            
            # Execute the task
            logger.info(f"Assigning task {task_id} to agent {agent_id}")
            
            result = await agent.process_task(task)
            
            # Record successful completion
            execution_time = time.monotonic() - start_time
            self._task_latencies.append(execution_time)
            
            # Update agent performance metrics
            self._update_agent_performance(
                agent_id, 
                execution_time, 
                True, 
                task_type
            )
            
            # Mark task as complete
            self.task_queue.mark_task_complete(task_id, result, agent_id)
            
            # Store result for anyone waiting
            self._task_results[task_id] = result
            
            # Notify any waiters
            if task_id in self._task_completion_events:
                self._task_completion_events[task_id].set()
                
        except Exception as e:
            error_time = time.monotonic() - start_time
            logger.error(f"Error processing task {task_id}: {e}", exc_info=True)
            
            # Update agent performance if we know which agent was used
            agent_id = getattr(task, "assigned_agent", None)
            if agent_id:
                self._update_agent_performance(
                    agent_id,
                    error_time,
                    False,
                    task_type,
                    str(e)
                )
            
            # Handle retry logic
            retry_success = await self._handle_task_retry(task, e, agent_id)
            
            if not retry_success:
                # Mark as failed if we're not retrying
                self._handle_task_failure(task_id, e)
    
    async def _select_agent_for_task(self, task: Task) -> Optional[BaseAgent]:
        """Select the best agent for a task using load balancing and health metrics"""
        task_type = task.task_type
        agents = self._task_type_to_agents.get(task_type, [])
        
        if not agents:
            logger.warning(f"No agents registered for task type {task_type}")
            return None
            
        # Filter out unavailable agents (e.g., in ERROR state)
        available_agents = [a for a in agents if a.state in [AgentState.IDLE, AgentState.WORKING]]
        
        if not available_agents:
            logger.warning(f"No available agents for task type {task_type}")
            return None
            
        if len(available_agents) == 1:
            # Only one agent available, use it
            return available_agents[0]
            
        # Load balancing strategies:
        
        # 1. Weighted random selection based on agent health and load
        import random
        if random.random() < 0.7:  # 70% of the time use weighted health selection
            weighted_agents = []
            for agent in available_agents:
                # Calculate weight based on health score
                health = self._agent_health_scores.get(agent.agent_id, 0.5)
                
                # Penalize busy agents
                if agent.state == AgentState.WORKING:
                    health *= 0.7
                    
                # Add to weighted list
                weighted_agents.append((agent, health))
                
            # Normalize weights
            total_weight = sum(w for _, w in weighted_agents)
            if total_weight > 0:
                normalized_weights = [(a, w/total_weight) for a, w in weighted_agents]
                
                # Select agent based on weights
                r = random.random()
                cumulative = 0
                for agent, weight in normalized_weights:
                    cumulative += weight
                    if r <= cumulative:
                        return agent
            
            # Fallback if weights don't work out
            return random.choice(available_agents)
            
        # 2. Round-robin selection (30% of the time)
        else:
            idx = self._round_robin_indexes[task_type] % len(available_agents)
            self._round_robin_indexes[task_type] += 1
            return available_agents[idx]
    
    def _update_agent_performance(
        self, 
        agent_id: str, 
        execution_time: float, 
        is_success: bool, 
        task_type: str,
        error: Optional[str] = None
    ):
        """Update agent performance metrics and health score"""
        if agent_id not in self._agent_performance_metrics:
            self._agent_performance_metrics[agent_id] = {
                "success_count": 0,
                "error_count": 0,
                "total_time": 0.0,
                "avg_time": 0.0,
                "last_errors": [],
                "task_counts": defaultdict(int),
                "success_rate": 1.0,
            }
            
        metrics = self._agent_performance_metrics[agent_id]
        
        # Update basic counters
        if is_success:
            metrics["success_count"] += 1
        else:
            metrics["error_count"] += 1
            metrics["last_errors"].append({
                "error": str(error),
                "task_type": task_type,
                "timestamp": time.time()
            })
            # Keep only last 10 errors
            metrics["last_errors"] = metrics["last_errors"][-10:]
            
        # Update timing metrics
        metrics["total_time"] += execution_time
        total_tasks = metrics["success_count"] + metrics["error_count"]
        metrics["avg_time"] = metrics["total_time"] / total_tasks if total_tasks > 0 else 0
        
        # Update task type counter
        metrics["task_counts"][task_type] += 1
        
        # Calculate success rate
        metrics["success_rate"] = (
            metrics["success_count"] / total_tasks if total_tasks > 0 else 1.0
        )
        
        # Update health score - weighted combination of success rate and relative speed
        # This will be between 0.0 and 1.0, higher is better
        success_weight = 0.7  # Success rate is more important than speed
        
        # Get median task time for this type for comparison
        median_time = self._get_median_task_time(task_type) or execution_time
        
        # Calculate speed factor (faster than median = better score)
        speed_factor = min(1.0, median_time / max(0.001, execution_time))
        
        # Calculate new health score
        new_health = (
            metrics["success_rate"] * success_weight + 
            speed_factor * (1 - success_weight)
        )
        
        # Use exponential moving average for health score
        alpha = 0.3  # Weight for new observation
        current_health = self._agent_health_scores.get(agent_id, 0.5)
        self._agent_health_scores[agent_id] = current_health * (1 - alpha) + new_health * alpha
    
    def _get_median_task_time(self, task_type: str) -> Optional[float]:
        """Get the median execution time for tasks of a given type"""
        # In a real implementation, this would use a time-series database
        # For now, we'll just use the overall median
        if not self._task_latencies:
            return None
            
        sorted_times = sorted(self._task_latencies)
        mid = len(sorted_times) // 2
        
        if len(sorted_times) % 2 == 0:
            return (sorted_times[mid-1] + sorted_times[mid]) / 2
        else:
            return sorted_times[mid]
    
    def _handle_task_failure(self, task_id: str, error: Exception):
        """Handle a failed task by recording error and notifying waiters"""
        self._task_errors[task_id] = error
        self.task_queue.mark_task_failed(task_id, error)
        
        # Set completion event to unblock any waiters
        if task_id in self._task_completion_events:
            self._task_completion_events[task_id].set()
            
        logger.error(f"Task {task_id} failed: {str(error)}")
    
    async def _handle_task_retry(self, task: Task, error: Exception, agent_id: str) -> bool:
        """
        Handle task retry logic - returns True if the task was retried,
        False if it should be marked as failed
        """
        task_id = task.task_id
        task_type = task.task_type
        
        # Check current retry count
        retry_count = self._failed_task_retry_counts[task_id]
        
        if retry_count >= self._max_retries:
            logger.warning(f"Task {task_id} failed after {retry_count} retries, giving up")
            return False
            
        # Increment retry count
        self._failed_task_retry_counts[task_id] += 1
        new_retry_count = self._failed_task_retry_counts[task_id]
        
        # Log retry
        logger.info(f"Retrying task {task_id} (attempt {new_retry_count}/{self._max_retries})")
        
        # Wait a bit before retrying (exponential backoff)
        backoff_seconds = min(2 ** (new_retry_count - 1), 30)  # Max 30 second backoff
        await asyncio.sleep(backoff_seconds)
        
        # Create a new task entry with incremented priority
        new_priority = TaskPriority(max(task.priority.value - 1, TaskPriority.CRITICAL.value))  # Boost priority
        new_task = Task(task_id, task_type, new_priority, task.data)
        
        # Add to queue with dependencies cleared (to avoid deadlock)
        self.task_queue.add_task(new_task)
        
        # Clear cached agent from this task to try a different agent
        try:
            agents = self._task_type_to_agents.get(task_type, [])
            if len(agents) > 1 and agent_id:
                # Try to assign to a different agent by artificially lowering the health
                # of the previous agent for this task type temporarily
                old_health = self._agent_health_scores.get(agent_id, 0.5)
                self._agent_health_scores[agent_id] = old_health * 0.5  # Temporary penalty
                
                # Schedule a task to restore health after a delay
                async def restore_health():
                    await asyncio.sleep(10)  # Wait 10 seconds
                    self._agent_health_scores[agent_id] = old_health
                    
                asyncio.create_task(restore_health())
        except Exception as e:
            logger.warning(f"Error adjusting agent health for retry: {e}")
        
        return True
    
    def _log_worker_status(self):
        """Log the current status of workers and task queue"""
        task_count = len(self._task_completion_events)
        pending_count = len([1 for e in self._task_completion_events.values() if not e.is_set()])
        
        logger.info(f"Workers: {len(self._workers)}, Tasks: {task_count}, Pending: {pending_count}")
    
    async def get_status(self) -> Dict[str, Any]:
        """Get detailed status of the task manager"""
        agent_statuses = {}
        for agent_id, agent in self._all_agents.items():
            agent_statuses[agent_id] = {
                "state": agent.state.value,
                "health": self._agent_health_scores.get(agent_id, 0.0),
                "task_types": self._agent_to_task_types.get(agent_id, []),
                "metrics": self._agent_performance_metrics.get(agent_id, {})
            }
            
        return {
            "instance_id": self.instance_id,
            "workers": len(self._workers),
            "running": self.running,
            "agents": agent_statuses,
            "task_count": len(self._task_completion_events),
            "pending_tasks": len([1 for e in self._task_completion_events.values() if not e.is_set()])
        }
    
    def _get_agent_success_rate(self, agent_id: str) -> float:
        """Calculate the success rate for an agent"""
        metrics = self._agent_performance_metrics.get(agent_id, {})
        success_count = metrics.get("success_count", 0)
        error_count = metrics.get("error_count", 0)
        
        total = success_count + error_count
        if total == 0:
            return 1.0
            
        return success_count / total