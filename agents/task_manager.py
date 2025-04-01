import logging
import asyncio
import time
import uuid
from typing import Dict, List, Any, Optional, Set, Callable, Awaitable, Union, TypeVar
from collections import defaultdict
import heapq

from .base_agent import BaseAgent, Task, TaskPriority, AgentState
from monitor.metrics import metrics_collector

logger = logging.getLogger(__name__)

T = TypeVar('T')

class TaskQueue:
    """Priority queue for tasks with dependency resolution"""
    
    def __init__(self):
        self._queue = []  # heap for priority queue
        self._tasks: Dict[str, Task] = {}  # task_id -> Task
        self._waiting_on_deps: Dict[str, Set[str]] = defaultdict(set)  # task_id -> set of dependency task_ids
        self._blocking: Dict[str, Set[str]] = defaultdict(set)  # task_id -> set of tasks waiting on this
        self._counter = 0  # tie breaker for same priority items
        
    def add_task(self, task: Task):
        """Add a task to the queue, taking dependencies into account"""
        if task.task_id in self._tasks:
            logger.warning(f"Task {task.task_id} already in queue, skipping")
            return
            
        self._tasks[task.task_id] = task
        
        # Check if task has dependencies
        if task.dependencies:
            unmet_deps = set()
            for dep_id in task.dependencies:
                # If dependency exists and is not completed
                if dep_id in self._tasks and not self._tasks[dep_id].is_completed:
                    unmet_deps.add(dep_id)
                    self._blocking[dep_id].add(task.task_id)
            
            # If has unmet dependencies, add to waiting list
            if unmet_deps:
                self._waiting_on_deps[task.task_id] = unmet_deps
                return
                
        # Otherwise, add to priority queue
        self._counter += 1
        heapq.heappush(self._queue, (task.priority.value, self._counter, task.task_id))
    
    def mark_task_completed(self, task_id: str, result: Any = None):
        """Mark a task as completed and release its dependents"""
        if task_id not in self._tasks:
            logger.warning(f"Task {task_id} not found in queue")
            return
            
        task = self._tasks[task_id]
        task.complete(result)
        
        # Check for tasks waiting on this one
        for dependent_id in list(self._blocking.get(task_id, set())):
            if dependent_id in self._waiting_on_deps:
                deps = self._waiting_on_deps[dependent_id]
                deps.remove(task_id)
                
                # If no more dependencies, move to main queue
                if not deps:
                    dependent_task = self._tasks[dependent_id]
                    self._counter += 1
                    heapq.heappush(self._queue, 
                                  (dependent_task.priority.value, self._counter, dependent_id))
                    del self._waiting_on_deps[dependent_id]
                    
        # Clean up the blocking set
        if task_id in self._blocking:
            del self._blocking[task_id]
                    
    def mark_task_failed(self, task_id: str, error: Exception):
        """Mark a task as failed and cascade failure to dependents if configured"""
        if task_id not in self._tasks:
            logger.warning(f"Task {task_id} not found in queue")
            return
            
        task = self._tasks[task_id]
        task.fail(error)
        
        # For now, leave dependents in waiting state
        # They'll never be scheduled unless configured to ignore failed dependencies
                    
    def get_next_task(self) -> Optional[Task]:
        """Get the next task respecting priority and dependencies"""
        while self._queue:
            _, _, task_id = heapq.heappop(self._queue)
            task = self._tasks[task_id]
            
            # Check if task can be processed (might have failed deps)
            if task_id in self._waiting_on_deps and self._waiting_on_deps[task_id]:
                # Still has dependencies, put back in waiting
                continue
                
            # If task isn't completed or failed, return it
            if not task.is_completed and not task.is_failed:
                return task
                
        return None
        
    @property
    def size(self) -> int:
        """Return the total number of incomplete tasks"""
        return len([t for t in self._tasks.values() 
                   if not t.is_completed and not t.is_failed]) 
    
    @property
    def waiting_tasks(self) -> int:
        """Return the number of tasks waiting for dependencies"""
        return len(self._waiting_on_deps)
    
    @property
    def ready_tasks(self) -> int:
        """Return the number of tasks ready to be processed"""
        return len(self._queue)


class TaskManager:
    """Manages and orchestrates tasks across multiple micro-agents"""
    
    def __init__(self, max_concurrent_tasks: int = 10):
        self.agents: Dict[str, List[BaseAgent]] = defaultdict(list)
        self.agent_by_id: Dict[str, BaseAgent] = {}
        self.task_queue = TaskQueue()
        self.max_concurrent_tasks = max_concurrent_tasks
        self.running_tasks: Dict[str, asyncio.Task] = {}
        self.completed_tasks: Dict[str, Task] = {}
        self.task_results: Dict[str, Any] = {}
        self.running = False
        self.event_loop = None
        self._workers: List[asyncio.Task] = []
        self._ticks = 0
        
    async def initialize(self):
        """Initialize all registered agents"""
        logger.info("Initializing TaskManager and all agents")
        
        # Initialize all agents concurrently
        init_tasks = []
        for agent_id, agent in self.agent_by_id.items():
            init_tasks.append(agent.initialize())
            
        # Wait for all to initialize
        await asyncio.gather(*init_tasks)
        self.event_loop = asyncio.get_running_loop()
        logger.info("TaskManager initialization complete")
        
    async def shutdown(self):
        """Shut down the task manager and all agents"""
        logger.info("Shutting down TaskManager")
        self.running = False
        
        # Wait for workers to finish
        if self._workers:
            for worker in self._workers:
                if not worker.done():
                    worker.cancel()
            
            await asyncio.gather(*self._workers, return_exceptions=True)
            self._workers = []
            
        # Shutdown all agents
        shutdown_tasks = []
        for agent_id, agent in self.agent_by_id.items():
            shutdown_tasks.append(agent.shutdown())
            
        await asyncio.gather(*shutdown_tasks, return_exceptions=True)
        logger.info("TaskManager shutdown complete")
        
    def register_agent(self, agent: BaseAgent):
        """Register an agent with the task manager"""
        self.agents[agent.agent_type].append(agent)
        self.agent_by_id[agent.agent_id] = agent
        logger.info(f"Registered agent: {agent.agent_id} of type {agent.agent_type}")
        
    def register_agent_for_task_types(self, agent: BaseAgent, task_types: List[str]):
        """Register an agent to handle specific task types
        
        Args:
            agent: The agent to register
            task_types: List of task types this agent can handle
        """
        # First register the agent normally
        self.agent_by_id[agent.agent_id] = agent
        
        # Then register for each task type
        for task_type in task_types:
            self.agents[task_type].append(agent)
            
        logger.info(f"Registered agent: {agent.agent_id} for task types: {', '.join(task_types)}")
        
    async def create_task(self, 
                        task_type: str, 
                        data: Dict[str, Any] = None,
                        priority: TaskPriority = TaskPriority.MEDIUM,
                        dependencies: List[str] = None) -> str:
        """Create a new task and add it to the queue"""
        task_id = f"{task_type}-{uuid.uuid4().hex[:8]}"
        task = Task(task_id, task_type, priority, data)
        
        # Add dependencies if specified
        if dependencies:
            task.dependencies = set(dependencies)
            
        self.task_queue.add_task(task)
        
        # If there are available workers, schedule immediately
        await self._schedule_tasks()
        
        return task_id
        
    async def create_dependent_task_chain(self, 
                                        task_specs: List[Dict[str, Any]]) -> List[str]:
        """Create a chain of tasks where each depends on the previous"""
        task_ids = []
        prev_id = None
        
        for spec in task_specs:
            # Get the task specification
            task_type = spec["task_type"]
            data = spec.get("data", {})
            priority = spec.get("priority", TaskPriority.MEDIUM)
            
            # Add the previous task as a dependency
            deps = spec.get("dependencies", [])
            if prev_id:
                deps.append(prev_id)
                
            # Create the task    
            task_id = await self.create_task(
                task_type,
                data,
                priority,
                deps
            )
            
            task_ids.append(task_id)
            prev_id = task_id
            
        return task_ids
        
    async def get_task_result(self, task_id: str, 
                            timeout: float = None) -> Any:
        """Get the result of a task, waiting if necessary"""
        # If already completed, return result
        if task_id in self.task_results:
            logger.info(f"Task {task_id} result found immediately in cache")
            return self.task_results[task_id]
            
        # Otherwise wait for task to complete
        logger.info(f"Waiting for task {task_id} to complete (timeout={timeout}s)")
        start_time = time.monotonic()
        wait_count = 0
        
        while timeout is None or (time.monotonic() - start_time < timeout):
            if task_id in self.task_results:
                logger.info(f"Task {task_id} result found after {time.monotonic() - start_time:.2f}s")
                return self.task_results[task_id]
                
            if task_id in self.completed_tasks:
                task = self.completed_tasks[task_id]
                if task.is_failed:
                    logger.warning(f"Task {task_id} failed: {task.error}")
                    raise task.error or RuntimeError(f"Task {task_id} failed without specific error")
                    
                logger.info(f"Task {task_id} found in completed_tasks after {time.monotonic() - start_time:.2f}s")
                self.task_results[task_id] = task.result
                return task.result
            
            wait_count += 1
            if wait_count % 10 == 0:  # Log every 10 waits (about 1 second)
                elapsed = time.monotonic() - start_time
                logger.info(f"Still waiting for task {task_id} after {elapsed:.2f}s")
                
            await asyncio.sleep(0.1)
            
        logger.error(f"Timeout waiting for task {task_id} after {timeout}s")
        raise asyncio.TimeoutError(f"Timeout waiting for task {task_id}")
        
    async def start(self):
        """Start processing tasks"""
        if self.running:
            logger.info("TaskManager is already running - skipping start")
            return
            
        self.running = True
        
        # Create worker tasks
        logger.info(f"Creating {self.max_concurrent_tasks} worker tasks")
        for i in range(self.max_concurrent_tasks):
            worker = asyncio.create_task(self._worker_loop(f"worker-{i}"))
            self._workers.append(worker)
            
        # Start the scheduler loop
        logger.info("Creating scheduler task")
        scheduler = asyncio.create_task(self._scheduler_loop())
        self._workers.append(scheduler)
        
        logger.info(f"TaskManager started with {self.max_concurrent_tasks} workers")

        # Log the worker status after a brief delay to ensure they're running
        await asyncio.sleep(0.5)
        self._log_worker_status()
        
    async def _worker_loop(self, worker_id: str):
        """Worker loop that processes tasks"""
        logger.debug(f"Worker {worker_id} started")
        
        while self.running:
            try:
                task = self.task_queue.get_next_task()
                
                if not task:
                    # No task available, sleep and check again
                    await asyncio.sleep(0.1)
                    continue
                    
                # Find an available agent of the right type
                agent = await self._get_available_agent(task.task_type)
                
                if not agent:
                    logger.warning(f"No available agent found for task type: {task.task_type}, retrying later")
                    # No agent available, put task back and sleep
                    self.task_queue.add_task(task)
                    await asyncio.sleep(0.5)
                    continue
                    
                # Process the task
                logger.info(f"Worker {worker_id} processing task {task.task_id} with agent {agent.agent_id}")
                
                try:
                    # Add a stricter timeout to task processing
                    task_timeout = 60  # 1 minute max for any task
                    
                    try:
                        result = await asyncio.wait_for(
                            agent.process_task(task),
                            timeout=task_timeout
                        )
                        logger.info(f"Task {task.task_id} completed successfully")
                        self.task_queue.mark_task_completed(task.task_id, result)
                        self.completed_tasks[task.task_id] = task
                        self.task_results[task.task_id] = result
                    except asyncio.TimeoutError:
                        logger.error(f"Task {task.task_id} timed out after {task_timeout} seconds")
                        error = TimeoutError(f"Task processing timed out after {task_timeout} seconds")
                        self.task_queue.mark_task_failed(task.task_id, error)
                        self.completed_tasks[task.task_id] = task
                        self.task_results[task.task_id] = {"error": f"Timeout after {task_timeout}s", "fallback": True}
                        
                        # Try to recover the agent
                        await agent._try_recovery()
                        
                except Exception as e:
                    logger.error(f"Error processing task {task.task_id}: {str(e)}", exc_info=True)
                    self.task_queue.mark_task_failed(task.task_id, e)
                    self.completed_tasks[task.task_id] = task
                    self.task_results[task.task_id] = {"error": str(e), "fallback": True}
                    
            except Exception as e:
                logger.error(f"Error in worker {worker_id}: {str(e)}", exc_info=True)
                await asyncio.sleep(1)  # Sleep to avoid tight loop on persistent errors
                
        logger.debug(f"Worker {worker_id} stopped")
    
    async def _scheduler_loop(self):
        """Schedule tasks periodically"""
        logger.debug("Scheduler started")
        
        while self.running:
            try:
                self._ticks += 1
                await self._schedule_tasks()
                
                # Every 10 ticks, log status
                if self._ticks % 10 == 0:
                    await self._log_status()
                    
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"Error in scheduler: {str(e)}", exc_info=True)
                await asyncio.sleep(1)
                
        logger.debug("Scheduler stopped")
    
    async def _schedule_tasks(self):
        """Schedule tasks based on priority and dependencies"""
        # No-op for now, the worker loops do the scheduling
        pass
    
    async def _log_status(self):
        """Log status of tasks and agents"""
        agent_stats = {
            "idle": sum(1 for a in self.agent_by_id.values() if a.state == AgentState.IDLE),
            "working": sum(1 for a in self.agent_by_id.values() if a.state == AgentState.WORKING),
            "error": sum(1 for a in self.agent_by_id.values() if a.state == AgentState.ERROR),
        }
        
        queue_stats = {
            "waiting": self.task_queue.waiting_tasks,
            "ready": self.task_queue.ready_tasks,
            "running": len(self.running_tasks),
            "completed": len(self.completed_tasks),
        }
        
        logger.debug(f"TaskManager status: Agents={agent_stats}, Tasks={queue_stats}")
        
        # Update metrics
        await metrics_collector.update_system_status(
            "processing",
            {"tasks": queue_stats, "agents": agent_stats}
        )
    
    async def _get_available_agent(self, task_type: str) -> Optional[BaseAgent]:
        """Find an available agent for a task type"""
        if task_type not in self.agents:
            logger.warning(f"No agents registered for task type: {task_type}")
            return None
            
        # Find an idle agent
        for agent in self.agents[task_type]:
            if agent.state == AgentState.IDLE:
                return agent
                
        return None
        
    def get_all_agent_stats(self) -> Dict[str, Dict]:
        """Get stats from all agents"""
        return {agent_id: agent.get_stats() for agent_id, agent in self.agent_by_id.items()}
    
    def _log_worker_status(self):
        """Log detailed status of worker tasks to help with debugging"""
        running_workers = 0
        done_workers = 0
        cancelled_workers = 0
        exception_workers = 0
        
        for i, worker in enumerate(self._workers):
            if worker.done():
                if worker.cancelled():
                    cancelled_workers += 1
                elif worker.exception() is not None:
                    exception_workers += 1
                else:
                    done_workers += 1
            else:
                running_workers += 1
        
        logger.info(f"Worker status: Running={running_workers}, Done={done_workers}, "
                    f"Cancelled={cancelled_workers}, Exception={exception_workers}")
        
        # Log agent statuses
        agent_counts = defaultdict(int)
        for agent_type, agents in self.agents.items():
            for agent in agents:
                agent_counts[agent.state.value] += 1
            
            logger.info(f"Agent type '{agent_type}' count: {len(agents)}")
        
        logger.info(f"Agent states: {dict(agent_counts)}")
        
        # Check specifically for source_discovery agents
        if 'source_discovery' in self.agents:
            logger.info(f"Source discovery agents: {len(self.agents['source_discovery'])}")
            for i, agent in enumerate(self.agents['source_discovery']):
                logger.info(f"  Agent {i+1}: {agent.agent_id}, State: {agent.state.value}, "
                           f"Current task: {agent.current_task.task_id if agent.current_task else 'None'}")