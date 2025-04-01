import logging
import asyncio
import time
import uuid
from typing import Dict, List, Any, Optional, Set, Callable, Awaitable, Union, TypeVar, DefaultDict
from collections import defaultdict
import heapq
import traceback
import random
import concurrent.futures
from functools import partial

from .base_agent import BaseAgent, Task, TaskPriority, AgentState
from monitor.metrics import metrics_collector

logger = logging.getLogger(__name__)

T = TypeVar('T')

class TaskQueue:
    """Enhanced priority queue for tasks with dependency resolution and advanced scheduling"""
    
    def __init__(self):
        self._queue = []  # heap for priority queue
        self._tasks: Dict[str, Task] = {}  # task_id -> Task
        self._waiting_on_deps: Dict[str, Set[str]] = defaultdict(set)  # task_id -> set of dependency task_ids
        self._blocking: Dict[str, Set[str]] = defaultdict(set)  # task_id -> set of tasks waiting on this
        self._counter = 0  # tie breaker for same priority items
        self._task_types: Dict[str, List[str]] = defaultdict(list)  # task_type -> list of task_ids
        self._agent_task_counts: Dict[str, int] = defaultdict(int)  # agent_id -> count of assigned tasks
        
    def add_task(self, task: Task):
        """Add a task to the queue, taking dependencies into account"""
        if task.task_id in self._tasks:
            logger.warning(f"Task {task.task_id} already in queue, skipping")
            return
            
        self._tasks[task.task_id] = task
        
        # Track by task type for stats and agent assignment
        self._task_types[task.task_type].append(task.task_id)
        
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
    
    def mark_task_completed(self, task_id: str, result: Any = None, agent_id: Optional[str] = None):
        """Mark a task as completed and release its dependents"""
        if task_id not in self._tasks:
            logger.warning(f"Task {task_id} not found in queue")
            return
            
        task = self._tasks[task_id]
        task.complete(result)
        
        # Track agent completion stats if provided
        if agent_id and agent_id in self._agent_task_counts:
            self._agent_task_counts[agent_id] -= 1
        
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
        
        # Clean up task type tracking
        task_type = task.task_type
        if task_id in self._task_types.get(task_type, []):
            self._task_types[task_type].remove(task_id)
                    
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
            
        # Handle dependent tasks - mark them as failed too if the dependency is required
        dependent_ids = list(self._blocking.get(task_id, set()))
        for dependent_id in dependent_ids:
            dependent_task = self._tasks.get(dependent_id)
            if dependent_task and not dependent_task.is_failed:
                # Check if the dependency was required
                # For now, all dependencies are considered required
                # This could be extended with an optional flag in the future
                dependent_task.fail(Exception(f"Required dependency {task_id} failed"))
                
                # Recursively handle dependents of this task
                self.mark_task_failed(dependent_id, Exception(f"Cascading failure from {task_id}"))

        # Clean up the task's dependencies
        if task_id in self._blocking:
            del self._blocking[task_id]
                
        # Clean up task type tracking
        task_type = task.task_type
        if task_id in self._task_types.get(task_type, []):
            self._task_types[task_type].remove(task_id)
                    
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

    def get_task_for_agent(self, agent_id: str, agent_task_types: List[str]) -> Optional[Task]:
        """Get a suitable task for a specific agent based on its supported task types"""
        # Create a list of tasks that can be processed by this agent
        eligible_tasks = []
        
        # First check for high priority tasks of the agent's types
        for task_type in agent_task_types:
            for task_id in self._task_types.get(task_type, []):
                task = self._tasks.get(task_id)
                if (not task or task.is_completed or task.is_failed or 
                    task_id in self._waiting_on_deps):
                    continue
                    
                # Add to eligible tasks with priority as key
                eligible_tasks.append((task.priority.value, self._counter, task_id))
                
        # If we have eligible tasks, return the highest priority one
        if eligible_tasks:
            eligible_tasks.sort()  # Sort by priority
            _, _, task_id = eligible_tasks[0]
            
            # Remove from queue to avoid double-assignment
            for i, (_, _, queue_task_id) in enumerate(self._queue):
                if queue_task_id == task_id:
                    self._queue.pop(i)
                    heapq.heapify(self._queue)  # Re-heapify after removal
                    break
                    
            # Track assignment
            self._agent_task_counts[agent_id] += 1
            
            return self._tasks[task_id]
            
        # No specific tasks for this agent's types, just return the next task from the queue
        # This is needed for the regular get_next_task calls
        return self.get_next_task()
        
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
        
    def get_task_counts_by_type(self) -> Dict[str, int]:
        """Return counts of incomplete tasks by type"""
        return {task_type: len(task_ids) for task_type, task_ids in self._task_types.items()}
        
    def get_agent_task_counts(self) -> Dict[str, int]:
        """Return the current task load for each agent"""
        return dict(self._agent_task_counts)
        
    def get_task_by_id(self, task_id: str) -> Optional[Task]:
        """Get a task by its ID"""
        return self._tasks.get(task_id)
        
    def get_dependent_tasks(self, task_id: str) -> List[Task]:
        """Get all tasks that depend on the given task"""
        if task_id not in self._blocking:
            return []
            
        dependent_ids = self._blocking[task_id]
        return [self._tasks[dep_id] for dep_id in dependent_ids if dep_id in self._tasks]


class TaskManager:
    """
    Manages and orchestrates tasks across multiple micro-agents 
    with improved load balancing and failure handling
    """
    
    def __init__(self, max_concurrent_tasks: int = 10):
        # Core task management
        self.max_concurrent_tasks = max_concurrent_tasks
        self.task_queue = TaskQueue()
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
        self._max_retries = 2
        
        # Advanced scheduling
        self._agent_health_scores: Dict[str, float] = {}  # 0.0 to 1.0, higher is healthier
        self._agent_performance_metrics: Dict[str, Dict[str, Any]] = defaultdict(dict)
        
        # Load balancing
        self._round_robin_indexes: Dict[str, int] = defaultdict(int)  # task_type -> current index
        
        # For monitoring
        self._task_latencies: List[float] = []  # track task execution times
        
        logger.info(f"Task Manager initialized with {max_concurrent_tasks} max concurrent tasks")
        
    async def initialize(self):
        """Initialize the task manager and all registered agents"""
        logger.info("Initializing Task Manager")
        
        # Initialize all agents
        for agent in self._all_agents.values():
            logger.info(f"Initializing agent: {agent.agent_id}")
            await agent.initialize()
            self._agent_health_scores[agent.agent_id] = 1.0  # Start with perfect health
            
        logger.info("Task Manager initialization complete")
        
    async def start(self):
        """Start the task processing workers"""
        if self.running:
            logger.warning("Task Manager is already running")
            return
            
        self.running = True
        for _ in range(self.max_concurrent_tasks):
            worker = asyncio.create_task(self._worker_loop())
            self._workers.append(worker)
            
        logger.info(f"Task Manager started with {len(self._workers)} workers")
        
    async def shutdown(self):
        """Shutdown the task manager and all agents"""
        logger.info("Shutting down Task Manager")
        
        # Stop accepting new tasks
        self.running = False
        
        # Wait for all workers to complete (with timeout)
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
            
        # Shutdown all agents
        for agent in self._all_agents.values():
            logger.info(f"Shutting down agent: {agent.agent_id}")
            await agent.shutdown()
            
        logger.info("Task Manager shutdown complete")
        self._log_performance_summary()
        
    def _log_performance_summary(self):
        """Log performance metrics at shutdown"""
        if not self._task_latencies:
            return
            
        avg_latency = sum(self._task_latencies) / len(self._task_latencies)
        logger.info(f"Task Execution Summary: {len(self._task_latencies)} tasks processed")
        logger.info(f"Average task latency: {avg_latency:.2f}s")
        
        # Agent performance
        for agent_id, metrics in self._agent_performance_metrics.items():
            success_count = metrics.get('success_count', 0)
            failure_count = metrics.get('failure_count', 0)
            total = success_count + failure_count
            
            if total > 0:
                success_rate = (success_count / total) * 100
                avg_time = metrics.get('total_time', 0) / total if total > 0 else 0
                logger.info(
                    f"Agent {agent_id}: {success_count}/{total} tasks completed successfully "
                    f"({success_rate:.1f}%), avg time: {avg_time:.2f}s"
                )
        
    def register_agent_for_task_types(self, agent: BaseAgent, task_types: List[str]):
        """Register an agent for specific task types"""
        agent_id = agent.agent_id
        
        # Add to agent registry
        self._all_agents[agent_id] = agent
        
        # Map task types to agent
        for task_type in task_types:
            self._task_type_to_agents[task_type].append(agent)
            if task_type not in self._agent_to_task_types[agent_id]:
                self._agent_to_task_types[agent_id].append(task_type)
                
        logger.info(f"Registered agent {agent_id} for task types: {', '.join(task_types)}")
        
    async def create_task(
        self, 
        task_type: str, 
        data: Dict[str, Any] = None, 
        priority: TaskPriority = TaskPriority.MEDIUM,
        dependencies: List[str] = None
    ) -> str:
        """Create and queue a new task, returning its ID"""
        task_id = f"{task_type}_{uuid.uuid4().hex[:8]}"
        
        task = Task(task_id, task_type, priority, data or {})
        
        # Add dependencies if provided
        if dependencies:
            for dep_id in dependencies:
                task.dependencies.add(dep_id)
                
        # Register completion event
        self._task_completion_events[task_id] = asyncio.Event()
        
        # Add to task queue
        self.task_queue.add_task(task)
        
        # Log task creation with more details
        dep_str = f" with {len(task.dependencies)} dependencies" if task.dependencies else ""
        logger.debug(f"Created task {task_id} ({task_type}){dep_str} at priority {priority.name}")
        
        # Task queue stats after adding
        await metrics_collector.update_system_status(
            "Task queued", 
            {"task_type": task_type, "priority": priority.name, "queue_size": self.task_queue.size}
        )
        
        return task_id
        
    async def get_task_result(self, task_id: str, timeout: Optional[float] = None) -> Any:
        """
        Wait for a task to complete and return its result.
        Raises the task's exception if it failed.
        """
        if task_id not in self._task_completion_events:
            raise ValueError(f"No such task: {task_id}")
            
        # If already completed or failed, return immediately
        task = self.task_queue.get_task_by_id(task_id)
        if task and task.is_completed:
            return task.result
        if task and task.is_failed:
            raise task.error or Exception(f"Task {task_id} failed with no error details")
            
        # Wait for completion event with optional timeout
        event = self._task_completion_events[task_id]
        try:
            await asyncio.wait_for(event.wait(), timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(f"Timeout waiting for task {task_id} to complete")
            
        # Check for task error
        if task_id in self._task_errors:
            raise self._task_errors[task_id]
            
        # Return result
        if task_id in self._task_results:
            return self._task_results[task_id]
        else:
            # Should never happen, but just in case
            raise Exception(f"Task {task_id} completed but has no result")
            
    async def _worker_loop(self):
        """Worker loop that processes tasks"""
        try:
            while self.running:
                # Get a task with semaphore
                async with self._worker_semaphore:
                    # No specific agent assignment here - just get the next task
                    task = self.task_queue.get_next_task()
                    
                    if not task:
                        # No tasks available, wait a bit and try again
                        await asyncio.sleep(0.1)
                        continue
                        
                    # Process the task
                    task_id = task.task_id
                    task_type = task.task_type
                    
                    await self._process_task(task)
                    
        except asyncio.CancelledError:
            # Worker is being cancelled during shutdown
            logger.debug("Task worker cancelled during shutdown")
        except Exception as e:
            logger.error(f"Error in task worker: {e}", exc_info=True)
            
    async def _process_task(self, task: Task) -> None:
        """Process a task by finding an appropriate agent and executing it"""
        task_id = task.task_id
        task_type = task.task_type
        
        # Select an agent for this task type
        agent = await self._select_agent_for_task(task)
        
        if not agent:
            error = ValueError(f"No agent available for task type: {task_type}")
            self._handle_task_failure(task_id, error)
            return
            
        # Log task processing
        agent_id = agent.agent_id
        logger.debug(f"Processing task {task_id} ({task_type}) with agent {agent_id}")
        
        # Track start time for performance metrics
        start_time = time.monotonic()
        
        try:
            # Process the task with the agent
            result = await agent.process_task(task)
            
            # Record successful completion
            self._task_results[task_id] = result
            self.task_queue.mark_task_completed(task_id, result, agent_id)
            
            # Update performance metrics
            execution_time = time.monotonic() - start_time
            self._task_latencies.append(execution_time)
            self._update_agent_performance(
                agent_id, execution_time, is_success=True, task_type=task_type
            )
            
            # Set completion event
            if task_id in self._task_completion_events:
                self._task_completion_events[task_id].set()
                
            logger.debug(f"Task {task_id} completed in {execution_time:.2f}s")
            
        except Exception as e:
            # Get stack trace for error logging
            error_info = traceback.format_exc()
            logger.error(f"Error processing task {task_id}: {str(e)}\n{error_info}")
            
            # Check if we should retry the task
            if not await self._handle_task_retry(task, e, agent_id):
                # If not retrying or retry failed, mark as failed
                self._handle_task_failure(task_id, e)
                
            # Update agent health and performance metrics
            execution_time = time.monotonic() - start_time
            self._update_agent_performance(
                agent_id, execution_time, is_success=False, task_type=task_type, error=str(e)
            )
            
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
        if random.random() < 0.7:  # 70% of the time use weighted health selection
            weighted_agents = []
            for agent in available_agents:
                # Get health score (default to 0.5 if not available)
                health_score = self._agent_health_scores.get(agent.agent_id, 0.5)
                
                # Add the agent to the weighted list based on its health
                # Higher health score = more entries = higher chance of selection
                weight = int(health_score * 10) + 1  # Ensure at least weight 1
                weighted_agents.extend([agent] * weight)
                
            if weighted_agents:
                return random.choice(weighted_agents)
        
        # 2. Round robin selection (for the rest of the time)
        if available_agents:
            rr_index = self._round_robin_indexes[task_type]
            agent = available_agents[rr_index % len(available_agents)]
            self._round_robin_indexes[task_type] = (rr_index + 1) % len(available_agents)
            return agent
            
        # Fallback to first available agent
        return available_agents[0]
    
    def _update_agent_performance(
        self, 
        agent_id: str, 
        execution_time: float, 
        is_success: bool, 
        task_type: str,
        error: Optional[str] = None
    ):
        """Update agent performance metrics and health score"""
        metrics = self._agent_performance_metrics[agent_id]
        
        # Update success/failure counts
        if is_success:
            metrics['success_count'] = metrics.get('success_count', 0) + 1
        else:
            metrics['failure_count'] = metrics.get('failure_count', 0) + 1
            
        # Update time metrics
        metrics['total_time'] = metrics.get('total_time', 0) + execution_time
        
        # Track by task type
        type_metrics = metrics.setdefault('by_type', {}).setdefault(task_type, {})
        if is_success:
            type_metrics['success_count'] = type_metrics.get('success_count', 0) + 1
        else:
            type_metrics['failure_count'] = type_metrics.get('failure_count', 0) + 1
            
        # Update health score - calculate success rate with a bias toward recent performance
        total = metrics.get('success_count', 0) + metrics.get('failure_count', 0)
        if total > 0:
            success_rate = metrics.get('success_count', 0) / total
            
            # Get previous health score or default to success rate
            prev_health = self._agent_health_scores.get(agent_id, success_rate)
            
            # Health score is 70% previous score, 30% current success (more weight to history)
            # This allows health to recover after temporary failures
            new_health = (prev_health * 0.7) + (success_rate * 0.3)
            
            # But if this task failed, apply an immediate penalty
            if not is_success:
                new_health *= 0.8  # 20% penalty for any failure
                
            self._agent_health_scores[agent_id] = new_health
            
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
        new_priority = TaskPriority(min(task.priority.value, TaskPriority.HIGH.value))  # Boost priority
        new_task = Task(task_id, task_type, new_priority, task.data)
        
        # Add to queue with dependencies cleared (to avoid deadlock)
        self.task_queue.add_task(new_task)
        
        # Clear cached agent from this task to try a different agent
        try:
            agents = self._task_type_to_agents.get(task_type, [])
            if len(agents) > 1:
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
        total_tasks = self.task_queue.size
        waiting_tasks = self.task_queue.waiting_tasks
        ready_tasks = self.task_queue.ready_tasks
        
        logger.info(
            f"Task Manager status: {len(self._workers)} workers, {total_tasks} total tasks "
            f"({ready_tasks} ready, {waiting_tasks} waiting)"
        )
        
        # Log task distribution by type
        task_counts = self.task_queue.get_task_counts_by_type()
        if task_counts:
            type_counts_str = ", ".join(f"{t}: {c}" for t, c in task_counts.items())
            logger.info(f"Tasks by type: {type_counts_str}")
            
        # Log agent health
        if self._agent_health_scores:
            health_str = ", ".join(
                f"{agent_id}: {score:.2f}" 
                for agent_id, score in self._agent_health_scores.items()
            )
            logger.info(f"Agent health scores: {health_str}")
            
    async def get_status(self) -> Dict[str, Any]:
        """Get detailed status of the task manager"""
        status = {
            "running": self.running,
            "workers": len(self._workers),
            "tasks": {
                "total": self.task_queue.size,
                "waiting": self.task_queue.waiting_tasks,
                "ready": self.task_queue.ready_tasks,
                "by_type": self.task_queue.get_task_counts_by_type()
            },
            "agents": {
                "count": len(self._all_agents),
                "health": dict(self._agent_health_scores),
                "task_types": dict(self._agent_to_task_types)
            }
        }
        
        # Add agent details
        agent_details = {}
        for agent_id, agent in self._all_agents.items():
            agent_details[agent_id] = {
                "type": agent.agent_type,
                "state": agent.state.value,
                "health": self._agent_health_scores.get(agent_id, 0.5),
                "success_rate": self._get_agent_success_rate(agent_id)
            }
            
        status["agent_details"] = agent_details
        
        return status
    
    def _get_agent_success_rate(self, agent_id: str) -> float:
        """Calculate the success rate for an agent"""
        metrics = self._agent_performance_metrics.get(agent_id, {})
        success = metrics.get('success_count', 0)
        failure = metrics.get('failure_count', 0)
        total = success + failure
        
        if total == 0:
            return 1.0  # Default to perfect if no data
            
        return success / total