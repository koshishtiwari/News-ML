import logging
import asyncio
import time
import json
from typing import Dict, Any, Optional, List, Set, Tuple, Callable, Awaitable
from dataclasses import dataclass, field, asdict
import uuid

from .base_agent import BaseAgent, Task, TaskPriority, AgentState
from monitor.metrics import metrics_collector

logger = logging.getLogger(__name__)

@dataclass
class Memory:
    """Represents a memory item for cognitive agents"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    content: Any = None
    context: Dict[str, Any] = field(default_factory=dict)
    importance: float = 0.5  # 0.0 to 1.0
    created_at: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)
    access_count: int = 0
    
    def access(self):
        """Mark this memory as accessed"""
        self.last_accessed = time.time()
        self.access_count += 1
        return self
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Memory':
        """Create from dictionary"""
        return cls(**data)

@dataclass
class Observation:
    """An observation made by an agent about the environment or task"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    content: str = ""
    source: str = ""
    confidence: float = 1.0
    timestamp: float = field(default_factory=time.time)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)

@dataclass
class Insight:
    """A derived insight from observations and reasoning"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    content: str = ""
    supporting_observations: List[str] = field(default_factory=list)
    confidence: float = 0.7
    timestamp: float = field(default_factory=time.time)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)

@dataclass
class Goal:
    """A goal for a cognitive agent"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    description: str = ""
    priority: float = 0.5  # 0.0 to 1.0
    status: str = "active"  # active, achieved, abandoned
    created_at: float = field(default_factory=time.time)
    achieved_at: Optional[float] = None
    parent_goal_id: Optional[str] = None
    criteria: Dict[str, Any] = field(default_factory=dict)
    
    def achieve(self):
        """Mark this goal as achieved"""
        self.status = "achieved"
        self.achieved_at = time.time()
        return self
    
    def abandon(self):
        """Mark this goal as abandoned"""
        self.status = "abandoned"
        return self
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)

class CognitiveAgent(BaseAgent):
    """
    Advanced agent with cognitive capabilities including:
    - Self-directed reasoning
    - Memory and learning
    - Goal-setting and planning
    - Collaborative decision-making
    """
    
    def __init__(self, 
                 agent_type: str,
                 llm_provider=None,
                 reasoning_interval: float = 30.0,
                 memory_capacity: int = 1000):
        super().__init__(agent_type)
        self.llm_provider = llm_provider
        self.reasoning_interval = reasoning_interval
        self.memory_capacity = memory_capacity
        
        # Cognitive state
        self.memories: Dict[str, Memory] = {}
        self.observations: List[Observation] = []
        self.insights: List[Insight] = []
        self.goals: Dict[str, Goal] = {}
        
        # Collaborative state
        self.agent_network: Dict[str, Dict[str, Any]] = {}
        self.shared_knowledge: Dict[str, Any] = {}
        
        # Reasoning task
        self._reasoning_task: Optional[asyncio.Task] = None
        self._reasoning_active = False
        
        logger.info(f"Cognitive agent {self.agent_id} initialized")
    
    async def _initialize_resources(self):
        """Initialize agent resources and start the reasoning loop"""
        await super()._initialize_resources()
        
        # Initialize cognitive systems
        await self._initialize_cognitive_systems()
        
        # Start background reasoning loop
        self._reasoning_active = True
        self._reasoning_task = asyncio.create_task(self._reasoning_loop())
        
        logger.info(f"Cognitive agent {self.agent_id} reasoning loop started")
    
    async def _initialize_cognitive_systems(self):
        """Initialize cognitive subsystems"""
        # This would include loading saved memory, connecting to shared knowledge 
        # repositories, etc.
        pass
    
    async def _cleanup_resources(self):
        """Clean up resources and stop the reasoning loop"""
        # Stop the reasoning loop
        self._reasoning_active = False
        if self._reasoning_task:
            try:
                self._reasoning_task.cancel()
                await asyncio.wait_for(self._reasoning_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
            
        # Persist memory and insights
        await self._persist_cognitive_state()
        
        await super()._cleanup_resources()
    
    async def _persist_cognitive_state(self):
        """Save the cognitive state for future restoration"""
        # This would persist memories, insights, etc. to storage
        pass
    
    async def _reasoning_loop(self):
        """Background loop for autonomous reasoning"""
        try:
            while self._reasoning_active:
                if self.state == AgentState.IDLE:
                    # Only reason when not busy with a task
                    await self._perform_reasoning_cycle()
                
                # Wait for the next reasoning cycle
                await asyncio.sleep(self.reasoning_interval)
        except asyncio.CancelledError:
            logger.info(f"Reasoning loop for {self.agent_id} was cancelled")
        except Exception as e:
            logger.error(f"Error in reasoning loop for {self.agent_id}: {str(e)}", exc_info=True)
    
    async def _perform_reasoning_cycle(self):
        """Perform a single reasoning cycle"""
        logger.debug(f"Agent {self.agent_id} starting reasoning cycle")
        
        try:
            # 1. Review recent observations and memories
            relevant_memories = self._retrieve_relevant_memories()
            
            # 2. Generate new insights
            if self.llm_provider and relevant_memories:
                new_insights = await self._generate_insights(relevant_memories)
                if new_insights:
                    self.insights.extend(new_insights)
                    
            # 3. Evaluate current goals
            await self._evaluate_goals()
            
            # 4. Consider creating new goals
            await self._consider_new_goals()
            
            # 5. Plan actions for goals
            await self._plan_actions_for_goals()
        
        except Exception as e:
            logger.error(f"Error during reasoning cycle for {self.agent_id}: {str(e)}", exc_info=True)
    
    def _retrieve_relevant_memories(self, context: Dict[str, Any] = None, limit: int = 10) -> List[Memory]:
        """Retrieve memories relevant to the current context"""
        # For now, just return recent memories
        # In a production system, this would use vector similarity, relevance scoring, etc.
        sorted_memories = sorted(
            self.memories.values(), 
            key=lambda m: m.importance * (0.9 ** ((time.time() - m.last_accessed) / 86400)),
            reverse=True
        )
        return sorted_memories[:limit]
    
    async def _generate_insights(self, memories: List[Memory]) -> List[Insight]:
        """Generate new insights based on memories and observations"""
        if not self.llm_provider:
            return []
            
        try:
            # Format memories for LLM consumption
            memory_texts = [f"Memory {i+1}: {m.content}" for i, m in enumerate(memories)]
            memory_context = "\n".join(memory_texts)
            
            # Generate insights using LLM
            prompt = f"""Based on the following information, generate 1-3 meaningful insights:

{memory_context}

For each insight:
1. Provide a clear, specific statement
2. Rate your confidence (0.0-1.0)
3. Reference which memories support this insight (by number)

Format: [
  {{
    "content": "insight statement",
    "confidence": 0.X,
    "supporting_observations": [1, 2, ...]
  }},
  ...
]"""

            insight_text = await self.llm_provider.generate(prompt)
            
            # Parse insights
            try:
                insight_data = json.loads(insight_text)
                insights = []
                
                for item in insight_data:
                    supporting_ids = []
                    for idx in item.get("supporting_observations", []):
                        if 0 <= idx-1 < len(memories):
                            supporting_ids.append(memories[idx-1].id)
                    
                    insights.append(Insight(
                        content=item.get("content", ""),
                        supporting_observations=supporting_ids,
                        confidence=item.get("confidence", 0.7)
                    ))
                return insights
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse insights from LLM: {insight_text[:100]}...")
                return []
                
        except Exception as e:
            logger.error(f"Error generating insights: {str(e)}", exc_info=True)
            return []
    
    async def _evaluate_goals(self):
        """Evaluate progress on current goals"""
        for goal_id, goal in list(self.goals.items()):
            if goal.status != "active":
                continue
                
            # Check if goal criteria are met
            is_achieved = await self._check_goal_achievement(goal)
            if is_achieved:
                goal.achieve()
                logger.info(f"Agent {self.agent_id} achieved goal: {goal.description}")
            
            # Check if goal should be abandoned
            if not is_achieved and await self._should_abandon_goal(goal):
                goal.abandon()
                logger.info(f"Agent {self.agent_id} abandoned goal: {goal.description}")
    
    async def _check_goal_achievement(self, goal: Goal) -> bool:
        """Check if a goal has been achieved"""
        # Implementation would depend on the goal criteria
        return False
    
    async def _should_abandon_goal(self, goal: Goal) -> bool:
        """Determine if a goal should be abandoned"""
        # Implementation would check for timeouts, impossibility, etc.
        return False
    
    async def _consider_new_goals(self):
        """Consider creating new goals based on current state"""
        if not self.llm_provider:
            return
            
        # Count active goals
        active_goals = sum(1 for g in self.goals.values() if g.status == "active")
        
        # Only create new goals if we don't have too many already
        if active_goals >= 3:
            return
            
        try:
            # Get recent insights
            recent_insights = sorted(self.insights, key=lambda i: i.timestamp, reverse=True)[:5]
            
            if not recent_insights:
                return
                
            # Format insights for LLM
            insight_texts = [f"Insight: {i.content}" for i in recent_insights]
            insight_context = "\n".join(insight_texts)
            
            # Current goals
            current_goal_texts = [f"Current Goal: {g.description}" for g in self.goals.values() 
                              if g.status == "active"]
            goal_context = "\n".join(current_goal_texts) if current_goal_texts else "No current goals."
            
            # Generate potential goals
            prompt = f"""As {self.agent_type}, consider these recent insights:

{insight_context}

{goal_context}

Based on these insights and considering current goals, suggest 1-2 new goals I should pursue.
Each goal should be:
1. Specific and actionable
2. Relevant to my role as {self.agent_type}
3. Achievable with available resources

Format: [
  {{
    "description": "goal description",
    "priority": 0.X (0.0-1.0),
    "criteria": {{ "specific_measurable_criteria": "value" }}
  }},
  ...
]"""

            goals_text = await self.llm_provider.generate(prompt)
            
            # Parse goals
            try:
                goals_data = json.loads(goals_text)
                for item in goals_data:
                    new_goal = Goal(
                        description=item.get("description", ""),
                        priority=item.get("priority", 0.5),
                        criteria=item.get("criteria", {})
                    )
                    self.goals[new_goal.id] = new_goal
                    logger.info(f"Agent {self.agent_id} created new goal: {new_goal.description}")
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse goals from LLM: {goals_text[:100]}...")
                
        except Exception as e:
            logger.error(f"Error considering new goals: {str(e)}", exc_info=True)
    
    async def _plan_actions_for_goals(self):
        """Plan actions to achieve active goals"""
        active_goals = [g for g in self.goals.values() if g.status == "active"]
        
        if not active_goals or not self.llm_provider:
            return
            
        # Sort goals by priority
        active_goals.sort(key=lambda g: g.priority, reverse=True)
        
        # For each goal, plan actions
        for goal in active_goals:
            await self._plan_for_goal(goal)
    
    async def _plan_for_goal(self, goal: Goal):
        """Plan actions for a specific goal"""
        # This would create tasks to achieve the goal
        pass
    
    async def remember(self, content: Any, context: Dict[str, Any] = None, importance: float = 0.5):
        """Store a new memory"""
        memory = Memory(
            content=content,
            context=context or {},
            importance=importance
        )
        self.memories[memory.id] = memory
        
        # Prune memories if we exceed capacity
        if len(self.memories) > self.memory_capacity:
            self._prune_memories()
            
        return memory.id
    
    def _prune_memories(self):
        """Remove less important memories when we exceed capacity"""
        if len(self.memories) <= self.memory_capacity:
            return
            
        # Calculate memory value based on importance, recency, and access count
        def memory_value(mem):
            recency_factor = 1.0 / (1.0 + (time.time() - mem.last_accessed) / 86400)  # Days
            access_factor = min(1.0, 0.1 * mem.access_count)
            return mem.importance * 0.6 + recency_factor * 0.3 + access_factor * 0.1
            
        # Sort by value
        sorted_memories = sorted(self.memories.values(), key=memory_value)
        
        # Remove lowest value memories
        to_remove = len(self.memories) - self.memory_capacity
        for memory in sorted_memories[:to_remove]:
            del self.memories[memory.id]
    
    async def observe(self, content: str, source: str, confidence: float = 1.0) -> str:
        """Record an observation"""
        observation = Observation(
            content=content,
            source=source,
            confidence=confidence
        )
        self.observations.append(observation)
        
        # Limit observations list size
        if len(self.observations) > 100:
            self.observations = self.observations[-100:]
            
        # High importance observations become memories
        if confidence > 0.7:
            await self.remember(content, 
                              context={"source": source, "observation_id": observation.id},
                              importance=confidence)
            
        return observation.id
    
    async def create_goal(self, description: str, priority: float = 0.5, 
                        criteria: Dict[str, Any] = None) -> str:
        """Create a new goal for this agent"""
        goal = Goal(
            description=description,
            priority=priority,
            criteria=criteria or {}
        )
        self.goals[goal.id] = goal
        logger.info(f"Agent {self.agent_id} was assigned goal: {description}")
        return goal.id
    
    async def share_knowledge(self, topic: str, content: Any) -> bool:
        """Share knowledge with other agents"""
        self.shared_knowledge[topic] = {
            "content": content,
            "source_agent": self.agent_id,
            "timestamp": time.time()
        }
        # In a real implementation, this would publish to a shared knowledge base
        return True
    
    async def process_task(self, task: Task) -> Any:
        """Process a task with cognitive capabilities"""
        # Record this task as an observation
        await self.observe(
            f"Received task: {task.task_type} (ID: {task.task_id})",
            source="task_manager",
            confidence=1.0
        )
        
        # Process the task with the parent implementation
        try:
            result = await super().process_task(task)
            
            # Record successful completion
            await self.observe(
                f"Completed task: {task.task_type} (ID: {task.task_id})",
                source="self",
                confidence=1.0
            )
            
            # Remember important task results
            await self.remember(
                f"Task {task.task_id} ({task.task_type}) completed with result: {str(result)[:100]}...",
                context={"task_id": task.task_id, "task_type": task.task_type},
                importance=0.7
            )
            
            return result
            
        except Exception as e:
            # Record task failure
            await self.observe(
                f"Failed task: {task.task_type} (ID: {task.task_id}) with error: {str(e)}",
                source="self",
                confidence=1.0
            )
            raise