"""
Root Agent - Main orchestration agent that coordinates between specialized agents.
Handles routing, context management, and cross-agent collaboration.
"""

import json
import logging
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime
from dataclasses import dataclass, asdict
from enum import Enum
import asyncio
from concurrent.futures import ThreadPoolExecutor

from langchain.agents import AgentExecutor, Tool
from langchain.agents.format_scratchpad import format_log_to_str
from langchain.agents.output_parsers import ReActSingleInputOutputParser
from langchain.prompts import PromptTemplate
from langchain.tools.render import render_text_description
from langchain.schema import AgentAction, AgentFinish
from langchain.chat_models import ChatOpenAI
from langchain.memory import ConversationBufferMemory
from langchain.callbacks.streaming_stdout import StreamingStdOutCallbackHandler

from agents.sales_agent import SalesAgent, create_sales_agent
from agents.marketing_agent import MarketingAgent, create_marketing_agent
from agents.anomaly_agent import AnomalyAgent
from tools.snowflake_query_tool import SnowflakeQueryTool
from tools.alert_tool import AlertTool
from tools.report_generator import ReportGenerator
from memory.vector_store import VectorStore
from memory.embeddings import EmbeddingManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AgentType(Enum):
    """Types of specialized agents."""
    SALES = "sales"
    MARKETING = "marketing"
    ANOMALY = "anomaly"
    CUSTOMER_SERVICE = "customer_service"  # Future expansion
    INVENTORY = "inventory"  # Future expansion


@dataclass
class AgentRequest:
    """Request structure for agent invocation."""
    query: str
    context: Dict[str, Any]
    user_id: str
    session_id: str
    priority: str  # "low", "medium", "high", "critical"
    required_agents: List[AgentType]
    deadline: Optional[datetime] = None


@dataclass
class AgentResponse:
    """Response structure from agent."""
    agent_type: AgentType
    agent_id: str
    response: str
    confidence: float
    insights: List[Dict]
    metadata: Dict[str, Any]
    processing_time: float


@dataclass
class OrchestrationResult:
    """Result of agent orchestration."""
    request_id: str
    user_id: str
    session_id: str
    primary_agent: AgentType
    responses: Dict[AgentType, AgentResponse]
    consolidated_response: str
    recommendations: List[Dict]
    next_actions: List[Dict]
    timestamp: datetime


class RootAgent:
    """
    Root orchestration agent that coordinates between specialized agents.
    """
    
    def __init__(
        self,
        agent_id: str = "root_agent_001",
        model_name: str = "gpt-4",
        temperature: float = 0.2,
        verbose: bool = False,
        max_workers: int = 5
    ):
        """
        Initialize Root Agent.
        
        Args:
            agent_id: Unique identifier for the agent
            model_name: LLM model to use
            temperature: Model temperature
            verbose: Enable verbose logging
            max_workers: Maximum number of parallel workers
        """
        self.agent_id = agent_id
        self.verbose = verbose
        
        # Initialize LLM
        self.llm = ChatOpenAI(
            model_name=model_name,
            temperature=temperature,
            streaming=True,
            callbacks=[StreamingStdOutCallbackHandler()] if verbose else []
        )
        
        # Initialize specialized agents
        self.sales_agents = self._initialize_sales_agents()
        self.marketing_agents = self._initialize_marketing_agents()
        self.anomaly_agent = AnomalyAgent(agent_id="anomaly_detector_001")
        
        # Initialize tools
        self.query_tool = SnowflakeQueryTool()
        self.alert_tool = AlertTool()
        self.report_tool = ReportGenerator()
        
        # Initialize memory and embeddings
        self.memory = ConversationBufferMemory(
            memory_key="chat_history",
            return_messages=True,
            max_token_limit=4000
        )
        self.vector_store = VectorStore()
        self.embeddings = EmbeddingManager()
        
        # Initialize executor for parallel processing
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # Initialize root agent
        self.agent_executor = self._create_agent()
        
        # Agent state
        self.active_sessions = {}
        self.request_history = []
        self.agent_performance = {}
        
        logger.info(f"Root Agent {agent_id} initialized with {max_workers} workers")
    
    def _initialize_sales_agents(self) -> Dict[str, SalesAgent]:
        """Initialize specialized sales agents."""
        return {
            "performance": create_sales_agent("sales_performance_001", "performance_analyst"),
            "customer_insights": create_sales_agent("sales_customer_001", "customer_insights"),
            "revenue_optimization": create_sales_agent("sales_revenue_001", "revenue_optimization"),
            "forecasting": create_sales_agent("sales_forecasting_001", "forecasting")
        }
    
    def _initialize_marketing_agents(self) -> Dict[str, MarketingAgent]:
        """Initialize specialized marketing agents."""
        return {
            "campaign": create_marketing_agent("marketing_campaign_001", "campaign_analyst"),
            "acquisition": create_marketing_agent("marketing_acquisition_001", "customer_acquisition"),
            "roi": create_marketing_agent("marketing_roi_001", "roi_optimization"),
            "content": create_marketing_agent("marketing_content_001", "content_strategy")
        }
    
    def _create_agent(self) -> AgentExecutor:
        """Create the root orchestration agent."""
        
        # Define root agent tools
        tools = [
            Tool(
                name="route_to_sales_agent",
                func=self.route_to_sales_agent,
                description="Route query to appropriate sales agent based on topic."
            ),
            Tool(
                name="route_to_marketing_agent",
                func=self.route_to_marketing_agent,
                description="Route query to appropriate marketing agent based on topic."
            ),
            Tool(
                name="route_to_anomaly_agent",
                func=self.route_to_anomaly_agent,
                description="Route query to anomaly detection agent."
            ),
            Tool(
                name="orchestrate_multiple_agents",
                func=self.orchestrate_multiple_agents,
                description="Orchestrate multiple agents to work on complex queries."
            ),
            Tool(
                name="consolidate_responses",
                func=self.consolidate_responses,
                description="Consolidate responses from multiple agents into a coherent answer."
            ),
            Tool(
                name="query_knowledge_base",
                func=self.query_knowledge_base,
                description="Query the knowledge base for historical information and context."
            ),
            Tool(
                name="update_session_context",
                func=self.update_session_context,
                description="Update session context for ongoing conversations."
            ),
            Tool(
                name="generate_executive_summary",
                func=self.generate_executive_summary,
                description="Generate executive summary from multiple agent responses."
            ),
            Tool(
                name="escalate_to_human",
                func=self.escalate_to_human,
                description="Escalate complex issues to human experts."
            )
        ]
        
        # Create agent prompt template
        prompt_template = """
        You are the Root Orchestration AI Agent. Your role is to coordinate between specialized agents,
        manage context, and ensure comprehensive responses to user queries.
        
        Responsibilities:
        1. Understand user query intent and context
        2. Route queries to appropriate specialized agents
        3. Coordinate multiple agents for complex queries
        4. Consolidate and synthesize agent responses
        5. Manage session context and memory
        6. Handle escalations and exceptions
        
        Available Specialized Agents:
        - Sales Agents: performance analysis, customer insights, revenue optimization, forecasting
        - Marketing Agents: campaign analysis, customer acquisition, ROI optimization, content strategy
        - Anomaly Agent: fraud detection, anomaly monitoring, system health
        
        Routing Guidelines:
        1. Sales-related queries → Sales Agents
           - Sales performance, revenue, forecasting, customer analysis
        2. Marketing-related queries → Marketing Agents
           - Campaigns, acquisition, ROI, content, promotions
        3. Anomaly detection → Anomaly Agent
           - Fraud, unusual patterns, system issues, data quality
        4. Complex queries → Multiple Agents
           - Business strategy, cross-functional analysis, executive decisions
        
        Response Protocol:
        1. Acknowledge query and processing plan
        2. Route to appropriate agent(s)
        3. Consolidate responses
        4. Provide comprehensive answer with recommendations
        5. Suggest next steps or follow-up questions
        
        Current Date: {current_date}
        Session Context: {session_context}
        
        Previous conversation:
        {chat_history}
        
        Tools available:
        {tools}
        
        User Input: {input}
        
        {agent_scratchpad}
        """
        
        # Create agent
        agent = {
            "input": lambda x: x["input"],
            "agent_scratchpad": lambda x: format_log_to_str(x["intermediate_steps"]),
            "chat_history": lambda x: x["chat_history"],
            "current_date": lambda x: datetime.now().strftime("%Y-%m-%d"),
            "session_context": lambda x: x.get("session_context", "No active session")
        }
        
        prompt = PromptTemplate(
            template=prompt_template,
            input_variables=["input", "chat_history", "current_date", "session_context"],
            partial_variables={
                "tools": render_text_description(tools),
                "tool_names": ", ".join([t.name for t in tools])
            }
        )
        
        # Create agent chain
        agent_chain = agent | prompt | self.llm | ReActSingleInputOutputParser()
        
        return AgentExecutor(
            agent=agent_chain,
            tools=tools,
            memory=self.memory,
            verbose=self.verbose,
            handle_parsing_errors=True,
            max_iterations=6,
            early_stopping_method="generate"
        )
    
    async def process_request(self, request: AgentRequest) -> OrchestrationResult:
        """
        Process a user request through the agent system.
        
        Args:
            request: Agent request object
            
        Returns:
            Orchestration result
        """
        start_time = datetime.now()
        request_id = f"req_{start_time.strftime('%Y%m%d_%H%M%S')}_{hash(request.query) % 10000:04d}"
        
        logger.info(f"Processing request {request_id}: {request.query[:100]}...")
        
        try:
            # Update session context
            session_key = f"{request.user_id}_{request.session_id}"
            if session_key not in self.active_sessions:
                self.active_sessions[session_key] = {
                    "user_id": request.user_id,
                    "session_id": request.session_id,
                    "created_at": datetime.now(),
                    "context": request.context,
                    "history": []
                }
            
            # Determine which agents to involve
            agents_to_invoke = await self._determine_agents(request)
            
            # Invoke agents in parallel
            agent_responses = await self._invoke_agents_parallel(
                request.query,
                agents_to_invoke,
                request.context
            )
            
            # Consolidate responses
            consolidated = await self._consolidate_responses(agent_responses)
            
            # Generate recommendations and next actions
            recommendations = await self._generate_recommendations(agent_responses)
            next_actions = await self._determine_next_actions(agent_responses, request)
            
            # Update session history
            self.active_sessions[session_key]["history"].append({
                "request_id": request_id,
                "query": request.query,
                "agents_invoked": [agt.value for agt in agents_to_invoke],
                "timestamp": datetime.now().isoformat()
            })
            
            # Update request history
            self.request_history.append({
                "request_id": request_id,
                "query": request.query,
                "user_id": request.user_id,
                "agents": [agt.value for agt in agents_to_invoke],
                "processing_time": (datetime.now() - start_time).total_seconds(),
                "timestamp": start_time.isoformat()
            })
            
            # Update agent performance metrics
            self._update_agent_performance(agent_responses)
            
            # Store in knowledge base
            await self._store_in_knowledge_base(request, agent_responses, consolidated)
            
            # Create orchestration result
            result = OrchestrationResult(
                request_id=request_id,
                user_id=request.user_id,
                session_id=request.session_id,
                primary_agent=self._determine_primary_agent(agents_to_invoke),
                responses=agent_responses,
                consolidated_response=consolidated,
                recommendations=recommendations,
                next_actions=next_actions,
                timestamp=datetime.now()
            )
            
            logger.info(f"Request {request_id} completed in {(datetime.now() - start_time).total_seconds():.2f}s")
            return result
            
        except Exception as e:
            logger.error(f"Error processing request {request_id}: {str(e)}")
            
            # Create error result
            return OrchestrationResult(
                request_id=request_id,
                user_id=request.user_id,
                session_id=request.session_id,
                primary_agent=AgentType.SALES,  # Default
                responses={},
                consolidated_response=f"I encountered an error processing your request: {str(e)}",
                recommendations=[],
                next_actions=[{"action": "retry", "description": "Please try again or rephrase your question"}],
                timestamp=datetime.now()
            )
    
    async def _determine_agents(self, request: AgentRequest) -> List[AgentType]:
        """
        Determine which agents to invoke based on the query.
        
        Args:
            request: Agent request
            
        Returns:
            List of agent types to invoke
        """
        # If required agents are specified, use them
        if request.required_agents:
            return request.required_agents
        
        # Analyze query to determine appropriate agents
        query_lower = request.query.lower()
        
        agents = set()
        
        # Sales-related keywords
        sales_keywords = ["sales", "revenue", "profit", "customer", "product", "forecast", "inventory"]
        if any(keyword in query_lower for keyword in sales_keywords):
            agents.add(AgentType.SALES)
        
        # Marketing-related keywords
        marketing_keywords = ["marketing", "campaign", "acquisition", "roi", "conversion", "ctr", "impression"]
        if any(keyword in query_lower for keyword in marketing_keywords):
            agents.add(AgentType.MARKETING)
        
        # Anomaly-related keywords
        anomaly_keywords = ["anomaly", "fraud", "suspicious", "unusual", "error", "issue", "problem"]
        if any(keyword in query_lower for keyword in anomaly_keywords):
            agents.add(AgentType.ANOMALY)
        
        # Complex queries might need multiple agents
        if len(query_lower.split()) > 15 or "strategy" in query_lower or "overview" in query_lower:
            agents.update([AgentType.SALES, AgentType.MARKETING])
        
        # Default to sales agent if no specific agent determined
        if not agents:
            agents.add(AgentType.SALES)
        
        return list(agents)
    
    async def _invoke_agents_parallel(
        self,
        query: str,
        agents: List[AgentType],
        context: Dict
    ) -> Dict[AgentType, AgentResponse]:
        """
        Invoke multiple agents in parallel.
        
        Args:
            query: User query
            agents: List of agent types to invoke
            context: Additional context
            
        Returns:
            Dictionary of agent responses
        """
        responses = {}
        
        # Create tasks for each agent
        tasks = []
        for agent_type in agents:
            task = self._invoke_single_agent(agent_type, query, context)
            tasks.append((agent_type, task))
        
        # Execute tasks in parallel
        results = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)
        
        # Process results
        for (agent_type, _), result in zip(tasks, results):
            if isinstance(result, Exception):
                logger.error(f"Agent {agent_type.value} failed: {str(result)}")
                responses[agent_type] = AgentResponse(
                    agent_type=agent_type,
                    agent_id=f"{agent_type.value}_error",
                    response=f"Agent encountered an error: {str(result)}",
                    confidence=0.0,
                    insights=[],
                    metadata={"error": True},
                    processing_time=0.0
                )
            else:
                responses[agent_type] = result
        
        return responses
    
    async def _invoke_single_agent(
        self,
        agent_type: AgentType,
        query: str,
        context: Dict
    ) -> AgentResponse:
        """
        Invoke a single agent.
        
        Args:
            agent_type: Type of agent to invoke
            query: User query
            context: Additional context
            
        Returns:
            Agent response
        """
        start_time = datetime.now()
        
        try:
            if agent_type == AgentType.SALES:
                # Determine which sales agent to use
                sales_agent = await self._select_sales_agent(query)
                result = await sales_agent.process_query(query, context)
                
                response = AgentResponse(
                    agent_type=agent_type,
                    agent_id=sales_agent.agent_id,
                    response=result.get("response", ""),
                    confidence=result.get("confidence", 0.5),
                    insights=result.get("insights", []),
                    metadata={
                        "role": sales_agent.role.value,
                        "agent_status": sales_agent.get_agent_status()
                    },
                    processing_time=(datetime.now() - start_time).total_seconds()
                )
                
            elif agent_type == AgentType.MARKETING:
                # Determine which marketing agent to use
                marketing_agent = await self._select_marketing_agent(query)
                result = await marketing_agent.process_query(query, context)
                
                response = AgentResponse(
                    agent_type=agent_type,
                    agent_id=marketing_agent.agent_id,
                    response=result.get("response", ""),
                    confidence=0.7,  # Marketing agents don't return confidence
                    insights=result.get("insights", []),
                    metadata={
                        "role": marketing_agent.role.value,
                        "agent_status": marketing_agent.get_agent_status()
                    },
                    processing_time=(datetime.now() - start_time).total_seconds()
                )
                
            elif agent_type == AgentType.ANOMALY:
                result = await self.anomaly_agent.process_query(query, context)
                
                response = AgentResponse(
                    agent_type=agent_type,
                    agent_id=self.anomaly_agent.agent_id,
                    response=result.get("response", ""),
                    confidence=0.8,  # Anomaly agents have high confidence
                    insights=result.get("detections", []),
                    metadata={
                        "agent_status": self.anomaly_agent.get_agent_status()
                    },
                    processing_time=(datetime.now() - start_time).total_seconds()
                )
                
            else:
                # Default response for unknown agent types
                response = AgentResponse(
                    agent_type=agent_type,
                    agent_id=f"{agent_type.value}_unknown",
                    response=f"Agent type {agent_type.value} is not yet implemented.",
                    confidence=0.0,
                    insights=[],
                    metadata={"error": "not_implemented"},
                    processing_time=(datetime.now() - start_time).total_seconds()
                )
            
            return response
            
        except Exception as e:
            logger.error(f"Error invoking agent {agent_type.value}: {str(e)}")
            raise
    
    async def _select_sales_agent(self, query: str) -> SalesAgent:
        """
        Select appropriate sales agent based on query.
        
        Args:
            query: User query
            
        Returns:
            Selected sales agent
        """
        query_lower = query.lower()
        
        # Simple keyword-based routing
        if any(word in query_lower for word in ["customer", "segment", "churn", "retention"]):
            return self.sales_agents["customer_insights"]
        elif any(word in query_lower for word in ["price", "discount", "margin", "profit"]):
            return self.sales_agents["revenue_optimization"]
        elif any(word in query_lower for word in ["forecast", "predict", "trend", "future"]):
            return self.sales_agents["forecasting"]
        else:
            return self.sales_agents["performance"]  # Default
    
    async def _select_marketing_agent(self, query: str) -> MarketingAgent:
        """
        Select appropriate marketing agent based on query.
        
        Args:
            query: User query
            
        Returns:
            Selected marketing agent
        """
        query_lower = query.lower()
        
        # Simple keyword-based routing
        if any(word in query_lower for word in ["campaign", "ad", "promotion"]):
            return self.marketing_agents["campaign"]
        elif any(word in query_lower for word in ["acquisition", "lead", "conversion"]):
            return self.marketing_agents["acquisition"]
        elif any(word in query_lower for word in ["roi", "return", "investment", "spend"]):
            return self.marketing_agents["roi"]
        elif any(word in query_lower for word in ["content", "article", "social", "email"]):
            return self.marketing_agents["content"]
        else:
            return self.marketing_agents["campaign"]  # Default
    
    async def _consolidate_responses(self, responses: Dict[AgentType, AgentResponse]) -> str:
        """
        Consolidate multiple agent responses into a coherent answer.
        
        Args:
            responses: Dictionary of agent responses
            
        Returns:
            Consolidated response
        """
        if not responses:
            return "No agents were able to process your query."
        
        if len(responses) == 1:
            # Single agent response
            response = list(responses.values())[0]
            return response.response
        
        # Multiple agents - consolidate using LLM
        consolidation_prompt = """
        You are consolidating responses from multiple AI agents into a single coherent answer.
        
        Original Query: {query}
        
        Agent Responses:
        {agent_responses}
        
        Please consolidate these responses into a single comprehensive answer that:
        1. Addresses all aspects of the original query
        2. Resolves any contradictions between agents
        3. Provides a clear, structured response
        4. Highlights key insights and recommendations
        5. Maintains the expertise of each specialized agent
        
        Consolidated Response:
        """
        
        # Format agent responses
        formatted_responses = []
        for agent_type, response in responses.items():
            formatted_responses.append(f"""
            Agent: {agent_type.value.upper()}
            Confidence: {response.confidence:.1%}
            Response: {response.response}
            Key Insights: {json.dumps(response.insights[:3], indent=2) if response.insights else 'None'}
            """)
        
        # Get original query from first response metadata
        query = "Unknown query"
        if responses:
            first_response = list(responses.values())[0]
            if "original_query" in first_response.metadata:
                query = first_response.metadata["original_query"]
        
        # Generate consolidated response
        full_prompt = consolidation_prompt.format(
            query=query,
            agent_responses="\n---\n".join(formatted_responses)
        )
        
        try:
            consolidated_response = await self.llm.agenerate([full_prompt])
            return consolidated_response.generations[0][0].text
        except Exception as e:
            logger.error(f"Error consolidating responses: {str(e)}")
            
            # Fallback: combine responses
            combined = "Based on analysis from multiple agents:\n\n"
            for agent_type, response in responses.items():
                combined += f"## {agent_type.value.upper()} AGENT:\n{response.response}\n\n"
            return combined
    
    async def _generate_recommendations(self, responses: Dict[AgentType, AgentResponse]) -> List[Dict]:
        """
        Generate recommendations from agent responses.
        
        Args:
            responses: Dictionary of agent responses
            
        Returns:
            List of recommendations
        """
        recommendations = []
        
        for agent_type, response in responses.items():
            # Extract recommendations from insights
            for insight in response.insights:
                if isinstance(insight, dict) and "recommendation" in insight:
                    recommendations.append({
                        "agent": agent_type.value,
                        "recommendation": insight["recommendation"],
                        "confidence": response.confidence,
                        "category": insight.get("category", "general")
                    })
            
            # Also look for recommendations in the response text
            if "recommend" in response.response.lower() or "suggest" in response.response.lower():
                # Use LLM to extract recommendations from text
                extraction_prompt = f"""
                Extract actionable recommendations from the following text:
                
                {response.response}
                
                Return as JSON array of recommendations with fields: action, impact, priority (high/medium/low), timeline.
                
                JSON:
                """
                
                try:
                    extraction_result = await self.llm.agenerate([extraction_prompt])
                    extracted = json.loads(extraction_result.generations[0][0].text)
                    
                    for rec in extracted:
                        recommendations.append({
                            "agent": agent_type.value,
                            "recommendation": rec.get("action", ""),
                            "impact": rec.get("impact", ""),
                            "priority": rec.get("priority", "medium"),
                            "timeline": rec.get("timeline", ""),
                            "source": "text_extraction"
                        })
                except:
                    pass
        
        # Remove duplicates and limit to top 5
        unique_recs = []
        seen = set()
        for rec in recommendations:
            rec_key = rec["recommendation"][:100]
            if rec_key not in seen:
                seen.add(rec_key)
                unique_recs.append(rec)
        
        return unique_recs[:5]
    
    async def _determine_next_actions(
        self,
        responses: Dict[AgentType, AgentResponse],
        request: AgentRequest
    ) -> List[Dict]:
        """
        Determine next actions based on agent responses.
        
        Args:
            responses: Dictionary of agent responses
            request: Original request
            
        Returns:
            List of next actions
        """
        actions = []
        
        # Check for follow-up questions
        follow_up_prompt = f"""
        Based on this query and agent responses, suggest 3 follow-up questions or next actions:
        
        Original Query: {request.query}
        
        Responses from {len(responses)} agents received.
        
        Suggest follow-ups that:
        1. Dive deeper into key insights
        2. Explore related areas
        3. Address potential next steps
        4. Are practical and actionable
        
        Return as JSON array with fields: action_type (question/analysis/monitoring), description, priority.
        
        JSON:
        """
        
        try:
            follow_up_result = await self.llm.agenerate([follow_up_prompt])
            follow_ups = json.loads(follow_up_result.generations[0][0].text)
            
            for fu in follow_ups:
                actions.append({
                    "type": fu.get("action_type", "question"),
                    "description": fu.get("description", ""),
                    "priority": fu.get("priority", "medium"),
                    "suggested_by": "root_agent"
                })
        except:
            # Default follow-ups
            actions = [
                {
                    "type": "question",
                    "description": "Would you like me to analyze this further or compare with historical data?",
                    "priority": "medium",
                    "suggested_by": "default"
                },
                {
                    "type": "monitoring",
                    "description": "I can set up monitoring for key metrics mentioned in the analysis",
                    "priority": "low",
                    "suggested_by": "default"
                }
            ]
        
        return actions
    
    def _determine_primary_agent(self, agents: List[AgentType]) -> AgentType:
        """Determine the primary agent from a list."""
        if not agents:
            return AgentType.SALES
        
        # Simple heuristic: sales first, then marketing, then anomaly
        if AgentType.SALES in agents:
            return AgentType.SALES
        elif AgentType.MARKETING in agents:
            return AgentType.MARKETING
        elif AgentType.ANOMALY in agents:
            return AgentType.ANOMALY
        else:
            return agents[0]
    
    def _update_agent_performance(self, responses: Dict[AgentType, AgentResponse]):
        """Update agent performance metrics."""
        for agent_type, response in responses.items():
            agent_key = f"{agent_type.value}_{response.agent_id}"
            
            if agent_key not in self.agent_performance:
                self.agent_performance[agent_key] = {
                    "invocations": 0,
                    "total_confidence": 0,
                    "avg_processing_time": 0,
                    "last_invoked": None
                }
            
            perf = self.agent_performance[agent_key]
            perf["invocations"] += 1
            perf["total_confidence"] += response.confidence
            perf["avg_processing_time"] = (
                (perf["avg_processing_time"] * (perf["invocations"] - 1) + response.processing_time) 
                / perf["invocations"]
            )
            perf["last_invoked"] = datetime.now().isoformat()
    
    async def _store_in_knowledge_base(
        self,
        request: AgentRequest,
        responses: Dict[AgentType, AgentResponse],
        consolidated: str
    ):
        """Store request and responses in knowledge base."""
        try:
            # Create document for vector store
            doc_text = f"""
            Query: {request.query}
            
            Consolidated Response: {consolidated}
            
            Involved Agents: {[agt.value for agt in responses.keys()]}
            
            User Context: {json.dumps(request.context, indent=2)}
            """
            
            await self.vector_store.store_document(
                text=doc_text,
                metadata={
                    "request_id": f"req_{hash(request.query) % 10000:04d}",
                    "user_id": request.user_id,
                    "session_id": request.session_id,
                    "agents": [agt.value for agt in responses.keys()],
                    "timestamp": datetime.now().isoformat(),
                    "priority": request.priority,
                    "type": "orchestrated_response"
                }
            )
        except Exception as e:
            logger.warning(f"Failed to store in knowledge base: {str(e)}")
    
    # Tool implementations for the root agent
    
    async def route_to_sales_agent(self, query: str, context: Dict = None) -> str:
        """Route query to sales agent."""
        try:
            agent = await self._select_sales_agent(query)
            result = await agent.process_query(query, context or {})
            return f"Sales Agent ({agent.role.value}) Response:\n\n{result.get('response', '')}"
        except Exception as e:
            return f"Error routing to sales agent: {str(e)}"
    
    async def route_to_marketing_agent(self, query: str, context: Dict = None) -> str:
        """Route query to marketing agent."""
        try:
            agent = await self._select_marketing_agent(query)
            result = await agent.process_query(query, context or {})
            return f"Marketing Agent ({agent.role.value}) Response:\n\n{result.get('response', '')}"
        except Exception as e:
            return f"Error routing to marketing agent: {str(e)}"
    
    async def route_to_anomaly_agent(self, query: str, context: Dict = None) -> str:
        """Route query to anomaly agent."""
        try:
            result = await self.anomaly_agent.process_query(query, context or {})
            return f"Anomaly Agent Response:\n\n{result.get('response', '')}"
        except Exception as e:
            return f"Error routing to anomaly agent: {str(e)}"
    
    async def orchestrate_multiple_agents(
        self,
        query: str,
        agent_types: List[str],
        context: Dict = None
    ) -> str:
        """Orchestrate multiple agents for a complex query."""
        try:
            # Parse agent types
            agents = [AgentType(agt.lower()) for agt in agent_types]
            
            # Create request
            request = AgentRequest(
                query=query,
                context=context or {},
                user_id="system",
                session_id="orchestration",
                priority="medium",
                required_agents=agents
            )
            
            # Process request
            result = await self.process_request(request)
            
            return result.consolidated_response
        except Exception as e:
            return f"Error orchestrating agents: {str(e)}"
    
    async def consolidate_responses(self, responses_json: str) -> str:
        """Consolidate multiple agent responses."""
        try:
            responses = json.loads(responses_json)
            
            # Convert to AgentResponse objects
            agent_responses = {}
            for resp in responses:
                agent_type = AgentType(resp["agent_type"])
                agent_responses[agent_type] = AgentResponse(
                    agent_type=agent_type,
                    agent_id=resp["agent_id"],
                    response=resp["response"],
                    confidence=resp.get("confidence", 0.5),
                    insights=resp.get("insights", []),
                    metadata=resp.get("metadata", {}),
                    processing_time=resp.get("processing_time", 0)
                )
            
            return await self._consolidate_responses(agent_responses)
        except Exception as e:
            return f"Error consolidating responses: {str(e)}"
    
    async def query_knowledge_base(self, query: str, limit: int = 5) -> str:
        """Query the knowledge base."""
        try:
            results = await self.vector_store.search_documents(query, limit=limit)
            
            if not results:
                return "No relevant information found in knowledge base."
            
            formatted = "Relevant information from knowledge base:\n\n"
            for i, result in enumerate(results, 1):
                formatted += f"{i}. {result['text'][:500]}...\n"
                formatted += f"   Source: {result['metadata'].get('type', 'unknown')}"
                formatted += f" | Date: {result['metadata'].get('timestamp', 'unknown')}\n\n"
            
            return formatted
        except Exception as e:
            return f"Error querying knowledge base: {str(e)}"
    
    async def update_session_context(self, session_id: str, context_updates: Dict) -> str:
        """Update session context."""
        try:
            # Find session
            for session_key, session_data in self.active_sessions.items():
                if session_data["session_id"] == session_id:
                    session_data["context"].update(context_updates)
                    return f"Session {session_id} context updated successfully."
            
            return f"Session {session_id} not found."
        except Exception as e:
            return f"Error updating session context: {str(e)}"
    
    async def generate_executive_summary(self, responses_json: str) -> str:
        """Generate executive summary from agent responses."""
        try:
            responses = json.loads(responses_json)
            
            summary_prompt = """
            Generate an executive summary from the following agent responses.
            Focus on key findings, recommendations, and business impact.
            Keep it concise (3-5 paragraphs) and suitable for executive audience.
            
            Agent Responses:
            {responses}
            
            Executive Summary:
            """
            
            formatted_responses = []
            for resp in responses:
                formatted_responses.append(f"""
                Agent: {resp.get('agent_type', 'unknown')}
                Response: {resp.get('response', '')}
                Key Insights: {json.dumps(resp.get('insights', []), indent=2)}
                """)
            
            full_prompt = summary_prompt.format(
                responses="\n---\n".join(formatted_responses)
            )
            
            summary_result = await self.llm.agenerate([full_prompt])
            return summary_result.generations[0][0].text
            
        except Exception as e:
            return f"Error generating executive summary: {str(e)}"
    
    async def escalate_to_human(self, reason: str, details: Dict = None) -> str:
        """Escalate to human experts."""
        try:
            # Create escalation alert
            escalation_message = f"""
            ⚠️ HUMAN ESCALATION REQUIRED ⚠️
            
            Reason: {reason}
            
            Details: {json.dumps(details or {}, indent=2)}
            
            Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            Root Agent: {self.agent_id}
            """
            
            await self.alert_tool.send_alert(
                message=escalation_message,
                level="critical",
                category="human_escalation"
            )
            
            return "Issue has been escalated to human experts. They will contact you shortly."
        except Exception as e:
            return f"Error escalating to human: {str(e)}"
    
    def get_system_status(self) -> Dict:
        """Get overall system status."""
        return {
            "root_agent": {
                "agent_id": self.agent_id,
                "status": "active",
                "model": self.llm.model_name,
                "active_sessions": len(self.active_sessions),
                "total_requests": len(self.request_history),
                "avg_processing_time": (
                    sum(req["processing_time"] for req in self.request_history[-100:]) / 
                    min(100, len(self.request_history)) if self.request_history else 0
                )
            },
            "specialized_agents": {
                "sales_agents": len(self.sales_agents),
                "marketing_agents": len(self.marketing_agents),
                "anomaly_agents": 1
            },
            "performance_metrics": {
                "top_agents": sorted(
                    self.agent_performance.items(),
                    key=lambda x: x[1]["invocations"],
                    reverse=True
                )[:5]
            },
            "memory_usage": {
                "conversation_memory": len(self.memory.chat_memory.messages) if hasattr(self.memory.chat_memory, 'messages') else 0,
                "vector_store_docs": "unknown"  # Would need method to get count
            }
        }
    
    async def cleanup_old_sessions(self, max_age_hours: int = 24):
        """Clean up old sessions."""
        cutoff = datetime.now() - timedelta(hours=max_age_hours)
        
        old_sessions = []
        for session_key, session_data in self.active_sessions.items():
            if session_data["created_at"] < cutoff:
                old_sessions.append(session_key)
        
        for session_key in old_sessions:
            del self.active_sessions[session_key]
        
        logger.info(f"Cleaned up {len(old_sessions)} old sessions")
    
    async def start_monitoring(self):
        """Start monitoring tasks for all agents."""
        # Start anomaly agent monitoring
        self.anomaly_monitoring_task = asyncio.create_task(
            self.anomaly_agent.continuous_monitoring(interval_minutes=5)
        )
        
        logger.info("Started monitoring tasks for all agents")


# Factory function for creating root agent
def create_root_agent(
    agent_id: str = "root_agent_001",
    **kwargs
) -> RootAgent:
    """
    Factory function to create root agent.
    
    Args:
        agent_id: Unique agent identifier
        **kwargs: Additional agent parameters
        
    Returns:
        Configured RootAgent instance
    """
    return RootAgent(agent_id=agent_id, **kwargs)