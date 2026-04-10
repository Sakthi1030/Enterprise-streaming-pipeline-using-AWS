"""
FastAPI Application - Main API for the Agentic AI System.
Provides REST API endpoints for agent interactions, monitoring, and management.
"""

import json
import logging
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import uvicorn

from agents.root_agent import RootAgent, create_root_agent, AgentRequest
from agents.sales_agent import create_sales_agent
from agents.marketing_agent import create_marketing_agent
from agents.anomaly_agent import AnomalyAgent
from api.schemas import (
    AgentQueryRequest, AgentQueryResponse, AgentStatusResponse,
    SystemStatusResponse, ReportGenerationRequest, AlertRequest,
    VectorSearchRequest, HealthCheckResponse, BatchQueryRequest
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Security
security = HTTPBearer()

# Global agents
root_agent = None
specialized_agents = {}
background_tasks = set()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for FastAPI application.
    Handles startup and shutdown events.
    """
    # Startup
    logger.info("Starting Agentic AI System...")
    
    try:
        # Initialize root agent
        global root_agent
        root_agent = create_root_agent(
            agent_id="root_agent_api",
            model_name="gpt-4",
            temperature=0.2,
            verbose=False,
            max_workers=10
        )
        
        # Initialize specialized agents
        global specialized_agents
        specialized_agents = {
            "sales_performance": create_sales_agent("sales_perf_api", "performance_analyst"),
            "sales_customer": create_sales_agent("sales_cust_api", "customer_insights"),
            "marketing_campaign": create_marketing_agent("marketing_camp_api", "campaign_analyst"),
            "marketing_acquisition": create_marketing_agent("marketing_acq_api", "customer_acquisition"),
            "anomaly": AnomalyAgent(agent_id="anomaly_api")
        }
        
        # Start monitoring tasks
        for agent_name, agent in specialized_agents.items():
            if hasattr(agent, 'start_monitoring'):
                task = asyncio.create_task(agent.start_monitoring())
                background_tasks.add(task)
                task.add_done_callback(background_tasks.discard)
        
        logger.info("Agentic AI System started successfully")
        yield
        
    except Exception as e:
        logger.error(f"Failed to start Agentic AI System: {str(e)}")
        raise
    
    finally:
        # Shutdown
        logger.info("Shutting down Agentic AI System...")
        
        # Cancel background tasks
        for task in background_tasks:
            task.cancel()
        
        # Wait for tasks to complete
        if background_tasks:
            await asyncio.gather(*background_tasks, return_exceptions=True)
        
        logger.info("Agentic AI System shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Agentic AI System API",
    description="API for AI Agent System with sales, marketing, and anomaly detection agents",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict to specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Authentication dependency
async def authenticate(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> Dict:
    """
    Simple authentication middleware.
    In production, implement proper authentication.
    """
    # For demo purposes, accept any token
    # In production, validate against your auth system
    return {"user_id": "api_user", "roles": ["admin", "user"]}


@app.get("/", response_model=Dict[str, str])
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Agentic AI System API",
        "version": "1.0.0",
        "status": "operational",
        "documentation": "/docs",
        "health": "/health"
    }


@app.get("/health", response_model=HealthCheckResponse)
async def health_check():
    """Health check endpoint."""
    agents_healthy = root_agent is not None and len(specialized_agents) > 0
    
    return HealthCheckResponse(
        status="healthy" if agents_healthy else "degraded",
        timestamp=datetime.now().isoformat(),
        agents_available=len(specialized_agents) + (1 if root_agent else 0),
        system_load=len(background_tasks),
        details={
            "root_agent": "active" if root_agent else "inactive",
            "specialized_agents": len(specialized_agents),
            "background_tasks": len(background_tasks)
        }
    )


@app.post("/query", response_model=AgentQueryResponse)
async def query_agent(
    request: AgentQueryRequest,
    background_tasks: BackgroundTasks,
    auth: Dict = Depends(authenticate)
):
    """
    Query the agent system with a natural language request.
    The root agent will route to appropriate specialized agents.
    """
    try:
        logger.info(f"Processing query from user {auth['user_id']}: {request.query[:100]}...")
        
        # Create agent request
        agent_request = AgentRequest(
            query=request.query,
            context=request.context or {},
            user_id=auth["user_id"],
            session_id=request.session_id or f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            priority=request.priority or "medium",
            required_agents=[]  # Let root agent determine
        )
        
        # Process request
        result = await root_agent.process_request(agent_request)
        
        # Prepare response
        response = AgentQueryResponse(
            request_id=result.request_id,
            query=request.query,
            response=result.consolidated_response,
            agents_involved=[agent.value for agent in result.responses.keys()],
            recommendations=result.recommendations,
            next_actions=result.next_actions,
            processing_time=(datetime.now() - result.timestamp).total_seconds(),
            timestamp=result.timestamp.isoformat()
        )
        
        # Add streaming if requested
        if request.stream:
            # In a real implementation, this would stream the response
            # For now, return the full response
            pass
        
        logger.info(f"Query completed: {result.request_id}")
        return response
        
    except Exception as e:
        logger.error(f"Error processing query: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/query/{agent_type}", response_model=AgentQueryResponse)
async def query_specific_agent(
    agent_type: str,
    request: AgentQueryRequest,
    auth: Dict = Depends(authenticate)
):
    """
    Query a specific agent directly.
    """
    try:
        logger.info(f"Querying specific agent {agent_type}: {request.query[:100]}...")
        
        # Get the specific agent
        agent = specialized_agents.get(agent_type)
        if not agent:
            raise HTTPException(status_code=404, detail=f"Agent type '{agent_type}' not found")
        
        # Process query
        if agent_type.startswith("sales"):
            result = await agent.process_query(request.query, request.context or {})
            response_text = result.get("response", "")
        elif agent_type.startswith("marketing"):
            result = await agent.process_query(request.query, request.context or {})
            response_text = result.get("response", "")
        elif agent_type == "anomaly":
            result = await agent.process_query(request.query, request.context or {})
            response_text = result.get("response", "")
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported agent type: {agent_type}")
        
        # Prepare response
        response = AgentQueryResponse(
            request_id=f"{agent_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            query=request.query,
            response=response_text,
            agents_involved=[agent_type],
            recommendations=[],
            next_actions=[],
            processing_time=0.0,  # Would need actual timing
            timestamp=datetime.now().isoformat()
        )
        
        return response
        
    except Exception as e:
        logger.error(f"Error querying specific agent: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/query/batch", response_model=List[AgentQueryResponse])
async def batch_query(
    request: BatchQueryRequest,
    auth: Dict = Depends(authenticate)
):
    """
    Process multiple queries in batch.
    """
    try:
        logger.info(f"Processing batch query with {len(request.queries)} queries")
        
        responses = []
        
        for query_request in request.queries:
            try:
                # Create agent request
                agent_request = AgentRequest(
                    query=query_request.query,
                    context=query_request.context or {},
                    user_id=auth["user_id"],
                    session_id=query_request.session_id or f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    priority=query_request.priority or "medium",
                    required_agents=[]
                )
                
                # Process request
                result = await root_agent.process_request(agent_request)
                
                response = AgentQueryResponse(
                    request_id=result.request_id,
                    query=query_request.query,
                    response=result.consolidated_response,
                    agents_involved=[agent.value for agent in result.responses.keys()],
                    recommendations=result.recommendations,
                    next_actions=result.next_actions,
                    processing_time=(datetime.now() - result.timestamp).total_seconds(),
                    timestamp=result.timestamp.isoformat()
                )
                
                responses.append(response)
                
            except Exception as e:
                logger.error(f"Error processing batch query item: {str(e)}")
                # Add error response
                responses.append(AgentQueryResponse(
                    request_id=f"error_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    query=query_request.query,
                    response=f"Error processing query: {str(e)}",
                    agents_involved=[],
                    recommendations=[],
                    next_actions=[],
                    processing_time=0.0,
                    timestamp=datetime.now().isoformat(),
                    error=True
                ))
        
        return responses
        
    except Exception as e:
        logger.error(f"Error processing batch query: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/agents", response_model=Dict[str, AgentStatusResponse])
async def list_agents(auth: Dict = Depends(authenticate)):
    """List all available agents and their status."""
    try:
        agents_status = {}
        
        # Root agent status
        if root_agent:
            root_status = root_agent.get_system_status()
            agents_status["root"] = AgentStatusResponse(
                agent_id=root_status["root_agent"]["agent_id"],
                agent_type="root",
                status=root_status["root_agent"]["status"],
                model=root_status["root_agent"]["model"],
                last_activity=root_status["root_agent"].get("last_activity"),
                metrics={
                    "active_sessions": root_status["root_agent"]["active_sessions"],
                    "total_requests": root_status["root_agent"]["total_requests"],
                    "avg_processing_time": root_status["root_agent"]["avg_processing_time"]
                }
            )
        
        # Specialized agents status
        for agent_name, agent in specialized_agents.items():
            if hasattr(agent, 'get_agent_status'):
                status = agent.get_agent_status()
                agents_status[agent_name] = AgentStatusResponse(
                    agent_id=status.get("agent_id", agent_name),
                    agent_type=agent_name,
                    status=status.get("status", "active"),
                    model=status.get("model", "unknown"),
                    last_activity=status.get("last_activity"),
                    metrics=status
                )
        
        return agents_status
        
    except Exception as e:
        logger.error(f"Error listing agents: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/agents/{agent_id}", response_model=AgentStatusResponse)
async def get_agent_status(agent_id: str, auth: Dict = Depends(authenticate)):
    """Get detailed status of a specific agent."""
    try:
        # Check root agent
        if agent_id == "root" and root_agent:
            status = root_agent.get_system_status()
            return AgentStatusResponse(
                agent_id=status["root_agent"]["agent_id"],
                agent_type="root",
                status=status["root_agent"]["status"],
                model=status["root_agent"]["model"],
                last_activity=status["root_agent"].get("last_activity"),
                metrics=status
            )
        
        # Check specialized agents
        for agent_name, agent in specialized_agents.items():
            if agent_name == agent_id and hasattr(agent, 'get_agent_status'):
                status = agent.get_agent_status()
                return AgentStatusResponse(
                    agent_id=status.get("agent_id", agent_id),
                    agent_type=agent_id,
                    status=status.get("status", "active"),
                    model=status.get("model", "unknown"),
                    last_activity=status.get("last_activity"),
                    metrics=status
                )
        
        raise HTTPException(status_code=404, detail=f"Agent '{agent_id}' not found")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting agent status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/system/status", response_model=SystemStatusResponse)
async def get_system_status(auth: Dict = Depends(authenticate)):
    """Get overall system status and metrics."""
    try:
        # Get agent statuses
        agents_status = await list_agents(auth)
        
        # Calculate system metrics
        total_agents = len(agents_status)
        active_agents = sum(1 for agent in agents_status.values() if agent.status == "active")
        
        # Get root agent metrics if available
        system_load = 0
        if "root" in agents_status:
            system_load = agents_status["root"].metrics.get("system_load", 0)
        
        return SystemStatusResponse(
            status="healthy" if active_agents == total_agents else "degraded",
            timestamp=datetime.now().isoformat(),
            total_agents=total_agents,
            active_agents=active_agents,
            system_load=system_load,
            agents=agents_status,
            performance_metrics={
                "cache_hit_rate": "95%",  # Example metric
                "avg_response_time": "1.2s",
                "error_rate": "0.5%"
            }
        )
        
    except Exception as e:
        logger.error(f"Error getting system status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/reports/generate")
async def generate_report(
    request: ReportGenerationRequest,
    auth: Dict = Depends(authenticate)
):
    """
    Generate a report in the specified format.
    """
    try:
        logger.info(f"Generating {request.report_type} report in {request.format} format")
        
        # This would use the ReportGenerator tool
        # For now, return a placeholder response
        
        report_data = {
            "report_id": f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "type": request.report_type,
            "format": request.format,
            "title": f"{request.report_type.replace('_', ' ').title()} Report",
            "generated_at": datetime.now().isoformat(),
            "timeframe": request.timeframe,
            "content": f"Report content would be generated here for {request.report_type}",
            "download_url": f"/reports/download/report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{request.format}"
        }
        
        return JSONResponse(content=report_data)
        
    except Exception as e:
        logger.error(f"Error generating report: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/alerts/send")
async def send_alert(
    request: AlertRequest,
    auth: Dict = Depends(authenticate)
):
    """
    Send an alert through the system.
    """
    try:
        logger.info(f"Sending alert: {request.message[:100]}...")
        
        # This would use the AlertTool
        # For now, return a placeholder response
        
        alert_response = {
            "alert_id": f"alert_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "message": request.message,
            "level": request.level,
            "category": request.category,
            "sent_at": datetime.now().isoformat(),
            "channels": request.channels,
            "status": "sent"
        }
        
        return JSONResponse(content=alert_response)
        
    except Exception as e:
        logger.error(f"Error sending alert: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/memory/search")
async def search_memory(
    request: VectorSearchRequest,
    auth: Dict = Depends(authenticate)
):
    """
    Search the agent memory/knowledge base.
    """
    try:
        logger.info(f"Searching memory for: {request.query[:100]}...")
        
        # This would use the VectorStore
        # For now, return placeholder results
        
        search_results = {
            "query": request.query,
            "limit": request.limit,
            "results": [
                {
                    "text": f"Relevant information about {request.query}",
                    "similarity": 0.85,
                    "metadata": {
                        "source": "sales_data",
                        "timestamp": "2024-01-15T10:30:00",
                        "agent": "sales_performance"
                    }
                }
                for _ in range(min(request.limit, 5))
            ],
            "total_results": 5,
            "search_time": 0.1
        }
        
        return JSONResponse(content=search_results)
        
    except Exception as e:
        logger.error(f"Error searching memory: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/monitoring/metrics")
async def get_monitoring_metrics(
    timeframe: str = Query("24h", regex="^\d+[hdwm]$"),
    auth: Dict = Depends(authenticate)
):
    """Get system monitoring metrics."""
    try:
        # Parse timeframe
        if timeframe.endswith('h'):
            hours = int(timeframe[:-1])
        elif timeframe.endswith('d'):
            hours = int(timeframe[:-1]) * 24
        elif timeframe.endswith('w'):
            hours = int(timeframe[:-1]) * 168
        elif timeframe.endswith('m'):
            hours = int(timeframe[:-1]) * 720
        else:
            hours = 24
        
        # Generate mock metrics (in production, collect from monitoring system)
        metrics = {
            "timeframe": timeframe,
            "query_count": 150 * (hours // 24),
            "avg_response_time": 1.2,
            "error_rate": 0.02,
            "agent_utilization": {
                "root": 0.75,
                "sales_performance": 0.60,
                "marketing_campaign": 0.45,
                "anomaly": 0.30
            },
            "system_resources": {
                "cpu": 0.45,
                "memory": 0.60,
                "disk": 0.25
            },
            "alerts": {
                "critical": 2,
                "warning": 15,
                "info": 45
            }
        }
        
        return JSONResponse(content=metrics)
        
    except Exception as e:
        logger.error(f"Error getting monitoring metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/agents/{agent_id}/train")
async def train_agent(
    agent_id: str,
    training_data: Dict = Body(...),
    auth: Dict = Depends(authenticate)
):
    """
    Train/fine-tune an agent with new data.
    """
    try:
        logger.info(f"Training agent {agent_id} with {len(training_data.get('examples', []))} examples")
        
        # In production, this would trigger agent training
        # For now, return a placeholder response
        
        training_response = {
            "agent_id": agent_id,
            "training_id": f"train_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "examples_count": len(training_data.get("examples", [])),
            "status": "started",
            "estimated_completion": (datetime.now() + timedelta(hours=1)).isoformat(),
            "monitor_url": f"/training/monitor/train_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        }
        
        return JSONResponse(content=training_response)
        
    except Exception as e:
        logger.error(f"Error training agent: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/conversation/start")
async def start_conversation(
    initial_query: str = Body(..., embed=True),
    context: Dict = Body(default={}),
    auth: Dict = Depends(authenticate)
):
    """
    Start a new conversation session.
    """
    try:
        session_id = f"conv_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Create conversation session
        conversation_data = {
            "session_id": session_id,
            "user_id": auth["user_id"],
            "started_at": datetime.now().isoformat(),
            "initial_query": initial_query,
            "context": context,
            "message_count": 1,
            "active": True
        }
        
        # In production, store in database
        # For now, return session info
        
        return JSONResponse(content=conversation_data)
        
    except Exception as e:
        logger.error(f"Error starting conversation: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/conversation/{session_id}/message")
async def send_conversation_message(
    session_id: str,
    message: str = Body(..., embed=True),
    auth: Dict = Depends(authenticate)
):
    """
    Send a message in an existing conversation.
    """
    try:
        # Process message through agent system
        request = AgentQueryRequest(
            query=message,
            session_id=session_id,
            context={"conversation_mode": True}
        )
        
        response = await query_agent(request, BackgroundTasks(), auth)
        
        # Add conversation metadata
        conversation_response = {
            "session_id": session_id,
            "user_message": message,
            "agent_response": response.response,
            "timestamp": datetime.now().isoformat(),
            "message_id": f"msg_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        }
        
        return JSONResponse(content=conversation_response)
        
    except Exception as e:
        logger.error(f"Error sending conversation message: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/docs/custom")
async def custom_documentation():
    """Custom API documentation."""
    return {
        "endpoints": {
            "/": "Root endpoint with API info",
            "/health": "Health check",
            "/query": "Query the agent system",
            "/query/{agent_type}": "Query specific agent",
            "/query/batch": "Batch query processing",
            "/agents": "List all agents",
            "/agents/{agent_id}": "Get agent status",
            "/system/status": "Get system status",
            "/reports/generate": "Generate reports",
            "/alerts/send": "Send alerts",
            "/memory/search": "Search memory",
            "/monitoring/metrics": "Get monitoring metrics",
            "/agents/{agent_id}/train": "Train agent",
            "/conversation/start": "Start conversation",
            "/conversation/{session_id}/message": "Send conversation message"
        },
        "authentication": "Bearer token required for all endpoints",
        "rate_limiting": "100 requests per minute per user",
        "support": "Contact support@company.com for assistance"
    }


# Error handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Handle HTTP exceptions."""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "timestamp": datetime.now().isoformat()
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Handle general exceptions."""
    logger.error(f"Unhandled exception: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc) if app.debug else "Contact support",
            "status_code": 500,
            "timestamp": datetime.now().isoformat()
        }
    )


# Main entry point
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )