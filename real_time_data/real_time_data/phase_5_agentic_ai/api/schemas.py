"""
API Schemas - Pydantic models for API request/response validation.
"""

from typing import Dict, List, Any, Optional, Union
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field, validator


# Enums
class AgentType(str, Enum):
    """Agent types."""
    ROOT = "root"
    SALES = "sales"
    MARKETING = "marketing"
    ANOMALY = "anomaly"
    CUSTOMER_SERVICE = "customer_service"
    INVENTORY = "inventory"


class ReportFormat(str, Enum):
    """Report formats."""
    PDF = "pdf"
    EXCEL = "excel"
    HTML = "html"
    CSV = "csv"
    JSON = "json"
    MARKDOWN = "markdown"


class ReportType(str, Enum):
    """Report types."""
    SALES_PERFORMANCE = "sales_performance"
    CUSTOMER_ANALYSIS = "customer_analysis"
    MARKETING_CAMPAIGN = "marketing_campaign"
    ANOMALY_DETECTION = "anomaly_detection"
    EXECUTIVE_SUMMARY = "executive_summary"
    DAILY_DASHBOARD = "daily_dashboard"
    CUSTOM = "custom"


class AlertLevel(str, Enum):
    """Alert levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertChannel(str, Enum):
    """Alert channels."""
    EMAIL = "email"
    SLACK = "slack"
    WEBHOOK = "webhook"
    IN_APP = "in_app"
    LOG = "log"
    ALL = "all"


class Priority(str, Enum):
    """Request priorities."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


# Request Models
class AgentQueryRequest(BaseModel):
    """Request to query an agent."""
    query: str = Field(..., description="Natural language query")
    context: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional context")
    session_id: Optional[str] = Field(None, description="Session ID for conversation context")
    priority: Optional[Priority] = Field(Priority.MEDIUM, description="Request priority")
    stream: bool = Field(False, description="Whether to stream the response")


class BatchQueryRequest(BaseModel):
    """Request for batch query processing."""
    queries: List[AgentQueryRequest] = Field(..., description="List of queries")


class ReportGenerationRequest(BaseModel):
    """Request to generate a report."""
    report_type: ReportType = Field(..., description="Type of report to generate")
    timeframe: str = Field("30d", description="Timeframe (e.g., 7d, 30d, QTD, YTD)")
    filters: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Report filters")
    format: ReportFormat = Field(ReportFormat.PDF, description="Output format")
    recipient_emails: Optional[List[str]] = Field(None, description="Email recipients")


class AlertRequest(BaseModel):
    """Request to send an alert."""
    message: str = Field(..., description="Alert message")
    level: AlertLevel = Field(AlertLevel.INFO, description="Alert level")
    category: str = Field("general", description="Alert category")
    channels: List[AlertChannel] = Field([AlertChannel.LOG], description="Channels to send alert through")


class VectorSearchRequest(BaseModel):
    """Request to search memory."""
    query: str = Field(..., description="Search query")
    limit: int = Field(5, description="Maximum number of results")
    filters: Optional[Dict[str, Any]] = Field(None, description="Metadata filters")


class TrainingExample(BaseModel):
    """Example for agent training."""
    input: str
    output: str
    context: Optional[Dict] = None


class TrainingRequest(BaseModel):
    """Request to train an agent."""
    examples: List[TrainingExample]
    epochs: int = 1
    learning_rate: float = 0.001


# Response Models
class Recommendation(BaseModel):
    """Actionable recommendation."""
    agent: str
    recommendation: str
    impact: Optional[str] = None
    priority: str = "medium"
    timeline: Optional[str] = None


class NextAction(BaseModel):
    """Suggested next action."""
    type: str
    description: str
    priority: str
    suggested_by: str


class AgentQueryResponse(BaseModel):
    """Response from agent query."""
    request_id: str
    query: str
    response: str
    agents_involved: List[str]
    recommendations: List[Dict[str, Any]] = []
    next_actions: List[Dict[str, Any]] = []
    processing_time: float
    timestamp: str
    error: bool = False


class AgentStatusResponse(BaseModel):
    """Agent status information."""
    agent_id: str
    agent_type: str
    status: str
    model: str
    last_activity: Optional[str] = None
    metrics: Dict[str, Any] = {}


class SystemStatusResponse(BaseModel):
    """Overall system status."""
    status: str
    timestamp: str
    total_agents: int
    active_agents: int
    system_load: int
    agents: Dict[str, AgentStatusResponse]
    performance_metrics: Dict[str, Any]


class HealthCheckResponse(BaseModel):
    """Health check response."""
    status: str
    timestamp: str
    agents_available: int
    system_load: int
    details: Dict[str, Any]