"""
Anomaly Detection Agent - Specialized agent for detecting anomalies and fraud.
Handles real-time anomaly detection, fraud prevention, and alerting.
"""

import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import asyncio

from langchain.agents import AgentExecutor, Tool
from langchain.agents.format_scratchpad import format_log_to_str
from langchain.agents.output_parsers import ReActSingleInputOutputParser
from langchain.prompts import PromptTemplate
from langchain.tools.render import render_text_description
from langchain.schema import AgentAction, AgentFinish
from langchain.chat_models import ChatOpenAI
from langchain.memory import ConversationBufferMemory
from langchain.callbacks.streaming_stdout import StreamingStdOutCallbackHandler

from tools.snowflake_query_tool import SnowflakeQueryTool
from tools.alert_tool import AlertTool
from memory.vector_store import VectorStore

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AnomalyType(Enum):
    """Types of anomalies that can be detected."""
    FRAUD = "fraud"
    REVENUE_ANOMALY = "revenue_anomaly"
    TRAFFIC_ANOMALY = "traffic_anomaly"
    INVENTORY_ANOMALY = "inventory_anomaly"
    CUSTOMER_BEHAVIOR_ANOMALY = "customer_behavior_anomaly"
    SYSTEM_ANOMALY = "system_anomaly"


@dataclass
class AnomalyDetection:
    """Anomaly detection result."""
    anomaly_id: str
    anomaly_type: AnomalyType
    severity: str  # "critical", "high", "medium", "low"
    confidence: float
    detected_at: datetime
    metric: str
    expected_value: float
    actual_value: float
    deviation_percent: float
    description: str
    recommendations: List[str]
    affected_entities: List[Dict]


@dataclass
class FraudPattern:
    """Fraud pattern detection."""
    pattern_id: str
    pattern_type: str
    frequency: int
    total_amount: float
    first_seen: datetime
    last_seen: datetime
    affected_customers: List[str]
    risk_score: float
    mitigation_strategies: List[str]


class AnomalyAgent:
    """
    AI Agent specialized in anomaly detection and fraud prevention.
    """
    
    def __init__(
        self,
        agent_id: str = "anomaly_agent_001",
        model_name: str = "gpt-4",
        temperature: float = 0.1,  # Lower temperature for more deterministic detection
        verbose: bool = False,
        alert_threshold: float = 0.8  # Confidence threshold for alerts
    ):
        """
        Initialize Anomaly Detection Agent.
        
        Args:
            agent_id: Unique identifier for the agent
            model_name: LLM model to use
            temperature: Model temperature (lower for more consistency)
            verbose: Enable verbose logging
            alert_threshold: Confidence threshold for sending alerts
        """
        self.agent_id = agent_id
        self.verbose = verbose
        self.alert_threshold = alert_threshold
        
        # Initialize LLM
        self.llm = ChatOpenAI(
            model_name=model_name,
            temperature=temperature,
            streaming=True,
            callbacks=[StreamingStdOutCallbackHandler()] if verbose else []
        )
        
        # Initialize tools
        self.query_tool = SnowflakeQueryTool()
        self.alert_tool = AlertTool()
        
        # Initialize memory
        self.memory = ConversationBufferMemory(
            memory_key="chat_history",
            return_messages=True
        )
        self.vector_store = VectorStore()
        
        # Initialize agent
        self.agent_executor = self._create_agent()
        
        # Agent state
        self.detected_anomalies = []
        self.active_alerts = []
        self.fraud_patterns = []
        self.last_detection_time = None
        
        # Detection rules and thresholds
        self.detection_rules = self._initialize_detection_rules()
        
        logger.info(f"Anomaly Agent {agent_id} initialized")
    
    def _create_agent(self) -> AgentExecutor:
        """Create the anomaly detection agent with specialized tools."""
        
        # Define agent tools
        tools = [
            Tool(
                name="query_anomaly_data",
                func=self.query_tool.execute_query,
                description="Query data for anomaly detection from Snowflake. Use for fraud detection, revenue anomalies, etc."
            ),
            Tool(
                name="send_alert",
                func=self.alert_tool.send_alert,
                description="Send alerts for detected anomalies and fraud patterns."
            ),
            Tool(
                name="detect_revenue_anomalies",
                func=self.detect_revenue_anomalies,
                description="Detect revenue anomalies and unusual patterns."
            ),
            Tool(
                name="detect_fraud_patterns",
                func=self.detect_fraud_patterns,
                description="Detect fraud patterns in transactions."
            ),
            Tool(
                name="monitor_system_health",
                func=self.monitor_system_health,
                description="Monitor system health and detect operational anomalies."
            ),
            Tool(
                name="analyze_customer_behavior",
                func=self.analyze_customer_behavior,
                description="Analyze customer behavior for anomalies."
            ),
            Tool(
                name="get_historical_anomalies",
                func=self.get_historical_anomalies,
                description="Get historical anomalies for pattern analysis."
            ),
            Tool(
                name="update_detection_rules",
                func=self.update_detection_rules,
                description="Update anomaly detection rules based on new patterns."
            )
        ]
        
        # Create agent prompt template
        prompt_template = """
        You are an Anomaly Detection AI Agent specialized in detecting fraud, anomalies, and unusual patterns.
        Your role is to proactively monitor systems, detect anomalies, prevent fraud, and alert relevant stakeholders.
        
        Specializations:
        - Real-time fraud detection and prevention
        - Revenue and financial anomaly detection
        - System health and operational anomaly monitoring
        - Customer behavior anomaly analysis
        - Pattern recognition and trend analysis
        
        Detection Priorities:
        1. Financial fraud and revenue leakage
        2. System outages and performance issues
        3. Customer account compromise
        4. Inventory and supply chain anomalies
        5. Data quality and integrity issues
        
        Response Protocol:
        1. Immediate alert for critical anomalies (confidence > 0.9)
        2. Detailed analysis with evidence for medium-high anomalies
        3. Pattern recognition and trend analysis
        4. Mitigation and prevention recommendations
        5. Follow-up monitoring instructions
        
        Current Date: {current_date}
        
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
            "current_date": lambda x: datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        prompt = PromptTemplate(
            template=prompt_template,
            input_variables=["input", "chat_history", "current_date"],
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
            max_iterations=8,
            early_stopping_method="generate"
        )
    
    def _initialize_detection_rules(self) -> Dict:
        """Initialize anomaly detection rules and thresholds."""
        return {
            "revenue_anomaly": {
                "threshold": 0.3,  # 30% deviation from expected
                "time_window": "24h",
                "confidence_threshold": 0.8,
                "metrics": ["total_sales", "transaction_count", "average_order_value"]
            },
            "fraud_detection": {
                "threshold": 0.7,
                "time_window": "1h",
                "confidence_threshold": 0.85,
                "patterns": ["velocity", "geolocation", "device_fingerprint", "behavioral"]
            },
            "system_health": {
                "threshold": 0.2,
                "time_window": "15m",
                "confidence_threshold": 0.9,
                "metrics": ["error_rate", "response_time", "throughput", "availability"]
            },
            "customer_behavior": {
                "threshold": 0.4,
                "time_window": "7d",
                "confidence_threshold": 0.75,
                "patterns": ["purchase_frequency", "basket_size", "category_preference", "time_of_day"]
            },
            "inventory_anomaly": {
                "threshold": 0.5,
                "time_window": "24h",
                "confidence_threshold": 0.8,
                "metrics": ["stock_level", "sales_velocity", "out_of_stock_rate"]
            }
        }
    
    async def process_query(self, query: str, context: Dict = None) -> Dict:
        """
        Process an anomaly detection query.
        
        Args:
            query: User query or detection request
            context: Additional context for the query
            
        Returns:
            Dictionary with agent response and detections
        """
        try:
            logger.info(f"Processing anomaly query: {query}")
            
            # Add context to query if provided
            if context:
                query_with_context = f"{query}\n\nContext: {json.dumps(context, indent=2)}"
            else:
                query_with_context = query
            
            # Execute agent
            response = await self.agent_executor.ainvoke({
                "input": query_with_context,
                "chat_history": self.memory.load_memory_variables({}).get("chat_history", "")
            })
            
            # Extract detections from response
            detections = await self._extract_detections(response["output"])
            
            # Store in memory
            await self._update_agent_memory(query, response["output"], detections)
            
            # Send alerts for high-confidence detections
            await self._process_detections(detections)
            
            return {
                "agent_id": self.agent_id,
                "query": query,
                "response": response["output"],
                "detections": [d.__dict__ for d in detections],
                "alerts_sent": len(self.active_alerts[-10:]),
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error processing query: {str(e)}")
            return {
                "agent_id": self.agent_id,
                "error": str(e),
                "query": query,
                "response": "I encountered an error processing your query. Please try again.",
                "timestamp": datetime.now().isoformat()
            }
    
    async def _extract_detections(self, response: str) -> List[AnomalyDetection]:
        """
        Extract structured anomaly detections from agent response.
        
        Args:
            response: Agent response text
            
        Returns:
            List of AnomalyDetection objects
        """
        detections = []
        
        # Use LLM to extract structured detections
        detection_extraction_prompt = f"""
        Extract structured anomaly detections from the following text.
        Return as JSON array with fields: anomaly_type, severity, confidence, metric, expected_value, actual_value, deviation_percent, description, recommendations, affected_entities.
        
        Text: {response}
        
        JSON:
        """
        
        try:
            detection_response = await self.llm.agenerate([detection_extraction_prompt])
            detection_json = json.loads(detection_response.generations[0][0].text)
            
            for detection_data in detection_json:
                detection = AnomalyDetection(
                    anomaly_id=f"anomaly_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{len(detections)}",
                    anomaly_type=AnomalyType(detection_data.get("anomaly_type", "unknown").lower()),
                    severity=detection_data.get("severity", "medium"),
                    confidence=float(detection_data.get("confidence", 0.5)),
                    detected_at=datetime.now(),
                    metric=detection_data.get("metric", ""),
                    expected_value=float(detection_data.get("expected_value", 0)),
                    actual_value=float(detection_data.get("actual_value", 0)),
                    deviation_percent=float(detection_data.get("deviation_percent", 0)),
                    description=detection_data.get("description", ""),
                    recommendations=detection_data.get("recommendations", []),
                    affected_entities=detection_data.get("affected_entities", [])
                )
                detections.append(detection)
                
        except Exception as e:
            logger.warning(f"Failed to extract detections: {str(e)}")
        
        return detections
    
    async def _update_agent_memory(self, query: str, response: str, detections: List[AnomalyDetection]):
        """Update agent memory with query, response, and detections."""
        # Store in conversation memory
        self.memory.save_context({"input": query}, {"output": response})
        
        # Store detections in vector store
        for detection in detections:
            detection_text = f"Anomaly: {detection.anomaly_type.value} - {detection.description}"
            await self.vector_store.store_document(
                text=detection_text,
                metadata={
                    "agent_id": self.agent_id,
                    "anomaly_id": detection.anomaly_id,
                    "anomaly_type": detection.anomaly_type.value,
                    "severity": detection.severity,
                    "confidence": detection.confidence,
                    "metric": detection.metric,
                    "deviation_percent": detection.deviation_percent,
                    "timestamp": detection.detected_at.isoformat()
                }
            )
    
    async def _process_detections(self, detections: List[AnomalyDetection]):
        """Process detections and send alerts for high-confidence ones."""
        for detection in detections:
            # Check if detection meets alert threshold
            if detection.confidence >= self.alert_threshold:
                # Send alert
                alert_message = self._create_alert_message(detection)
                
                # Determine alert level based on severity
                if detection.severity == "critical":
                    alert_level = "critical"
                elif detection.severity == "high":
                    alert_level = "warning"
                else:
                    alert_level = "info"
                
                await self.alert_tool.send_alert(
                    message=alert_message,
                    level=alert_level,
                    category=f"anomaly_{detection.anomaly_type.value}"
                )
                
                # Store alert
                self.active_alerts.append({
                    "anomaly_id": detection.anomaly_id,
                    "alert_message": alert_message,
                    "level": alert_level,
                    "timestamp": datetime.now().isoformat(),
                    "detection": detection.__dict__
                })
                
                logger.info(f"Alert sent for anomaly: {detection.anomaly_id}")
    
    def _create_alert_message(self, detection: AnomalyDetection) -> str:
        """Create alert message from anomaly detection."""
        return f"""
        ⚠️ ANOMALY DETECTED ⚠️
        
        Type: {detection.anomaly_type.value.upper()}
        Severity: {detection.severity.upper()}
        Confidence: {detection.confidence:.1%}
        
        Metric: {detection.metric}
        Expected: {detection.expected_value}
        Actual: {detection.actual_value}
        Deviation: {detection.deviation_percent:.1%}
        
        Description: {detection.description}
        
        Recommendations:
        {chr(10).join(f'• {rec}' for rec in detection.recommendations)}
        
        Detected at: {detection.detected_at.strftime('%Y-%m-%d %H:%M:%S')}
        Anomaly ID: {detection.anomaly_id}
        """
    
    # Anomaly detection tools
    
    async def detect_revenue_anomalies(
        self,
        timeframe: str = "24h",
        confidence_threshold: float = 0.8
    ) -> List[AnomalyDetection]:
        """
        Detect revenue anomalies for the given timeframe.
        
        Args:
            timeframe: Timeframe for analysis
            confidence_threshold: Confidence threshold for detection
            
        Returns:
            List of detected anomalies
        """
        logger.info(f"Detecting revenue anomalies for timeframe: {timeframe}")
        
        # Query revenue data
        if timeframe.endswith('h'):
            hours = int(timeframe[:-1])
            interval = f"INTERVAL '{hours} HOUR'"
        elif timeframe.endswith('d'):
            days = int(timeframe[:-1])
            interval = f"INTERVAL '{days} DAY'"
        else:
            interval = "INTERVAL '24 HOUR'"
        
        query = f"""
        WITH hourly_revenue AS (
            SELECT 
                DATE_TRUNC('hour', transaction_timestamp) as hour,
                SUM(line_final_amount) as hourly_revenue,
                COUNT(DISTINCT transaction_id) as transaction_count,
                AVG(line_final_amount) as avg_order_value
            FROM ANALYTICS.FACT_SALES
            WHERE transaction_timestamp >= CURRENT_TIMESTAMP() - {interval}
            GROUP BY DATE_TRUNC('hour', transaction_timestamp)
            ORDER BY hour
        ),
        stats AS (
            SELECT 
                AVG(hourly_revenue) as avg_revenue,
                STDDEV(hourly_revenue) as std_revenue,
                AVG(transaction_count) as avg_transactions,
                STDDEV(transaction_count) as std_transactions
            FROM hourly_revenue
        )
        SELECT 
            hr.hour,
            hr.hourly_revenue,
            hr.transaction_count,
            hr.avg_order_value,
            s.avg_revenue,
            s.std_revenue,
            (hr.hourly_revenue - s.avg_revenue) / NULLIF(s.std_revenue, 0) as revenue_zscore,
            s.avg_transactions,
            s.std_transactions,
            (hr.transaction_count - s.avg_transactions) / NULLIF(s.std_transactions, 0) as transaction_zscore
        FROM hourly_revenue hr
        CROSS JOIN stats s
        WHERE ABS((hr.hourly_revenue - s.avg_revenue) / NULLIF(s.std_revenue, 0)) > 3
           OR ABS((hr.transaction_count - s.avg_transactions) / NULLIF(s.std_transactions, 0)) > 3
        ORDER BY hour DESC
        """
        
        try:
            results = await self.query_tool.execute_query(query)
            anomalies = []
            
            for row in results:
                # Determine anomaly type based on z-score
                if abs(row['revenue_zscore']) > 3:
                    anomaly_type = AnomalyType.REVENUE_ANOMALY
                    metric = "hourly_revenue"
                    expected = row['avg_revenue']
                    actual = row['hourly_revenue']
                    deviation = ((actual - expected) / expected * 100) if expected > 0 else 0
                    
                    # Calculate confidence based on z-score
                    confidence = min(0.99, abs(row['revenue_zscore']) / 5)
                    
                    # Determine severity
                    if abs(row['revenue_zscore']) > 4:
                        severity = "critical"
                    elif abs(row['revenue_zscore']) > 3:
                        severity = "high"
                    else:
                        severity = "medium"
                    
                    anomaly = AnomalyDetection(
                        anomaly_id=f"revenue_anomaly_{row['hour'].strftime('%Y%m%d_%H')}",
                        anomaly_type=anomaly_type,
                        severity=severity,
                        confidence=confidence,
                        detected_at=datetime.now(),
                        metric=metric,
                        expected_value=expected,
                        actual_value=actual,
                        deviation_percent=deviation,
                        description=f"Revenue anomaly detected at {row['hour']}: {actual:.2f} vs expected {expected:.2f} (z-score: {row['revenue_zscore']:.2f})",
                        recommendations=[
                            "Investigate cause of revenue spike/drop",
                            "Check for system issues or promotions",
                            "Monitor subsequent hours for patterns",
                            "Review transaction logs for anomalies"
                        ],
                        affected_entities=[{"type": "time_period", "value": row['hour'].isoformat()}]
                    )
                    anomalies.append(anomaly)
            
            return anomalies
            
        except Exception as e:
            logger.error(f"Error detecting revenue anomalies: {str(e)}")
            return []
    
    async def detect_fraud_patterns(
        self,
        timeframe: str = "1h",
        min_confidence: float = 0.85
    ) -> List[FraudPattern]:
        """
        Detect fraud patterns in transactions.
        
        Args:
            timeframe: Timeframe for analysis
            min_confidence: Minimum confidence threshold
            
        Returns:
            List of detected fraud patterns
        """
        logger.info(f"Detecting fraud patterns for timeframe: {timeframe}")
        
        # Query fraud data
        query = f"""
        WITH suspicious_transactions AS (
            SELECT 
                transaction_id,
                customer_id,
                transaction_timestamp,
                total_amount,
                payment_method,
                ip_address,
                device_type,
                fraud_score,
                fraud_detected
            FROM STAGING.TRANSACTIONS
            WHERE transaction_timestamp >= DATEADD(hour, -{timeframe.replace('h', '')}, CURRENT_TIMESTAMP())
            AND fraud_score > 70
            ORDER BY transaction_timestamp DESC
        ),
        velocity_check AS (
            SELECT 
                customer_id,
                COUNT(*) as transaction_count,
                SUM(total_amount) as total_amount,
                MIN(transaction_timestamp) as first_transaction,
                MAX(transaction_timestamp) as last_transaction
            FROM suspicious_transactions
            GROUP BY customer_id
            HAVING COUNT(*) > 3  -- Velocity threshold
        ),
        geolocation_check AS (
            SELECT 
                ip_address,
                COUNT(DISTINCT customer_id) as unique_customers,
                COUNT(*) as transaction_count
            FROM suspicious_transactions
            GROUP BY ip_address
            HAVING COUNT(DISTINCT customer_id) > 1  # Same IP, different customers
        )
        SELECT 
            'velocity' as pattern_type,
            vc.customer_id,
            vc.transaction_count,
            vc.total_amount,
            vc.first_transaction,
            vc.last_transaction,
            (vc.transaction_count / NULLIF(EXTRACT(EPOCH FROM (vc.last_transaction - vc.first_transaction)) / 3600, 0)) as transactions_per_hour
        FROM velocity_check vc
        UNION ALL
        SELECT 
            'geolocation' as pattern_type,
            NULL as customer_id,
            gc.transaction_count,
            NULL as total_amount,
            NULL as first_transaction,
            NULL as last_transaction,
            gc.unique_customers as transactions_per_hour
        FROM geolocation_check gc
        """
        
        try:
            results = await self.query_tool.execute_query(query)
            patterns = []
            
            for row in results:
                pattern_type = row['pattern_type']
                
                # Calculate risk score
                if pattern_type == 'velocity':
                    risk_score = min(1.0, row['transactions_per_hour'] / 10)  # Scale based on transactions per hour
                    affected_customers = [row['customer_id']] if row['customer_id'] else []
                else:
                    risk_score = min(1.0, row['unique_customers'] / 5)  # Scale based on unique customers per IP
                    affected_customers = []
                
                if risk_score >= min_confidence:
                    pattern = FraudPattern(
                        pattern_id=f"fraud_pattern_{pattern_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                        pattern_type=pattern_type,
                        frequency=row['transaction_count'],
                        total_amount=row['total_amount'] or 0,
                        first_seen=row['first_transaction'] or datetime.now(),
                        last_seen=row['last_transaction'] or datetime.now(),
                        affected_customers=affected_customers,
                        risk_score=risk_score,
                        mitigation_strategies=self._get_fraud_mitigation_strategies(pattern_type)
                    )
                    patterns.append(pattern)
            
            return patterns
            
        except Exception as e:
            logger.error(f"Error detecting fraud patterns: {str(e)}")
            return []
    
    def _get_fraud_mitigation_strategies(self, pattern_type: str) -> List[str]:
        """Get mitigation strategies for fraud patterns."""
        strategies = {
            "velocity": [
                "Implement velocity limits per customer",
                "Require additional authentication for high-frequency transactions",
                "Review transaction history for pattern consistency",
                "Temporarily limit account functionality"
            ],
            "geolocation": [
                "Flag transactions from suspicious IP addresses",
                "Implement IP reputation checking",
                "Require additional verification for IP changes",
                "Monitor for proxy/VPN usage"
            ],
            "device_fingerprint": [
                "Track device fingerprints across transactions",
                "Flag transactions from new/unrecognized devices",
                "Implement device reputation scoring",
                "Require device verification for new devices"
            ],
            "behavioral": [
                "Establish baseline behavior for each customer",
                "Flag deviations from normal behavior patterns",
                "Implement behavioral biometrics",
                "Use machine learning for pattern recognition"
            ]
        }
        return strategies.get(pattern_type, [
            "Review transaction details",
            "Contact customer for verification",
            "Monitor for additional suspicious activity",
            "Update fraud detection rules"
        ])
    
    async def monitor_system_health(self) -> Dict:
        """
        Monitor system health and detect operational anomalies.
        
        Returns:
            System health status and anomalies
        """
        logger.info("Monitoring system health")
        
        # Query system metrics (adjust based on available metrics)
        query = """
        WITH system_metrics AS (
            -- Error rates
            SELECT 
                'error_rate' as metric_type,
                DATE_TRUNC('hour', error_timestamp) as time_bucket,
                COUNT(*) as error_count,
                COUNT(DISTINCT session_id) as affected_sessions
            FROM SYSTEM.ERROR_LOGS  -- Adjust table name as needed
            WHERE error_timestamp >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
            GROUP BY DATE_TRUNC('hour', error_timestamp)
            
            UNION ALL
            
            -- Response times
            SELECT 
                'response_time' as metric_type,
                DATE_TRUNC('hour', request_timestamp) as time_bucket,
                AVG(response_time_ms) as avg_value,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_time_ms) as p95_value
            FROM SYSTEM.REQUEST_LOGS  -- Adjust table name as needed
            WHERE request_timestamp >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
            AND response_time_ms IS NOT NULL
            GROUP BY DATE_TRUNC('hour', request_timestamp)
            
            UNION ALL
            
            -- Throughput
            SELECT 
                'throughput' as metric_type,
                DATE_TRUNC('hour', request_timestamp) as time_bucket,
                COUNT(*) as request_count,
                COUNT(DISTINCT user_id) as unique_users
            FROM SYSTEM.REQUEST_LOGS
            WHERE request_timestamp >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
            GROUP BY DATE_TRUNC('hour', request_timestamp)
        )
        SELECT 
            metric_type,
            time_bucket,
            error_count,
            affected_sessions,
            avg_value,
            p95_value,
            request_count,
            unique_users
        FROM system_metrics
        ORDER BY metric_type, time_bucket DESC
        """
        
        try:
            results = await self.query_tool.execute_query(query)
            
            # Analyze system health
            health_status = {
                "metrics": results,
                "anomalies": [],
                "overall_status": "healthy",
                "last_check": datetime.now().isoformat()
            }
            
            # Check for anomalies in error rates
            error_metrics = [r for r in results if r['metric_type'] == 'error_rate']
            if error_metrics:
                recent_errors = error_metrics[0]['error_count'] if error_metrics else 0
                if recent_errors > 100:  # Threshold
                    health_status["anomalies"].append({
                        "type": "error_rate_high",
                        "metric": "error_count",
                        "value": recent_errors,
                        "threshold": 100,
                        "severity": "high"
                    })
                    health_status["overall_status"] = "degraded"
            
            # Check for anomalies in response times
            response_metrics = [r for r in results if r['metric_type'] == 'response_time']
            if response_metrics:
                recent_p95 = response_metrics[0]['p95_value'] if response_metrics else 0
                if recent_p95 > 2000:  # 2 seconds threshold
                    health_status["anomalies"].append({
                        "type": "response_time_high",
                        "metric": "p95_response_time",
                        "value": recent_p95,
                        "threshold": 2000,
                        "severity": "medium"
                    })
                    if health_status["overall_status"] == "healthy":
                        health_status["overall_status"] = "warning"
            
            return health_status
            
        except Exception as e:
            logger.error(f"Error monitoring system health: {str(e)}")
            return {
                "error": str(e),
                "overall_status": "unknown",
                "last_check": datetime.now().isoformat()
            }
    
    async def analyze_customer_behavior(self, customer_id: str = None) -> Dict:
        """
        Analyze customer behavior for anomalies.
        
        Args:
            customer_id: Optional specific customer ID
            
        Returns:
            Customer behavior analysis
        """
        logger.info(f"Analyzing customer behavior for: {customer_id or 'all customers'}")
        
        customer_filter = f"AND customer_id = '{customer_id}'" if customer_id else ""
        
        query = f"""
        WITH customer_behavior AS (
            SELECT 
                customer_id,
                DATE(transaction_timestamp) as transaction_date,
                COUNT(DISTINCT transaction_id) as daily_transactions,
                SUM(total_amount) as daily_spend,
                AVG(total_amount) as avg_transaction_value,
                COUNT(DISTINCT product_category) as unique_categories
            FROM STAGING.TRANSACTIONS t
            JOIN STAGING.TRANSACTION_ITEMS ti ON t.transaction_id = ti.transaction_id
            WHERE transaction_timestamp >= DATEADD(day, -30, CURRENT_DATE())
            {customer_filter}
            GROUP BY customer_id, DATE(transaction_timestamp)
        ),
        behavior_stats AS (
            SELECT 
                customer_id,
                AVG(daily_transactions) as avg_daily_transactions,
                STDDEV(daily_transactions) as std_daily_transactions,
                AVG(daily_spend) as avg_daily_spend,
                STDDEV(daily_spend) as std_daily_spend,
                AVG(avg_transaction_value) as avg_transaction_value,
                STDDEV(avg_transaction_value) as std_transaction_value
            FROM customer_behavior
            GROUP BY customer_id
        )
        SELECT 
            cb.customer_id,
            cb.transaction_date,
            cb.daily_transactions,
            cb.daily_spend,
            cb.avg_transaction_value,
            cb.unique_categories,
            bs.avg_daily_transactions,
            bs.std_daily_transactions,
            bs.avg_daily_spend,
            bs.std_daily_spend,
            (cb.daily_transactions - bs.avg_daily_transactions) / NULLIF(bs.std_daily_transactions, 0) as transactions_zscore,
            (cb.daily_spend - bs.avg_daily_spend) / NULLIF(bs.std_daily_spend, 0) as spend_zscore
        FROM customer_behavior cb
        JOIN behavior_stats bs ON cb.customer_id = bs.customer_id
        WHERE ABS((cb.daily_transactions - bs.avg_daily_transactions) / NULLIF(bs.std_daily_transactions, 0)) > 3
           OR ABS((cb.daily_spend - bs.avg_daily_spend) / NULLIF(bs.std_daily_spend, 0)) > 3
        ORDER BY cb.transaction_date DESC
        """
        
        try:
            results = await self.query_tool.execute_query(query)
            
            anomalies = []
            for row in results:
                if abs(row['transactions_zscore']) > 3 or abs(row['spend_zscore']) > 3:
                    anomaly_type = AnomalyType.CUSTOMER_BEHAVIOR_ANOMALY
                    
                    if abs(row['transactions_zscore']) > 3:
                        metric = "daily_transactions"
                        deviation = row['transactions_zscore']
                    else:
                        metric = "daily_spend"
                        deviation = row['spend_zscore']
                    
                    anomaly = {
                        "customer_id": row['customer_id'],
                        "date": row['transaction_date'].isoformat(),
                        "metric": metric,
                        "value": row[metric],
                        "expected": row[f'avg_{metric}'],
                        "z_score": deviation,
                        "severity": "high" if abs(deviation) > 4 else "medium",
                        "description": f"Customer behavior anomaly: {metric} = {row[metric]:.2f} (z-score: {deviation:.2f})"
                    }
                    anomalies.append(anomaly)
            
            return {
                "analysis_date": datetime.now().isoformat(),
                "customer_id": customer_id,
                "anomalies_detected": len(anomalies),
                "anomalies": anomalies,
                "timeframe": "30 days"
            }
            
        except Exception as e:
            logger.error(f"Error analyzing customer behavior: {str(e)}")
            return {"error": str(e)}
    
    async def get_historical_anomalies(
        self,
        days: int = 7,
        anomaly_type: str = None
    ) -> List[Dict]:
        """
        Get historical anomalies for pattern analysis.
        
        Args:
            days: Number of days to look back
            anomaly_type: Optional filter by anomaly type
            
        Returns:
            List of historical anomalies
        """
        logger.info(f"Getting historical anomalies for last {days} days")
        
        # This would query from the anomaly log/store
        # For now, return stored detections
        
        cutoff_time = datetime.now() - timedelta(days=days)
        historical = []
        
        for detection in self.detected_anomalies:
            if detection.detected_at >= cutoff_time:
                if not anomaly_type or detection.anomaly_type.value == anomaly_type:
                    historical.append({
                        "anomaly_id": detection.anomaly_id,
                        "type": detection.anomaly_type.value,
                        "severity": detection.severity,
                        "confidence": detection.confidence,
                        "detected_at": detection.detected_at.isoformat(),
                        "metric": detection.metric,
                        "deviation": detection.deviation_percent,
                        "description": detection.description
                    })
        
        return sorted(historical, key=lambda x: x["detected_at"], reverse=True)
    
    async def update_detection_rules(self, new_rules: Dict) -> bool:
        """
        Update anomaly detection rules.
        
        Args:
            new_rules: New detection rules
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Validate new rules
            for rule_type, rule_config in new_rules.items():
                if rule_type in self.detection_rules:
                    self.detection_rules[rule_type].update(rule_config)
                else:
                    self.detection_rules[rule_type] = rule_config
            
            logger.info(f"Updated detection rules: {list(new_rules.keys())}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating detection rules: {str(e)}")
            return False
    
    async def continuous_monitoring(self, interval_minutes: int = 5):
        """
        Run continuous anomaly monitoring.
        
        Args:
            interval_minutes: Monitoring interval in minutes
        """
        logger.info(f"Starting continuous monitoring with {interval_minutes} minute interval")
        
        while True:
            try:
                # Run detection checks
                revenue_anomalies = await self.detect_revenue_anomalies(timeframe="1h")
                fraud_patterns = await self.detect_fraud_patterns(timeframe="1h")
                system_health = await self.monitor_system_health()
                
                # Process detections
                all_detections = revenue_anomalies
                
                for detection in all_detections:
                    self.detected_anomalies.append(detection)
                
                # Send summary if anomalies detected
                if all_detections:
                    summary = f"""
                    📊 Anomaly Monitoring Summary
                    Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    Period: Last hour
                    
                    Detected:
                    - Revenue anomalies: {len(revenue_anomalies)}
                    - Fraud patterns: {len(fraud_patterns)}
                    - System health: {system_health.get('overall_status', 'unknown')}
                    
                    Total alerts sent: {len(self.active_alerts[-10:])}
                    """
                    
                    await self.alert_tool.send_alert(
                        message=summary,
                        level="info",
                        category="monitoring_summary"
                    )
                
                # Clean up old alerts
                self.active_alerts = [
                    alert for alert in self.active_alerts
                    if datetime.fromisoformat(alert['timestamp']) > datetime.now() - timedelta(hours=24)
                ]
                
                # Wait for next interval
                await asyncio.sleep(interval_minutes * 60)
                
            except Exception as e:
                logger.error(f"Error in continuous monitoring: {str(e)}")
                await asyncio.sleep(60)  # Wait a minute before retrying
    
    def get_agent_status(self) -> Dict:
        """
        Get current agent status and statistics.
        
        Returns:
            Agent status information
        """
        return {
            "agent_id": self.agent_id,
            "status": "active",
            "model": self.llm.model_name,
            "temperature": self.llm.temperature,
            "detected_anomalies": len(self.detected_anomalies),
            "active_alerts": len(self.active_alerts),
            "fraud_patterns": len(self.fraud_patterns),
            "last_detection": self.detected_anomalies[-1].detected_at.isoformat() if self.detected_anomalies else None,
            "monitoring_active": hasattr(self, '_monitoring_task') and not self._monitoring_task.done(),
            "detection_rules": list(self.detection_rules.keys()),
            "alert_threshold": self.alert_threshold
        }