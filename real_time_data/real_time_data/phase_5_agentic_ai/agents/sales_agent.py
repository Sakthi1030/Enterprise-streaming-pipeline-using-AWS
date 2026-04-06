"""
Sales Agent - Specialized agent for sales analysis and recommendations.
Handles sales performance, customer insights, and revenue optimization.
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
from tools.report_generator import ReportGenerator
from memory.vector_store import VectorStore

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SalesAgentRole(Enum):
    """Sales agent specialized roles."""
    PERFORMANCE_ANALYST = "performance_analyst"
    CUSTOMER_INSIGHTS = "customer_insights"
    REVENUE_OPTIMIZATION = "revenue_optimization"
    FORECASTING = "forecasting"


@dataclass
class SalesInsight:
    """Sales insight data structure."""
    insight_type: str
    metric: str
    value: float
    trend: str  # "up", "down", "stable"
    confidence: float
    recommendation: str
    impact_score: float
    timeframe: str


@dataclass
class CustomerSegment:
    """Customer segment data structure."""
    segment_id: str
    segment_name: str
    customer_count: int
    avg_lifetime_value: float
    purchase_frequency: float
    churn_risk: float
    top_categories: List[str]
    recommendation: str


class SalesAgent:
    """
    AI Agent specialized in sales analysis, insights, and recommendations.
    """
    
    def __init__(
        self,
        agent_id: str = "sales_agent_001",
        role: SalesAgentRole = SalesAgentRole.PERFORMANCE_ANALYST,
        model_name: str = "gpt-4",
        temperature: float = 0.3,
        verbose: bool = False
    ):
        """
        Initialize Sales Agent.
        
        Args:
            agent_id: Unique identifier for the agent
            role: Specialized role of the agent
            model_name: LLM model to use
            temperature: Model temperature
            verbose: Enable verbose logging
        """
        self.agent_id = agent_id
        self.role = role
        self.verbose = verbose
        
        # Initialize LLM
        self.llm = ChatOpenAI(
            model_name=model_name,
            temperature=temperature,
            streaming=True,
            callbacks=[StreamingStdOutCallbackHandler()] if verbose else []
        )
        
        # Initialize tools
        self.query_tool = SnowflakeQueryTool()
        self.report_tool = ReportGenerator()
        
        # Initialize memory
        self.memory = ConversationBufferMemory(
            memory_key="chat_history",
            return_messages=True
        )
        self.vector_store = VectorStore()
        
        # Initialize agent
        self.agent_executor = self._create_agent()
        
        # Agent state
        self.last_analysis_time = None
        self.cached_insights = {}
        
        logger.info(f"Sales Agent {agent_id} initialized with role: {role.value}")
    
    def _create_agent(self) -> AgentExecutor:
        """Create the sales agent with specialized tools and prompt."""
        
        # Define agent tools based on role
        tools = self._get_role_specific_tools()
        
        # Create agent prompt template
        prompt_template = self._get_agent_prompt()
        
        # Create agent
        agent = {
            "input": lambda x: x["input"],
            "agent_scratchpad": lambda x: format_log_to_str(x["intermediate_steps"]),
            "chat_history": lambda x: x["chat_history"],
            "current_date": lambda x: datetime.now().strftime("%Y-%m-%d"),
            "agent_role": lambda x: self.role.value
        }
        
        prompt = PromptTemplate(
            template=prompt_template,
            input_variables=["input", "chat_history", "current_date", "agent_role"],
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
            max_iterations=10,
            early_stopping_method="generate"
        )
    
    def _get_role_specific_tools(self) -> List[Tool]:
        """Get tools specific to the agent's role."""
        base_tools = [
            Tool(
                name="query_sales_data",
                func=self.query_tool.execute_query,
                description="Query sales data from Snowflake data warehouse. Use for sales metrics, trends, and analysis."
            ),
            Tool(
                name="generate_report",
                func=self.report_tool.generate_report,
                description="Generate sales reports and visualizations. Use for creating summaries and presentations."
            ),
            Tool(
                name="get_recent_insights",
                func=self.get_recent_insights,
                description="Get recent sales insights and trends from memory."
            ),
            Tool(
                name="store_insight",
                func=self.store_insight,
                description="Store a new sales insight in memory for future reference."
            )
        ]
        
        # Add role-specific tools
        if self.role == SalesAgentRole.CUSTOMER_INSIGHTS:
            base_tools.extend([
                Tool(
                    name="analyze_customer_segments",
                    func=self.analyze_customer_segments,
                    description="Analyze customer segments and behavior patterns."
                ),
                Tool(
                    name="identify_churn_risk",
                    func=self.identify_churn_risk,
                    description="Identify customers at risk of churning."
                )
            ])
        
        elif self.role == SalesAgentRole.REVENUE_OPTIMIZATION:
            base_tools.extend([
                Tool(
                    name="analyze_pricing_strategy",
                    func=self.analyze_pricing_strategy,
                    description="Analyze pricing strategy effectiveness and opportunities."
                ),
                Tool(
                    name="optimize_discounts",
                    func=self.optimize_discounts,
                    description="Optimize discount strategies for maximum revenue."
                )
            ])
        
        elif self.role == SalesAgentRole.FORECASTING:
            base_tools.extend([
                Tool(
                    name="generate_sales_forecast",
                    func=self.generate_sales_forecast,
                    description="Generate sales forecasts for future periods."
                ),
                Tool(
                    name="analyze_seasonal_patterns",
                    func=self.analyze_seasonal_patterns,
                    description="Analyze seasonal sales patterns and trends."
                )
            ])
        
        return base_tools
    
    def _get_agent_prompt(self) -> str:
        """Get the agent prompt template based on role."""
        
        role_prompts = {
            SalesAgentRole.PERFORMANCE_ANALYST: """
            You are a Sales Performance Analyst AI Agent. Your role is to analyze sales performance,
            identify trends, and provide actionable insights to improve sales outcomes.
            
            Specializations:
            - Daily/Weekly/Monthly sales performance analysis
            - Product performance and category analysis
            - Regional and store performance comparisons
            - Sales target achievement tracking
            - Performance bottleneck identification
            
            Always provide:
            1. Clear metrics and KPIs
            2. Trend analysis with comparisons
            3. Root cause analysis for issues
            4. Actionable recommendations
            5. Impact assessment
            
            Current Date: {current_date}
            Agent Role: {agent_role}
            
            Previous conversation:
            {chat_history}
            
            Tools available:
            {tools}
            
            User Input: {input}
            
            {agent_scratchpad}
            """,
            
            SalesAgentRole.CUSTOMER_INSIGHTS: """
            You are a Customer Insights Analyst AI Agent. Your role is to analyze customer behavior,
            segment customers, and provide insights to improve customer engagement and retention.
            
            Specializations:
            - Customer segmentation and profiling
            - Lifetime value analysis
            - Purchase pattern analysis
            - Churn prediction and prevention
            - Customer journey optimization
            
            Always provide:
            1. Customer segment analysis
            2. Behavioral patterns and trends
            3. Retention and engagement strategies
            4. Personalization opportunities
            5. ROI impact assessment
            
            Current Date: {current_date}
            Agent Role: {agent_role}
            
            Previous conversation:
            {chat_history}
            
            Tools available:
            {tools}
            
            User Input: {input}
            
            {agent_scratchpad}
            """,
            
            SalesAgentRole.REVENUE_OPTIMIZATION: """
            You are a Revenue Optimization Analyst AI Agent. Your role is to maximize revenue
            through pricing optimization, discount strategies, and cross-sell/up-sell opportunities.
            
            Specializations:
            - Price elasticity analysis
            - Discount optimization
            - Bundle and package optimization
            - Cross-sell and up-sell analysis
            - Revenue leakage identification
            
            Always provide:
            1. Revenue impact analysis
            2. Optimization opportunities
            3. Implementation recommendations
            4. Risk assessment
            5. Measurement framework
            
            Current Date: {current_date}
            Agent Role: {agent_role}
            
            Previous conversation:
            {chat_history}
            
            Tools available:
            {tools}
            
            User Input: {input}
            
            {agent_scratchpad}
            """,
            
            SalesAgentRole.FORECASTING: """
            You are a Sales Forecasting Analyst AI Agent. Your role is to predict future sales,
            analyze trends, and provide accurate forecasts for business planning.
            
            Specializations:
            - Short-term and long-term sales forecasting
            - Trend analysis and pattern recognition
            - Seasonality and holiday effects
            - Forecast accuracy measurement
            - Scenario planning and what-if analysis
            
            Always provide:
            1. Forecast with confidence intervals
            2. Key assumptions and drivers
            3. Risk factors and sensitivities
            4. Data quality assessment
            5. Monitoring recommendations
            
            Current Date: {current_date}
            Agent Role: {agent_role}
            
            Previous conversation:
            {chat_history}
            
            Tools available:
            {tools}
            
            User Input: {input}
            
            {agent_scratchpad}
            """
        }
        
        return role_prompts.get(self.role, role_prompts[SalesAgentRole.PERFORMANCE_ANALYST])
    
    async def process_query(self, query: str, context: Dict = None) -> Dict:
        """
        Process a sales-related query.
        
        Args:
            query: User query or request
            context: Additional context for the query
            
        Returns:
            Dictionary with agent response and insights
        """
        try:
            logger.info(f"Processing sales query: {query}")
            
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
            
            # Extract insights from response
            insights = await self._extract_insights(response["output"])
            
            # Store in memory
            await self._update_agent_memory(query, response["output"], insights)
            
            return {
                "agent_id": self.agent_id,
                "role": self.role.value,
                "query": query,
                "response": response["output"],
                "insights": insights,
                "timestamp": datetime.now().isoformat(),
                "confidence": self._calculate_confidence(response["output"])
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
    
    async def _extract_insights(self, response: str) -> List[SalesInsight]:
        """
        Extract structured insights from agent response.
        
        Args:
            response: Agent response text
            
        Returns:
            List of SalesInsight objects
        """
        insights = []
        
        # Use LLM to extract structured insights
        insight_extraction_prompt = f"""
        Extract structured sales insights from the following text.
        Return as JSON array with fields: insight_type, metric, value, trend, confidence, recommendation, impact_score, timeframe.
        
        Text: {response}
        
        JSON:
        """
        
        try:
            insight_response = await self.llm.agenerate([insight_extraction_prompt])
            insight_json = json.loads(insight_response.generations[0][0].text)
            
            for insight_data in insight_json:
                insight = SalesInsight(
                    insight_type=insight_data.get("insight_type", "unknown"),
                    metric=insight_data.get("metric", ""),
                    value=float(insight_data.get("value", 0)),
                    trend=insight_data.get("trend", "stable"),
                    confidence=float(insight_data.get("confidence", 0.5)),
                    recommendation=insight_data.get("recommendation", ""),
                    impact_score=float(insight_data.get("impact_score", 0)),
                    timeframe=insight_data.get("timeframe", "recent")
                )
                insights.append(insight)
                
        except Exception as e:
            logger.warning(f"Failed to extract insights: {str(e)}")
        
        return insights
    
    def _calculate_confidence(self, response: str) -> float:
        """
        Calculate confidence score for agent response.
        
        Args:
            response: Agent response text
            
        Returns:
            Confidence score between 0 and 1
        """
        # Simple confidence calculation based on response characteristics
        confidence_factors = {
            "contains_numbers": 0.2,
            "contains_comparisons": 0.2,
            "contains_recommendations": 0.3,
            "length_greater_than_100": 0.1,
            "mentions_data_sources": 0.2
        }
        
        confidence = 0.5  # Base confidence
        
        # Check for confidence factors
        if any(char.isdigit() for char in response):
            confidence += confidence_factors["contains_numbers"]
        
        if "vs" in response.lower() or "compared" in response.lower():
            confidence += confidence_factors["contains_comparisons"]
        
        if "recommend" in response.lower() or "suggest" in response.lower():
            confidence += confidence_factors["contains_recommendations"]
        
        if len(response) > 100:
            confidence += confidence_factors["length_greater_than_100"]
        
        if "data" in response.lower() or "query" in response.lower():
            confidence += confidence_factors["mentions_data_sources"]
        
        return min(1.0, max(0.0, confidence))
    
    async def _update_agent_memory(self, query: str, response: str, insights: List[SalesInsight]):
        """Update agent memory with query, response, and insights."""
        # Store in conversation memory
        self.memory.save_context({"input": query}, {"output": response})
        
        # Store insights in vector store
        for insight in insights:
            insight_text = f"{insight.insight_type}: {insight.metric} = {insight.value} ({insight.trend}) - {insight.recommendation}"
            await self.vector_store.store_document(
                text=insight_text,
                metadata={
                    "agent_id": self.agent_id,
                    "role": self.role.value,
                    "insight_type": insight.insight_type,
                    "metric": insight.metric,
                    "value": insight.value,
                    "trend": insight.trend,
                    "confidence": insight.confidence,
                    "impact_score": insight.impact_score,
                    "timeframe": insight.timeframe,
                    "timestamp": datetime.now().isoformat()
                }
            )
    
    # Role-specific tool implementations
    
    async def analyze_customer_segments(self, timeframe: str = "30d") -> List[CustomerSegment]:
        """
        Analyze customer segments for the given timeframe.
        
        Args:
            timeframe: Timeframe for analysis (e.g., "7d", "30d", "90d")
            
        Returns:
            List of CustomerSegment objects
        """
        logger.info(f"Analyzing customer segments for timeframe: {timeframe}")
        
        # Query customer segmentation data
        query = f"""
        SELECT 
            customer_segment,
            COUNT(DISTINCT customer_id) as customer_count,
            AVG(lifetime_value) as avg_lifetime_value,
            AVG(purchase_frequency) as avg_purchase_frequency,
            AVG(churn_risk_score) as avg_churn_risk,
            ARRAY_AGG(DISTINCT top_category) as top_categories
        FROM ANALYTICS.VW_CUSTOMER_SEGMENT_ANALYSIS
        WHERE last_purchase_date >= DATEADD(day, -{timeframe.replace('d', '')}, CURRENT_DATE())
        GROUP BY customer_segment
        ORDER BY avg_lifetime_value DESC
        LIMIT 10
        """
        
        try:
            results = await self.query_tool.execute_query(query)
            segments = []
            
            for row in results:
                segment = CustomerSegment(
                    segment_id=f"segment_{row['customer_segment'].lower().replace(' ', '_')}",
                    segment_name=row['customer_segment'],
                    customer_count=row['customer_count'],
                    avg_lifetime_value=row['avg_lifetime_value'],
                    purchase_frequency=row['avg_purchase_frequency'],
                    churn_risk=row['avg_churn_risk'],
                    top_categories=row['top_categories'],
                    recommendation=f"Focus on retention for {row['customer_segment']} segment with {row['customer_count']} customers"
                )
                segments.append(segment)
            
            return segments
            
        except Exception as e:
            logger.error(f"Error analyzing customer segments: {str(e)}")
            return []
    
    async def identify_churn_risk(self, threshold: float = 0.7, limit: int = 50) -> List[Dict]:
        """
        Identify customers at high risk of churning.
        
        Args:
            threshold: Churn risk threshold (0-1)
            limit: Maximum number of customers to return
            
        Returns:
            List of high-risk customers
        """
        logger.info(f"Identifying churn risk customers with threshold: {threshold}")
        
        query = f"""
        SELECT 
            customer_id,
            customer_name,
            email,
            last_purchase_date,
            recency_days,
            churn_risk_level,
            lifetime_value,
            total_orders
        FROM ANALYTICS.VW_CUSTOMER_CHURN_RISK
        WHERE churn_risk_level IN ('HIGH', 'MEDIUM')
        AND recency_days > 30
        ORDER BY churn_risk_level DESC, lifetime_value DESC
        LIMIT {limit}
        """
        
        try:
            results = await self.query_tool.execute_query(query)
            return results
            
        except Exception as e:
            logger.error(f"Error identifying churn risk: {str(e)}")
            return []
    
    async def analyze_pricing_strategy(self, category: str = None) -> Dict:
        """
        Analyze pricing strategy effectiveness.
        
        Args:
            category: Optional product category filter
            
        Returns:
            Pricing analysis results
        """
        logger.info(f"Analyzing pricing strategy for category: {category or 'all'}")
        
        category_filter = f"AND category = '{category}'" if category else ""
        
        query = f"""
        SELECT 
            category,
            subcategory,
            AVG(current_price) as avg_price,
            AVG(cost_price) as avg_cost,
            AVG(profit_margin_percent) as avg_margin,
            COUNT(DISTINCT product_id) as product_count,
            SUM(CASE WHEN stock_status = 'Low Stock' THEN 1 ELSE 0 END) as low_stock_count,
            AVG(rating) as avg_rating
        FROM ANALYTICS.DIM_PRODUCT
        WHERE is_current = TRUE {category_filter}
        GROUP BY category, subcategory
        ORDER BY avg_margin DESC
        """
        
        try:
            results = await self.query_tool.execute_query(query)
            
            analysis = {
                "category_analysis": results,
                "recommendations": [],
                "opportunities": []
            }
            
            # Generate recommendations
            for row in results:
                if row['avg_margin'] < 20:
                    analysis["opportunities"].append({
                        "category": row['category'],
                        "opportunity": "Low margin, consider price increase",
                        "current_margin": row['avg_margin'],
                        "target_margin": 25
                    })
                
                if row['low_stock_count'] > 0:
                    analysis["recommendations"].append({
                        "category": row['category'],
                        "recommendation": f"Address low stock for {row['low_stock_count']} products",
                        "priority": "high" if row['low_stock_count'] > 5 else "medium"
                    })
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing pricing strategy: {str(e)}")
            return {"error": str(e)}
    
    async def optimize_discounts(self) -> Dict:
        """
        Analyze and optimize discount strategies.
        
        Returns:
            Discount optimization recommendations
        """
        logger.info("Analyzing discount optimization")
        
        query = """
        SELECT 
            DATE_TRUNC('week', transaction_timestamp) as week,
            AVG(line_discount_percent) as avg_discount,
            SUM(line_final_amount) as total_sales,
            SUM(line_discount_amount) as total_discount,
            COUNT(DISTINCT transaction_id) as transaction_count,
            SUM(quantity) as total_quantity
        FROM ANALYTICS.FACT_SALES
        WHERE line_discount_percent > 0
        AND transaction_timestamp >= DATEADD(month, -3, CURRENT_DATE())
        GROUP BY DATE_TRUNC('week', transaction_timestamp)
        ORDER BY week DESC
        """
        
        try:
            results = await self.query_tool.execute_query(query)
            
            # Calculate optimization metrics
            total_discount_impact = sum(r['total_discount'] for r in results)
            total_sales = sum(r['total_sales'] for r in results)
            discount_rate = (total_discount_impact / total_sales * 100) if total_sales > 0 else 0
            
            optimization = {
                "historical_analysis": results,
                "summary_metrics": {
                    "total_discount_amount": total_discount_impact,
                    "total_sales_amount": total_sales,
                    "average_discount_rate_percent": discount_rate,
                    "analysis_period_weeks": len(results)
                },
                "recommendations": [
                    {
                        "action": "Review discount effectiveness",
                        "details": f"Current discount rate: {discount_rate:.2f}%",
                        "expected_impact": "Reduce discount leakage by 10-20%"
                    }
                ]
            }
            
            return optimization
            
        except Exception as e:
            logger.error(f"Error optimizing discounts: {str(e)}")
            return {"error": str(e)}
    
    async def generate_sales_forecast(
        self,
        horizon_days: int = 30,
        confidence_level: float = 0.95
    ) -> Dict:
        """
        Generate sales forecast for specified horizon.
        
        Args:
            horizon_days: Forecast horizon in days
            confidence_level: Confidence level for forecast
            
        Returns:
            Sales forecast results
        """
        logger.info(f"Generating sales forecast for {horizon_days} days")
        
        # Query historical sales data
        query = f"""
        SELECT 
            DATE(transaction_timestamp) as sale_date,
            SUM(line_final_amount) as daily_sales,
            COUNT(DISTINCT transaction_id) as daily_transactions,
            SUM(quantity) as daily_quantity
        FROM ANALYTICS.FACT_SALES
        WHERE transaction_timestamp >= DATEADD(day, -90, CURRENT_DATE())
        GROUP BY DATE(transaction_timestamp)
        ORDER BY sale_date
        """
        
        try:
            historical_data = await self.query_tool.execute_query(query)
            
            # Simple forecasting logic (in production, use proper time series model)
            # This is a placeholder - implement proper forecasting
            recent_sales = [row['daily_sales'] for row in historical_data[-7:]]
            avg_daily_sales = sum(recent_sales) / len(recent_sales) if recent_sales else 0
            
            # Generate forecast
            forecast = []
            for day in range(1, horizon_days + 1):
                forecast_date = (datetime.now() + timedelta(days=day)).date()
                
                # Simple forecast with seasonality and trend
                base_forecast = avg_daily_sales
                
                # Add weekend boost (20% increase)
                if forecast_date.weekday() >= 5:  # Saturday or Sunday
                    base_forecast *= 1.2
                
                forecast.append({
                    "date": forecast_date.isoformat(),
                    "forecast_sales": round(base_forecast, 2),
                    "confidence_lower": round(base_forecast * 0.8, 2),
                    "confidence_upper": round(base_forecast * 1.2, 2),
                    "confidence_level": confidence_level
                })
            
            total_forecast = sum(f['forecast_sales'] for f in forecast)
            
            return {
                "forecast_horizon_days": horizon_days,
                "confidence_level": confidence_level,
                "historical_data_points": len(historical_data),
                "average_daily_sales": round(avg_daily_sales, 2),
                "total_forecast_sales": round(total_forecast, 2),
                "daily_forecast": forecast,
                "assumptions": [
                    "Based on last 7 days average",
                    "Weekend boost applied (20% increase)",
                    "No major promotions or holidays considered"
                ]
            }
            
        except Exception as e:
            logger.error(f"Error generating sales forecast: {str(e)}")
            return {"error": str(e)}
    
    async def analyze_seasonal_patterns(self) -> Dict:
        """
        Analyze seasonal sales patterns.
        
        Returns:
            Seasonal pattern analysis
        """
        logger.info("Analyzing seasonal patterns")
        
        query = """
        SELECT 
            d.season,
            d.month,
            d.month_name,
            COUNT(DISTINCT fs.transaction_id) as transaction_count,
            SUM(fs.line_final_amount) as total_sales,
            AVG(fs.line_final_amount) as avg_transaction_value,
            SUM(fs.quantity) as total_quantity
        FROM ANALYTICS.FACT_SALES fs
        JOIN ANALYTICS.DIM_DATE d ON fs.date_key = d.date_key
        WHERE d.date >= DATEADD(year, -2, CURRENT_DATE())
        GROUP BY d.season, d.month, d.month_name
        ORDER BY d.season, d.month
        """
        
        try:
            results = await self.query_tool.execute_query(query)
            
            # Calculate seasonal patterns
            seasonal_data = {}
            for row in results:
                season = row['season']
                if season not in seasonal_data:
                    seasonal_data[season] = {
                        "total_sales": 0,
                        "transaction_count": 0,
                        "months": []
                    }
                
                seasonal_data[season]["total_sales"] += row['total_sales']
                seasonal_data[season]["transaction_count"] += row['transaction_count']
                seasonal_data[season]["months"].append({
                    "month": row['month_name'],
                    "sales": row['total_sales'],
                    "transactions": row['transaction_count'],
                    "avg_value": row['avg_transaction_value']
                })
            
            # Identify strongest season
            strongest_season = max(seasonal_data.items(), key=lambda x: x[1]["total_sales"])[0]
            
            return {
                "seasonal_analysis": seasonal_data,
                "strongest_season": strongest_season,
                "peak_month": max(
                    [(m['month'], m['sales']) for season_data in seasonal_data.values() for m in season_data['months']],
                    key=lambda x: x[1]
                )[0],
                "recommendations": [
                    f"Focus inventory planning around {strongest_season} season",
                    "Adjust marketing spend based on seasonal patterns",
                    "Plan promotions during slower seasons"
                ]
            }
            
        except Exception as e:
            logger.error(f"Error analyzing seasonal patterns: {str(e)}")
            return {"error": str(e)}
    
    # Memory access tools
    
    async def get_recent_insights(self, limit: int = 10) -> List[Dict]:
        """
        Get recent sales insights from memory.
        
        Args:
            limit: Maximum number of insights to return
            
        Returns:
            List of recent insights
        """
        try:
            recent_docs = await self.vector_store.search_documents(
                query="sales insights performance",
                limit=limit
            )
            
            insights = []
            for doc in recent_docs:
                insights.append({
                    "text": doc["text"],
                    "metadata": doc["metadata"],
                    "similarity": doc["similarity"]
                })
            
            return insights
            
        except Exception as e:
            logger.error(f"Error getting recent insights: {str(e)}")
            return []
    
    async def store_insight(self, insight_text: str, metadata: Dict = None) -> bool:
        """
        Store a new sales insight in memory.
        
        Args:
            insight_text: Insight text to store
            metadata: Additional metadata
            
        Returns:
            True if successful, False otherwise
        """
        try:
            default_metadata = {
                "agent_id": self.agent_id,
                "role": self.role.value,
                "timestamp": datetime.now().isoformat(),
                "source": "manual_entry"
            }
            
            if metadata:
                default_metadata.update(metadata)
            
            await self.vector_store.store_document(
                text=insight_text,
                metadata=default_metadata
            )
            
            logger.info(f"Stored insight: {insight_text[:100]}...")
            return True
            
        except Exception as e:
            logger.error(f"Error storing insight: {str(e)}")
            return False
    
    def get_agent_status(self) -> Dict:
        """
        Get current agent status and statistics.
        
        Returns:
            Agent status information
        """
        return {
            "agent_id": self.agent_id,
            "role": self.role.value,
            "status": "active",
            "model": self.llm.model_name,
            "temperature": self.llm.temperature,
            "memory_size": len(self.memory.chat_memory.messages) if hasattr(self.memory.chat_memory, 'messages') else 0,
            "last_activity": self.last_analysis_time or "never",
            "tools_available": [tool.name for tool in self._get_role_specific_tools()],
            "verbose": self.verbose
        }


# Factory function for creating sales agents
def create_sales_agent(
    agent_id: str,
    role: str = "performance_analyst",
    **kwargs
) -> SalesAgent:
    """
    Factory function to create sales agents.
    
    Args:
        agent_id: Unique agent identifier
        role: Agent role (performance_analyst, customer_insights, revenue_optimization, forecasting)
        **kwargs: Additional agent parameters
        
    Returns:
        Configured SalesAgent instance
    """
    role_enum = SalesAgentRole(role.lower())
    return SalesAgent(agent_id=agent_id, role=role_enum, **kwargs)