"""
Marketing Agent - Specialized agent for marketing analysis and optimization.
Handles campaign performance, customer acquisition, and marketing ROI.
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


class MarketingAgentRole(Enum):
    """Marketing agent specialized roles."""
    CAMPAIGN_ANALYST = "campaign_analyst"
    CUSTOMER_ACQUISITION = "customer_acquisition"
    ROI_OPTIMIZATION = "roi_optimization"
    CONTENT_STRATEGY = "content_strategy"


@dataclass
class CampaignPerformance:
    """Campaign performance data structure."""
    campaign_id: str
    campaign_name: str
    channel: str
    spend: float
    revenue: float
    roas: float
    cpa: float
    conversions: int
    status: str
    recommendation: str


@dataclass
class CustomerAcquisition:
    """Customer acquisition data structure."""
    channel: str
    new_customers: int
    acquisition_cost: float
    lifetime_value: float
    payback_period: float
    efficiency_score: float
    recommendation: str


class MarketingAgent:
    """
    AI Agent specialized in marketing analysis, optimization, and strategy.
    """
    
    def __init__(
        self,
        agent_id: str = "marketing_agent_001",
        role: MarketingAgentRole = MarketingAgentRole.CAMPAIGN_ANALYST,
        model_name: str = "gpt-4",
        temperature: float = 0.3,
        verbose: bool = False
    ):
        """
        Initialize Marketing Agent.
        
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
        self.last_analysis_time = None
        self.active_alerts = []
        
        logger.info(f"Marketing Agent {agent_id} initialized with role: {role.value}")
    
    def _create_agent(self) -> AgentExecutor:
        """Create the marketing agent with specialized tools and prompt."""
        
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
                name="query_marketing_data",
                func=self.query_tool.execute_query,
                description="Query marketing data from Snowflake data warehouse. Use for campaign metrics, ROI, and analysis."
            ),
            Tool(
                name="send_alert",
                func=self.alert_tool.send_alert,
                description="Send alerts for marketing anomalies, opportunities, or issues."
            ),
            Tool(
                name="get_campaign_performance",
                func=self.get_campaign_performance,
                description="Get detailed performance metrics for marketing campaigns."
            ),
            Tool(
                name="analyze_customer_acquisition",
                func=self.analyze_customer_acquisition,
                description="Analyze customer acquisition metrics and efficiency."
            )
        ]
        
        # Add role-specific tools
        if self.role == MarketingAgentRole.ROI_OPTIMIZATION:
            base_tools.extend([
                Tool(
                    name="optimize_marketing_spend",
                    func=self.optimize_marketing_spend,
                    description="Optimize marketing spend allocation across channels."
                ),
                Tool(
                    name="calculate_roi",
                    func=self.calculate_roi,
                    description="Calculate ROI for marketing activities and campaigns."
                )
            ])
        
        elif self.role == MarketingAgentRole.CONTENT_STRATEGY:
            base_tools.extend([
                Tool(
                    name="analyze_content_performance",
                    func=self.analyze_content_performance,
                    description="Analyze content performance across channels."
                ),
                Tool(
                    name="generate_content_ideas",
                    func=self.generate_content_ideas,
                    description="Generate content ideas based on performance data."
                )
            ])
        
        return base_tools
    
    def _get_agent_prompt(self) -> str:
        """Get the agent prompt template based on role."""
        
        role_prompts = {
            MarketingAgentRole.CAMPAIGN_ANALYST: """
            You are a Marketing Campaign Analyst AI Agent. Your role is to analyze campaign performance,
            optimize marketing efforts, and provide actionable insights to improve marketing ROI.
            
            Specializations:
            - Campaign performance analysis and optimization
            - Channel effectiveness and attribution
            - Conversion rate optimization
            - Budget allocation and spend efficiency
            - A/B testing and experimentation analysis
            
            Always provide:
            1. Campaign performance metrics (ROAS, CPA, CTR, etc.)
            2. Channel comparison and effectiveness
            3. Budget optimization recommendations
            4. Testing and experimentation suggestions
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
            
            MarketingAgentRole.CUSTOMER_ACQUISITION: """
            You are a Customer Acquisition Analyst AI Agent. Your role is to analyze customer acquisition,
            optimize acquisition channels, and improve customer acquisition efficiency.
            
            Specializations:
            - Customer acquisition cost analysis
            - Channel efficiency and optimization
            - Lead generation and conversion
            - Customer lifetime value optimization
            - Acquisition funnel analysis
            
            Always provide:
            1. Acquisition cost analysis by channel
            2. Customer lifetime value calculations
            3. Channel efficiency recommendations
            4. Funnel optimization suggestions
            5. Payback period analysis
            
            Current Date: {current_date}
            Agent Role: {agent_role}
            
            Previous conversation:
            {chat_history}
            
            Tools available:
            {tools}
            
            User Input: {input}
            
            {agent_scratchpad}
            """,
            
            MarketingAgentRole.ROI_OPTIMIZATION: """
            You are a Marketing ROI Optimization Analyst AI Agent. Your role is to maximize marketing ROI
            through spend optimization, channel allocation, and performance analysis.
            
            Specializations:
            - Marketing spend optimization
            - ROI calculation and analysis
            - Budget allocation strategies
            - Performance-based optimization
            - Incrementality measurement
            
            Always provide:
            1. ROI analysis by campaign and channel
            2. Spend optimization recommendations
            3. Budget reallocation strategies
            4. Performance benchmarks
            5. Incrementality assessment
            
            Current Date: {current_date}
            Agent Role: {agent_role}
            
            Previous conversation:
            {chat_history}
            
            Tools available:
            {tools}
            
            User Input: {input}
            
            {agent_scratchpad}
            """,
            
            MarketingAgentRole.CONTENT_STRATEGY: """
            You are a Content Strategy Analyst AI Agent. Your role is to analyze content performance,
            optimize content strategy, and drive engagement through effective content.
            
            Specializations:
            - Content performance analysis
            - Content strategy optimization
            - Audience engagement analysis
            - Content ROI measurement
            - Content calendar planning
            
            Always provide:
            1. Content performance metrics
            2. Engagement analysis by content type
            3. Content strategy recommendations
            4. ROI of content marketing
            5. Content calendar suggestions
            
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
        
        return role_prompts.get(self.role, role_prompts[MarketingAgentRole.CAMPAIGN_ANALYST])
    
    async def process_query(self, query: str, context: Dict = None) -> Dict:
        """
        Process a marketing-related query.
        
        Args:
            query: User query or request
            context: Additional context for the query
            
        Returns:
            Dictionary with agent response and insights
        """
        try:
            logger.info(f"Processing marketing query: {query}")
            
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
            insights = await self._extract_marketing_insights(response["output"])
            
            # Store in memory
            await self._update_agent_memory(query, response["output"], insights)
            
            # Check for alerts
            await self._check_for_alerts(insights)
            
            return {
                "agent_id": self.agent_id,
                "role": self.role.value,
                "query": query,
                "response": response["output"],
                "insights": insights,
                "timestamp": datetime.now().isoformat(),
                "alerts_generated": len(self.active_alerts[-5:]) if self.active_alerts else 0
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
    
    async def _extract_marketing_insights(self, response: str) -> List[Dict]:
        """
        Extract structured marketing insights from agent response.
        
        Args:
            response: Agent response text
            
        Returns:
            List of marketing insight dictionaries
        """
        insights = []
        
        # Use LLM to extract structured insights
        insight_extraction_prompt = f"""
        Extract structured marketing insights from the following text.
        Return as JSON array with fields: insight_type, metric, value, comparison, recommendation, impact, timeframe.
        
        Text: {response}
        
        JSON:
        """
        
        try:
            insight_response = await self.llm.agenerate([insight_extraction_prompt])
            insight_json = json.loads(insight_response.generations[0][0].text)
            insights = insight_json
            
        except Exception as e:
            logger.warning(f"Failed to extract insights: {str(e)}")
        
        return insights
    
    async def _update_agent_memory(self, query: str, response: str, insights: List[Dict]):
        """Update agent memory with query, response, and insights."""
        # Store in conversation memory
        self.memory.save_context({"input": query}, {"output": response})
        
        # Store insights in vector store
        for insight in insights:
            insight_text = f"{insight.get('insight_type', 'marketing')}: {insight.get('metric', '')} = {insight.get('value', '')} - {insight.get('recommendation', '')}"
            await self.vector_store.store_document(
                text=insight_text,
                metadata={
                    "agent_id": self.agent_id,
                    "role": self.role.value,
                    "insight_type": insight.get("insight_type", ""),
                    "metric": insight.get("metric", ""),
                    "value": insight.get("value", ""),
                    "comparison": insight.get("comparison", ""),
                    "impact": insight.get("impact", ""),
                    "timeframe": insight.get("timeframe", ""),
                    "timestamp": datetime.now().isoformat()
                }
            )
    
    async def _check_for_alerts(self, insights: List[Dict]):
        """Check insights for alert conditions."""
        alert_conditions = [
            ("ROAS", "below", 2.0),  # ROAS below 2.0
            ("CPA", "above", 50.0),  # CPA above $50
            ("CTR", "below", 0.02),  # CTR below 2%
            ("conversion_rate", "below", 0.02),  # Conversion rate below 2%
        ]
        
        for insight in insights:
            metric = insight.get("metric", "").lower()
            value = insight.get("value", 0)
            
            for alert_metric, condition, threshold in alert_conditions:
                if alert_metric.lower() in metric:
                    if (condition == "below" and value < threshold) or \
                       (condition == "above" and value > threshold):
                        
                        alert_message = f"Alert: {metric} is {value} ({condition} threshold {threshold})"
                        await self.alert_tool.send_alert(
                            message=alert_message,
                            level="warning",
                            category="marketing_performance"
                        )
                        
                        self.active_alerts.append({
                            "timestamp": datetime.now().isoformat(),
                            "metric": metric,
                            "value": value,
                            "condition": condition,
                            "threshold": threshold,
                            "message": alert_message
                        })
    
    # Marketing analysis tools
    
    async def get_campaign_performance(
        self,
        timeframe: str = "30d",
        status: str = "active"
    ) -> List[CampaignPerformance]:
        """
        Get performance metrics for marketing campaigns.
        
        Args:
            timeframe: Timeframe for analysis
            status: Campaign status filter
            
        Returns:
            List of CampaignPerformance objects
        """
        logger.info(f"Getting campaign performance for timeframe: {timeframe}")
        
        # Build query based on timeframe
        if timeframe.endswith('d'):
            days = int(timeframe[:-1])
            date_filter = f"start_date >= DATEADD(day, -{days}, CURRENT_DATE())"
        elif timeframe.endswith('m'):
            months = int(timeframe[:-1])
            date_filter = f"start_date >= DATEADD(month, -{months}, CURRENT_DATE())"
        else:
            date_filter = "1=1"
        
        query = f"""
        SELECT 
            campaign_id,
            campaign_name,
            channel,
            spend_amount,
            revenue_attributed,
            roas,
            cost_per_acquisition,
            conversions,
            status
        FROM ANALYTICS.FACT_MARKETING_PERFORMANCE mp
        JOIN ANALYTICS.DIM_CAMPAIGN c ON mp.campaign_sk = c.campaign_sk
        WHERE {date_filter}
        AND c.status = '{status}'
        AND c.is_current = TRUE
        ORDER BY roas DESC
        LIMIT 20
        """
        
        try:
            results = await self.query_tool.execute_query(query)
            campaigns = []
            
            for row in results:
                # Generate recommendation based on performance
                if row['roas'] < 2.0:
                    recommendation = f"Optimize campaign {row['campaign_name']} - ROAS below target (current: {row['roas']:.2f})"
                elif row['cost_per_acquisition'] > 50:
                    recommendation = f"Reduce CPA for {row['campaign_name']} - Current CPA: ${row['cost_per_acquisition']:.2f}"
                else:
                    recommendation = f"Campaign {row['campaign_name']} performing well - Maintain current strategy"
                
                campaign = CampaignPerformance(
                    campaign_id=row['campaign_id'],
                    campaign_name=row['campaign_name'],
                    channel=row['channel'],
                    spend=row['spend_amount'],
                    revenue=row['revenue_attributed'],
                    roas=row['roas'],
                    cpa=row['cost_per_acquisition'],
                    conversions=row['conversions'],
                    status=row['status'],
                    recommendation=recommendation
                )
                campaigns.append(campaign)
            
            return campaigns
            
        except Exception as e:
            logger.error(f"Error getting campaign performance: {str(e)}")
            return []
    
    async def analyze_customer_acquisition(self) -> List[CustomerAcquisition]:
        """
        Analyze customer acquisition metrics by channel.
        
        Returns:
            List of CustomerAcquisition objects
        """
        logger.info("Analyzing customer acquisition")
        
        query = """
        SELECT 
            channel,
            COUNT(DISTINCT customer_sk) as new_customers,
            SUM(spend_amount) / COUNT(DISTINCT customer_sk) as acquisition_cost,
            AVG(customer_lifetime_value) as avg_lifetime_value,
            AVG(payback_period_days) as avg_payback_period
        FROM ANALYTICS.FACT_MARKETING_PERFORMANCE mp
        JOIN ANALYTICS.DIM_CHANNEL c ON mp.channel_sk = c.channel_sk
        WHERE conversions > 0
        AND mp.reporting_date >= DATEADD(month, -3, CURRENT_DATE())
        GROUP BY channel
        ORDER BY new_customers DESC
        """
        
        try:
            results = await self.query_tool.execute_query(query)
            acquisitions = []
            
            for row in results:
                # Calculate efficiency score
                efficiency_score = (row['avg_lifetime_value'] / row['acquisition_cost']) \
                    if row['acquisition_cost'] > 0 else 0
                
                # Generate recommendation
                if efficiency_score > 3:
                    recommendation = f"Channel {row['channel']} highly efficient - Increase investment"
                elif efficiency_score > 1:
                    recommendation = f"Channel {row['channel']} efficient - Maintain current spend"
                else:
                    recommendation = f"Channel {row['channel']} inefficient - Review and optimize"
                
                acquisition = CustomerAcquisition(
                    channel=row['channel'],
                    new_customers=row['new_customers'],
                    acquisition_cost=row['acquisition_cost'],
                    lifetime_value=row['avg_lifetime_value'],
                    payback_period=row['avg_payback_period'],
                    efficiency_score=efficiency_score,
                    recommendation=recommendation
                )
                acquisitions.append(acquisition)
            
            return acquisitions
            
        except Exception as e:
            logger.error(f"Error analyzing customer acquisition: {str(e)}")
            return []
    
    async def optimize_marketing_spend(self) -> Dict:
        """
        Optimize marketing spend allocation across channels.
        
        Returns:
            Spend optimization recommendations
        """
        logger.info("Optimizing marketing spend")
        
        # Get current spend and performance
        query = """
        SELECT 
            channel,
            SUM(spend_amount) as total_spend,
            SUM(revenue_attributed) as total_revenue,
            AVG(roas) as avg_roas,
            COUNT(DISTINCT campaign_sk) as campaign_count
        FROM ANALYTICS.FACT_MARKETING_PERFORMANCE
        WHERE reporting_date >= DATEADD(month, -3, CURRENT_DATE())
        GROUP BY channel
        ORDER BY total_spend DESC
        """
        
        try:
            results = await self.query_tool.execute_query(query)
            
            total_spend = sum(r['total_spend'] for r in results)
            total_revenue = sum(r['total_revenue'] for r in results)
            
            # Calculate optimal allocation
            recommendations = []
            for row in results:
                current_allocation = (row['total_spend'] / total_spend * 100) if total_spend > 0 else 0
                
                # Simple optimization: allocate more to high ROAS channels
                if row['avg_roas'] > 3.0:
                    target_allocation = min(current_allocation * 1.5, 100)
                    action = "increase"
                elif row['avg_roas'] < 1.5:
                    target_allocation = max(current_allocation * 0.5, 0)
                    action = "decrease"
                else:
                    target_allocation = current_allocation
                    action = "maintain"
                
                if action != "maintain":
                    recommendations.append({
                        "channel": row['channel'],
                        "current_allocation_percent": round(current_allocation, 2),
                        "target_allocation_percent": round(target_allocation, 2),
                        "action": action,
                        "roas": round(row['avg_roas'], 2),
                        "current_spend": round(row['total_spend'], 2),
                        "recommended_spend": round((target_allocation / 100) * total_spend, 2)
                    })
            
            return {
                "current_total_spend": round(total_spend, 2),
                "current_total_revenue": round(total_revenue, 2),
                "overall_roas": round(total_revenue / total_spend, 2) if total_spend > 0 else 0,
                "channel_performance": results,
                "optimization_recommendations": recommendations,
                "expected_impact": "Increase overall ROAS by 10-20% with optimized allocation"
            }
            
        except Exception as e:
            logger.error(f"Error optimizing marketing spend: {str(e)}")
            return {"error": str(e)}
    
    async def calculate_roi(self, campaign_id: str = None) -> Dict:
        """
        Calculate ROI for marketing activities.
        
        Args:
            campaign_id: Optional specific campaign ID
            
        Returns:
            ROI analysis results
        """
        logger.info(f"Calculating ROI for campaign: {campaign_id or 'all'}")
        
        campaign_filter = f"AND c.campaign_id = '{campaign_id}'" if campaign_id else ""
        
        query = f"""
        SELECT 
            c.campaign_id,
            c.campaign_name,
            SUM(mp.spend_amount) as total_spend,
            SUM(mp.revenue_attributed) as total_revenue,
            AVG(mp.roas) as avg_roas,
            COUNT(DISTINCT mp.campaign_sk) as periods,
            MIN(mp.reporting_date) as start_date,
            MAX(mp.reporting_date) as end_date
        FROM ANALYTICS.FACT_MARKETING_PERFORMANCE mp
        JOIN ANALYTICS.DIM_CAMPAIGN c ON mp.campaign_sk = c.campaign_sk
        WHERE 1=1 {campaign_filter}
        GROUP BY c.campaign_id, c.campaign_name
        ORDER BY total_revenue DESC
        """
        
        try:
            results = await self.query_tool.execute_query(query)
            
            roi_analysis = []
            for row in results:
                roi = ((row['total_revenue'] - row['total_spend']) / row['total_spend'] * 100) \
                    if row['total_spend'] > 0 else 0
                
                analysis = {
                    "campaign_id": row['campaign_id'],
                    "campaign_name": row['campaign_name'],
                    "total_spend": round(row['total_spend'], 2),
                    "total_revenue": round(row['total_revenue'], 2),
                    "net_profit": round(row['total_revenue'] - row['total_spend'], 2),
                    "roi_percent": round(roi, 2),
                    "roas": round(row['avg_roas'], 2),
                    "analysis_period": {
                        "start_date": row['start_date'].isoformat() if row['start_date'] else None,
                        "end_date": row['end_date'].isoformat() if row['end_date'] else None,
                        "periods": row['periods']
                    },
                    "efficiency_rating": "high" if roi > 100 else "medium" if roi > 50 else "low"
                }
                roi_analysis.append(analysis)
            
            return {
                "roi_analysis": roi_analysis,
                "summary": {
                    "total_campaigns": len(roi_analysis),
                    "average_roi": round(sum(a['roi_percent'] for a in roi_analysis) / len(roi_analysis), 2) if roi_analysis else 0,
                    "total_investment": round(sum(a['total_spend'] for a in roi_analysis), 2),
                    "total_return": round(sum(a['total_revenue'] for a in roi_analysis), 2),
                    "net_profit": round(sum(a['net_profit'] for a in roi_analysis), 2)
                }
            }
            
        except Exception as e:
            logger.error(f"Error calculating ROI: {str(e)}")
            return {"error": str(e)}
    
    async def analyze_content_performance(self) -> Dict:
        """
        Analyze content performance across channels.
        
        Returns:
            Content performance analysis
        """
        logger.info("Analyzing content performance")
        
        # Note: This assumes a content performance table exists
        # Adjust query based on actual schema
        
        query = """
        SELECT 
            content_type,
            channel,
            COUNT(*) as content_count,
            SUM(impressions) as total_impressions,
            SUM(clicks) as total_clicks,
            AVG(click_through_rate) as avg_ctr,
            AVG(engagement_rate) as avg_engagement,
            SUM(conversions) as total_conversions
        FROM ANALYTICS.FACT_CONTENT_PERFORMANCE  -- Adjust table name as needed
        WHERE reporting_date >= DATEADD(month, -3, CURRENT_DATE())
        GROUP BY content_type, channel
        ORDER BY total_conversions DESC
        """
        
        try:
            results = await self.query_tool.execute_query(query)
            
            # Analyze content performance
            analysis = {
                "content_performance": results,
                "top_performing_content": max(results, key=lambda x: x.get('total_conversions', 0)) if results else {},
                "recommendations": []
            }
            
            # Generate recommendations
            for row in results:
                if row.get('avg_ctr', 0) < 0.02:
                    analysis["recommendations"].append({
                        "content_type": row['content_type'],
                        "channel": row['channel'],
                        "issue": f"Low CTR ({row.get('avg_ctr', 0):.2%})",
                        "action": "Optimize headlines and call-to-action"
                    })
                
                if row.get('avg_engagement', 0) < 0.01:
                    analysis["recommendations"].append({
                        "content_type": row['content_type'],
                        "channel": row['channel'],
                        "issue": f"Low engagement ({row.get('avg_engagement', 0):.2%})",
                        "action": "Improve content relevance and quality"
                    })
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing content performance: {str(e)}")
            return {"error": str(e)}
    
    async def generate_content_ideas(self, theme: str = None) -> List[Dict]:
        """
        Generate content ideas based on performance data.
        
        Args:
            theme: Optional content theme
            
        Returns:
            List of content ideas
        """
        logger.info(f"Generating content ideas for theme: {theme or 'general'}")
        
        # Get top performing content for inspiration
        query = """
        SELECT 
            content_title,
            content_type,
            channel,
            topic,
            performance_score
        FROM ANALYTICS.VW_CONTENT_PERFORMANCE  -- Adjust view name as needed
        WHERE performance_score > 8
        ORDER BY performance_score DESC
        LIMIT 10
        """
        
        try:
            top_content = await self.query_tool.execute_query(query)
            
            # Use LLM to generate new ideas based on top performers
            ideas_prompt = f"""
            Based on these top-performing content pieces, generate 5 new content ideas.
            Theme: {theme or 'sales and marketing'}
            
            Top performing content:
            {json.dumps(top_content, indent=2)}
            
            Generate ideas in JSON format with fields: title, content_type, channel, target_audience, key_message, expected_impact.
            JSON:
            """
            
            ideas_response = await self.llm.agenerate([ideas_prompt])
            ideas = json.loads(ideas_response.generations[0][0].text)
            
            return ideas
            
        except Exception as e:
            logger.error(f"Error generating content ideas: {str(e)}")
            return []
    
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
            "active_alerts": len(self.active_alerts),
            "last_alert": self.active_alerts[-1]["timestamp"] if self.active_alerts else None,
            "memory_size": len(self.memory.chat_memory.messages) if hasattr(self.memory.chat_memory, 'messages') else 0,
            "tools_available": [tool.name for tool in self._get_role_specific_tools()]
        }


# Factory function for creating marketing agents
def create_marketing_agent(
    agent_id: str,
    role: str = "campaign_analyst",
    **kwargs
) -> MarketingAgent:
    """
    Factory function to create marketing agents.
    
    Args:
        agent_id: Unique agent identifier
        role: Agent role (campaign_analyst, customer_acquisition, roi_optimization, content_strategy)
        **kwargs: Additional agent parameters
        
    Returns:
        Configured MarketingAgent instance
    """
    role_enum = MarketingAgentRole(role.lower())
    return MarketingAgent(agent_id=agent_id, role=role_enum, **kwargs)