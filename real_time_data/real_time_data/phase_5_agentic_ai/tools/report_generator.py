"""
Report Generator Tool - Tool for generating reports and visualizations.
Creates PDF, Excel, and HTML reports with charts and insights.
"""

import json
import logging
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, date, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
import asyncio
import pandas as pd
import numpy as np
from io import BytesIO, StringIO
import base64

# Try to import report generation libraries
try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter, A4
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    HAS_REPORTLAB = True
except ImportError:
    HAS_REPORTLAB = False
    logging.warning("reportlab not installed. PDF reports disabled.")

try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    logging.warning("matplotlib not installed. Charts disabled.")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ReportFormat(Enum):
    """Report output formats."""
    PDF = "pdf"
    EXCEL = "excel"
    HTML = "html"
    CSV = "csv"
    JSON = "json"
    MARKDOWN = "markdown"


class ReportType(Enum):
    """Types of reports that can be generated."""
    SALES_PERFORMANCE = "sales_performance"
    CUSTOMER_ANALYSIS = "customer_analysis"
    MARKETING_CAMPAIGN = "marketing_campaign"
    ANOMALY_DETECTION = "anomaly_detection"
    EXECUTIVE_SUMMARY = "executive_summary"
    DAILY_DASHBOARD = "daily_dashboard"
    CUSTOM = "custom"


@dataclass
class ReportConfig:
    """Report configuration."""
    title: str
    report_type: ReportType
    format: ReportFormat
    timeframe: str  # e.g., "7d", "30d", "QTD", "YTD"
    filters: Dict[str, Any]
    include_charts: bool = True
    include_summary: bool = True
    include_recommendations: bool = True
    branding: Optional[Dict] = None


@dataclass
class ChartConfig:
    """Chart configuration."""
    chart_type: str  # "line", "bar", "pie", "scatter", "heatmap"
    title: str
    data: pd.DataFrame
    x_column: str
    y_columns: List[str]
    colors: Optional[List[str]] = None
    width: int = 800
    height: int = 400
    style: str = "default"


class ReportGenerator:
    """
    Tool for generating reports in various formats with charts and insights.
    """
    
    def __init__(
        self,
        config: Optional[Dict] = None,
        template_dir: Optional[str] = None,
        cache_reports: bool = True,
        cache_ttl: int = 3600  # 1 hour
    ):
        """
        Initialize Report Generator.
        
        Args:
            config: Report generation configuration
            template_dir: Directory for report templates
            cache_reports: Whether to cache generated reports
            cache_ttl: Cache time-to-live in seconds
        """
        self.config = config or {}
        self.template_dir = template_dir
        self.cache_reports = cache_reports
        self.cache_ttl = cache_ttl
        
        # Report cache
        self.report_cache = {}
        self.cache_timestamps = {}
        
        # Initialize templates
        self.templates = self._load_templates()
        
        logger.info(f"Report Generator initialized with cache: {cache_reports}")
    
    def _load_templates(self) -> Dict:
        """Load report templates."""
        # Default templates (in production, these would be loaded from files)
        return {
            ReportType.SALES_PERFORMANCE: {
                "title": "Sales Performance Report",
                "sections": ["executive_summary", "sales_trends", "top_products", "regional_analysis", "recommendations"],
                "default_format": ReportFormat.PDF
            },
            ReportType.CUSTOMER_ANALYSIS: {
                "title": "Customer Analysis Report",
                "sections": ["customer_segments", "lifetime_value", "churn_analysis", "acquisition_channels", "recommendations"],
                "default_format": ReportFormat.HTML
            },
            ReportType.MARKETING_CAMPAIGN: {
                "title": "Marketing Campaign Report",
                "sections": ["campaign_performance", "roi_analysis", "channel_effectiveness", "audience_insights", "recommendations"],
                "default_format": ReportFormat.EXCEL
            },
            ReportType.ANOMALY_DETECTION: {
                "title": "Anomaly Detection Report",
                "sections": ["detected_anomalies", "impact_analysis", "root_cause", "mitigation_plan", "prevention_recommendations"],
                "default_format": ReportFormat.PDF
            },
            ReportType.EXECUTIVE_SUMMARY: {
                "title": "Executive Summary Report",
                "sections": ["key_metrics", "performance_highlights", "risks_opportunities", "strategic_recommendations"],
                "default_format": ReportFormat.PDF
            }
        }
    
    async def generate_report(
        self,
        config: ReportConfig,
        data: Optional[Dict] = None,
        query_callback: Optional[callable] = None
    ) -> Dict:
        """
        Generate a report based on configuration.
        
        Args:
            config: Report configuration
            data: Pre-loaded data (optional)
            query_callback: Callback to fetch data if not provided
            
        Returns:
            Dictionary with report data and files
        """
        start_time = datetime.now()
        
        try:
            # Check cache
            cache_key = self._create_cache_key(config)
            if self.cache_reports and cache_key in self.report_cache:
                cached_time = self.cache_timestamps.get(cache_key)
                if cached_time and (datetime.now() - cached_time).seconds < self.cache_ttl:
                    logger.info(f"Using cached report: {config.title}")
                    return self.report_cache[cache_key]
            
            # Fetch data if not provided
            if data is None and query_callback:
                logger.info(f"Fetching data for report: {config.title}")
                data = await query_callback(config)
            elif data is None:
                data = {}
            
            # Generate report based on format
            if config.format == ReportFormat.PDF:
                report_result = await self._generate_pdf_report(config, data)
            elif config.format == ReportFormat.EXCEL:
                report_result = await self._generate_excel_report(config, data)
            elif config.format == ReportFormat.HTML:
                report_result = await self._generate_html_report(config, data)
            elif config.format == ReportFormat.CSV:
                report_result = await self._generate_csv_report(config, data)
            elif config.format == ReportFormat.JSON:
                report_result = await self._generate_json_report(config, data)
            elif config.format == ReportFormat.MARKDOWN:
                report_result = await self._generate_markdown_report(config, data)
            else:
                raise ValueError(f"Unsupported report format: {config.format}")
            
            # Add metadata
            report_result["metadata"] = {
                "title": config.title,
                "report_type": config.report_type.value,
                "format": config.format.value,
                "timeframe": config.timeframe,
                "generated_at": datetime.now().isoformat(),
                "generation_time": (datetime.now() - start_time).total_seconds()
            }
            
            # Cache report
            if self.cache_reports:
                self.report_cache[cache_key] = report_result
                self.cache_timestamps[cache_key] = datetime.now()
            
            logger.info(f"Report generated: {config.title} in {config.format.value} format")
            return report_result
            
        except Exception as e:
            logger.error(f"Error generating report: {str(e)}")
            raise
    
    def _create_cache_key(self, config: ReportConfig) -> str:
        """Create cache key from report configuration."""
        import hashlib
        config_str = json.dumps(asdict(config), sort_keys=True)
        return f"report_{hashlib.md5(config_str.encode()).hexdigest()[:16]}"
    
    async def _generate_pdf_report(self, config: ReportConfig, data: Dict) -> Dict:
        """Generate PDF report."""
        if not HAS_REPORTLAB:
            raise ImportError("reportlab is required for PDF reports")
        
        try:
            # Create PDF document
            buffer = BytesIO()
            doc = SimpleDocTemplate(
                buffer,
                pagesize=letter,
                rightMargin=72,
                leftMargin=72,
                topMargin=72,
                bottomMargin=72
            )
            
            # Get styles
            styles = getSampleStyleSheet()
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=styles['Heading1'],
                fontSize=24,
                spaceAfter=30
            )
            heading_style = ParagraphStyle(
                'CustomHeading',
                parent=styles['Heading2'],
                fontSize=16,
                spaceAfter=12
            )
            normal_style = styles['Normal']
            
            # Build story (content)
            story = []
            
            # Title
            story.append(Paragraph(config.title, title_style))
            story.append(Spacer(1, 12))
            
            # Metadata
            metadata_text = f"""
            Report Type: {config.report_type.value.replace('_', ' ').title()}<br/>
            Timeframe: {config.timeframe}<br/>
            Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br/>
            """
            story.append(Paragraph(metadata_text, normal_style))
            story.append(Spacer(1, 24))
            
            # Process sections based on report type
            template = self.templates.get(config.report_type, {})
            sections = template.get("sections", [])
            
            for section in sections:
                story.append(Paragraph(section.replace('_', ' ').title(), heading_style))
                
                # Generate section content
                section_content = await self._generate_section_content(section, data, config)
                story.append(Paragraph(section_content, normal_style))
                
                # Add charts if enabled
                if config.include_charts and section in ["sales_trends", "customer_segments", "campaign_performance"]:
                    chart_data = self._extract_chart_data(section, data)
                    if chart_data is not None:
                        # In production, add chart image to PDF
                        pass
                
                story.append(Spacer(1, 12))
            
            # Recommendations section
            if config.include_recommendations:
                story.append(Paragraph("Recommendations", heading_style))
                recommendations = await self._generate_recommendations(data, config)
                for rec in recommendations:
                    story.append(Paragraph(f"• {rec}", normal_style))
                    story.append(Spacer(1, 6))
            
            # Build PDF
            doc.build(story)
            
            # Get PDF bytes
            pdf_bytes = buffer.getvalue()
            buffer.close()
            
            return {
                "content": base64.b64encode(pdf_bytes).decode('utf-8'),
                "filename": f"{config.title.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.pdf",
                "content_type": "application/pdf",
                "size_bytes": len(pdf_bytes)
            }
            
        except Exception as e:
            logger.error(f"Error generating PDF report: {str(e)}")
            raise
    
    async def _generate_excel_report(self, config: ReportConfig, data: Dict) -> Dict:
        """Generate Excel report with multiple sheets."""
        try:
            # Create Excel writer
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # Summary sheet
                summary_data = await self._create_summary_data(data, config)
                if summary_data:
                    summary_df = pd.DataFrame(summary_data)
                    summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
                # Data sheets based on report type
                if config.report_type == ReportType.SALES_PERFORMANCE:
                    # Sales trends
                    if 'sales_trends' in data:
                        sales_df = pd.DataFrame(data['sales_trends'])
                        sales_df.to_excel(writer, sheet_name='Sales Trends', index=False)
                    
                    # Top products
                    if 'top_products' in data:
                        products_df = pd.DataFrame(data['top_products'])
                        products_df.to_excel(writer, sheet_name='Top Products', index=False)
                    
                    # Regional analysis
                    if 'regional_analysis' in data:
                        regional_df = pd.DataFrame(data['regional_analysis'])
                        regional_df.to_excel(writer, sheet_name='Regional Analysis', index=False)
                
                elif config.report_type == ReportType.CUSTOMER_ANALYSIS:
                    # Customer segments
                    if 'customer_segments' in data:
                        segments_df = pd.DataFrame(data['customer_segments'])
                        segments_df.to_excel(writer, sheet_name='Customer Segments', index=False)
                    
                    # Lifetime value
                    if 'lifetime_value' in data:
                        ltv_df = pd.DataFrame(data['lifetime_value'])
                        ltv_df.to_excel(writer, sheet_name='Lifetime Value', index=False)
                
                # Recommendations sheet
                if config.include_recommendations:
                    recommendations = await self._generate_recommendations(data, config)
                    rec_df = pd.DataFrame({
                        'Priority': ['High', 'Medium', 'Low'][:len(recommendations)],
                        'Recommendation': recommendations,
                        'Expected Impact': ['Significant', 'Moderate', 'Minor'][:len(recommendations)],
                        'Timeline': ['Immediate', '30 days', '90 days'][:len(recommendations)]
                    })
                    rec_df.to_excel(writer, sheet_name='Recommendations', index=False)
            
            # Get Excel bytes
            excel_bytes = output.getvalue()
            output.close()
            
            return {
                "content": base64.b64encode(excel_bytes).decode('utf-8'),
                "filename": f"{config.title.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.xlsx",
                "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "size_bytes": len(excel_bytes)
            }
            
        except Exception as e:
            logger.error(f"Error generating Excel report: {str(e)}")
            raise
    
    async def _generate_html_report(self, config: ReportConfig, data: Dict) -> Dict:
        """Generate HTML report."""
        try:
            # Start HTML document
            html_parts = []
            
            # HTML header
            html_parts.append("""
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>{title}</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; color: #333; }}
                    .container {{ max-width: 1200px; margin: 0 auto; }}
                    .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; margin-bottom: 30px; }}
                    .header h1 {{ margin: 0; font-size: 2.5em; }}
                    .metadata {{ margin-top: 15px; font-size: 0.9em; opacity: 0.9; }}
                    .section {{ margin-bottom: 30px; padding: 20px; background: #f8f9fa; border-radius: 8px; }}
                    .section h2 {{ color: #495057; border-bottom: 2px solid #dee2e6; padding-bottom: 10px; margin-top: 0; }}
                    .data-table {{ width: 100%; border-collapse: collapse; margin: 15px 0; }}
                    .data-table th, .data-table td {{ padding: 12px 15px; text-align: left; border-bottom: 1px solid #dee2e6; }}
                    .data-table th {{ background-color: #e9ecef; font-weight: 600; }}
                    .data-table tr:hover {{ background-color: #f1f3f4; }}
                    .metric-card {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin: 10px 0; }}
                    .metric-value {{ font-size: 2em; font-weight: bold; color: #667eea; }}
                    .metric-label {{ color: #6c757d; font-size: 0.9em; }}
                    .recommendation {{ background: #e7f4e4; padding: 15px; border-left: 4px solid #28a745; margin: 10px 0; border-radius: 4px; }}
                    .chart-container {{ margin: 20px 0; }}
                    .footer {{ margin-top: 40px; padding-top: 20px; border-top: 1px solid #dee2e6; color: #6c757d; font-size: 0.9em; text-align: center; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h1>{title}</h1>
                        <div class="metadata">
                            <p><strong>Report Type:</strong> {report_type} • <strong>Timeframe:</strong> {timeframe} • <strong>Generated:</strong> {generated_at}</p>
                        </div>
                    </div>
            """.format(
                title=config.title,
                report_type=config.report_type.value.replace('_', ' ').title(),
                timeframe=config.timeframe,
                generated_at=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))
            
            # Summary section
            if config.include_summary:
                html_parts.append("""
                <div class="section">
                    <h2>Executive Summary</h2>
                    <div class="metric-cards">
                """)
                
                summary_metrics = await self._extract_summary_metrics(data, config)
                for metric in summary_metrics:
                    html_parts.append(f"""
                    <div class="metric-card">
                        <div class="metric-value">{metric['value']}</div>
                        <div class="metric-label">{metric['label']}</div>
                        {f"<div class='metric-trend' style='color: {metric['trend_color']};'>{metric['trend']}</div>" if 'trend' in metric else ''}
                    </div>
                    """)
                
                html_parts.append("""
                    </div>
                </div>
                """)
            
            # Data sections
            template = self.templates.get(config.report_type, {})
            sections = template.get("sections", [])
            
            for section in sections:
                if section == "executive_summary":
                    continue  # Already handled
                
                html_parts.append(f"""
                <div class="section">
                    <h2>{section.replace('_', ' ').title()}</h2>
                """)
                
                # Add section content
                section_content = await self._generate_section_content(section, data, config, format='html')
                html_parts.append(section_content)
                
                # Add chart if available and enabled
                if config.include_charts and section in ["sales_trends", "customer_segments", "campaign_performance"]:
                    chart_html = await self._generate_chart_html(section, data)
                    if chart_html:
                        html_parts.append(f"""
                        <div class="chart-container">
                            {chart_html}
                        </div>
                        """)
                
                html_parts.append("</div>")
            
            # Recommendations section
            if config.include_recommendations:
                html_parts.append("""
                <div class="section">
                    <h2>Recommendations</h2>
                """)
                
                recommendations = await self._generate_recommendations(data, config)
                for rec in recommendations:
                    html_parts.append(f"""
                    <div class="recommendation">
                        <strong>Recommendation:</strong> {rec}
                    </div>
                    """)
                
                html_parts.append("</div>")
            
            # Footer
            html_parts.append(f"""
                <div class="footer">
                    <p>Generated by AI Agent System • {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                    <p>Confidential - For internal use only</p>
                </div>
                </div>
            </body>
            </html>
            """)
            
            html_content = "\n".join(html_parts)
            
            return {
                "content": html_content,
                "filename": f"{config.title.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.html",
                "content_type": "text/html",
                "size_bytes": len(html_content.encode('utf-8'))
            }
            
        except Exception as e:
            logger.error(f"Error generating HTML report: {str(e)}")
            raise
    
    async def _generate_csv_report(self, config: ReportConfig, data: Dict) -> Dict:
        """Generate CSV report."""
        try:
            csv_parts = []
            
            # Header
            csv_parts.append(f"# {config.title}")
            csv_parts.append(f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            csv_parts.append(f"# Timeframe: {config.timeframe}")
            csv_parts.append("")
            
            # Process data sections
            for section_name, section_data in data.items():
                if isinstance(section_data, list) and section_data:
                    csv_parts.append(f"## {section_name.replace('_', ' ').title()}")
                    
                    # Convert to DataFrame for consistent CSV formatting
                    df = pd.DataFrame(section_data)
                    csv_buffer = StringIO()
                    df.to_csv(csv_buffer, index=False)
                    csv_parts.append(csv_buffer.getvalue())
                    csv_parts.append("")
            
            csv_content = "\n".join(csv_parts)
            
            return {
                "content": csv_content,
                "filename": f"{config.title.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.csv",
                "content_type": "text/csv",
                "size_bytes": len(csv_content.encode('utf-8'))
            }
            
        except Exception as e:
            logger.error(f"Error generating CSV report: {str(e)}")
            raise
    
    async def _generate_json_report(self, config: ReportConfig, data: Dict) -> Dict:
        """Generate JSON report."""
        try:
            report_data = {
                "metadata": {
                    "title": config.title,
                    "report_type": config.report_type.value,
                    "timeframe": config.timeframe,
                    "generated_at": datetime.now().isoformat(),
                    "filters": config.filters
                },
                "data": data,
                "summary": await self._create_summary_data(data, config),
                "recommendations": await self._generate_recommendations(data, config) if config.include_recommendations else []
            }
            
            json_content = json.dumps(report_data, indent=2, default=str)
            
            return {
                "content": json_content,
                "filename": f"{config.title.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.json",
                "content_type": "application/json",
                "size_bytes": len(json_content.encode('utf-8'))
            }
            
        except Exception as e:
            logger.error(f"Error generating JSON report: {str(e)}")
            raise
    
    async def _generate_markdown_report(self, config: ReportConfig, data: Dict) -> Dict:
        """Generate Markdown report."""
        try:
            md_parts = []
            
            # Title
            md_parts.append(f"# {config.title}")
            md_parts.append("")
            
            # Metadata
            md_parts.append(f"**Report Type:** {config.report_type.value.replace('_', ' ').title()}  ")
            md_parts.append(f"**Timeframe:** {config.timeframe}  ")
            md_parts.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            md_parts.append("")
            md_parts.append("---")
            md_parts.append("")
            
            # Summary
            if config.include_summary:
                md_parts.append("## Executive Summary")
                md_parts.append("")
                
                summary_metrics = await self._extract_summary_metrics(data, config)
                for metric in summary_metrics:
                    trend_marker = f" ({metric['trend']})" if 'trend' in metric else ""
                    md_parts.append(f"- **{metric['label']}:** {metric['value']}{trend_marker}")
                
                md_parts.append("")
            
            # Data sections
            template = self.templates.get(config.report_type, {})
            sections = template.get("sections", [])
            
            for section in sections:
                if section == "executive_summary":
                    continue
                
                md_parts.append(f"## {section.replace('_', ' ').title()}")
                md_parts.append("")
                
                section_content = await self._generate_section_content(section, data, config, format='markdown')
                md_parts.append(section_content)
                md_parts.append("")
                
                # Add table if data available
                if section in data and isinstance(data[section], list) and data[section]:
                    df = pd.DataFrame(data[section])
                    md_parts.append(df.to_markdown(index=False))
                    md_parts.append("")
            
            # Recommendations
            if config.include_recommendations:
                md_parts.append("## Recommendations")
                md_parts.append("")
                
                recommendations = await self._generate_recommendations(data, config)
                for i, rec in enumerate(recommendations, 1):
                    md_parts.append(f"{i}. {rec}")
                
                md_parts.append("")
            
            # Footer
            md_parts.append("---")
            md_parts.append(f"*Generated by AI Agent System • {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
            
            md_content = "\n".join(md_parts)
            
            return {
                "content": md_content,
                "filename": f"{config.title.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.md",
                "content_type": "text/markdown",
                "size_bytes": len(md_content.encode('utf-8'))
            }
            
        except Exception as e:
            logger.error(f"Error generating Markdown report: {str(e)}")
            raise
    
    async def _generate_section_content(
        self,
        section: str,
        data: Dict,
        config: ReportConfig,
        format: str = 'text'
    ) -> str:
        """Generate content for a specific section."""
        section_data = data.get(section, {})
        
        if format == 'html':
            if section == 'sales_trends':
                return f"""
                <p>Sales analysis for the {config.timeframe} timeframe shows key trends and patterns.</p>
                <p>Total revenue: ${section_data.get('total_revenue', 0):,.2f}</p>
                <p>Transaction count: {section_data.get('transaction_count', 0):,}</p>
                """
            elif section == 'customer_segments':
                return """
                <p>Customer segmentation analysis identifies key customer groups and their characteristics.</p>
                """
            else:
                return f"<p>Analysis of {section.replace('_', ' ')}.</p>"
        
        elif format == 'markdown':
            return f"Analysis of {section.replace('_', ' ')} for the {config.timeframe} timeframe."
        
        else:  # text
            return f"Analysis of {section.replace('_', ' ')} for the {config.timeframe} timeframe."
    
    async def _generate_recommendations(self, data: Dict, config: ReportConfig) -> List[str]:
        """Generate recommendations based on data analysis."""
        recommendations = []
        
        if config.report_type == ReportType.SALES_PERFORMANCE:
            if 'sales_trends' in data:
                trends = data['sales_trends']
                if isinstance(trends, dict) and trends.get('growth_rate', 0) < 0:
                    recommendations.append("Implement promotional campaigns to boost sales growth")
                if isinstance(trends, dict) and trends.get('avg_order_value', 0) < 100:
                    recommendations.append("Focus on up-selling and cross-selling to increase average order value")
            
            if 'top_products' in data and isinstance(data['top_products'], list):
                recommendations.append(f"Increase inventory for top-performing products")
        
        elif config.report_type == ReportType.CUSTOMER_ANALYSIS:
            recommendations.append("Implement targeted retention campaigns for high-value customer segments")
            recommendations.append("Improve onboarding process for new customers")
        
        elif config.report_type == ReportType.MARKETING_CAMPAIGN:
            recommendations.append("Reallocate budget to highest-performing marketing channels")
            recommendations.append("Optimize ad creatives based on engagement metrics")
        
        # Add general recommendations
        recommendations.append("Monitor key metrics weekly and adjust strategies accordingly")
        recommendations.append("Conduct A/B testing for optimization opportunities")
        
        return recommendations[:5]  # Limit to top 5
    
    async def _extract_summary_metrics(self, data: Dict, config: ReportConfig) -> List[Dict]:
        """Extract summary metrics from data."""
        metrics = []
        
        if config.report_type == ReportType.SALES_PERFORMANCE:
            if 'sales_trends' in data and isinstance(data['sales_trends'], dict):
                trends = data['sales_trends']
                metrics.extend([
                    {
                        "label": "Total Revenue",
                        "value": f"${trends.get('total_revenue', 0):,.2f}",
                        "trend": f"+{trends.get('growth_rate', 0):.1f}%",
                        "trend_color": "#28a745" if trends.get('growth_rate', 0) > 0 else "#dc3545"
                    },
                    {
                        "label": "Transactions",
                        "value": f"{trends.get('transaction_count', 0):,}"
                    },
                    {
                        "label": "Avg Order Value",
                        "value": f"${trends.get('avg_order_value', 0):,.2f}"
                    }
                ])
        
        elif config.report_type == ReportType.CUSTOMER_ANALYSIS:
            metrics.extend([
                {"label": "Total Customers", "value": "1,234"},
                {"label": "Avg Lifetime Value", "value": "$456.78"},
                {"label": "Retention Rate", "value": "85.2%"}
            ])
        
        return metrics
    
    async def _create_summary_data(self, data: Dict, config: ReportConfig) -> List[Dict]:
        """Create summary data for reports."""
        summary = []
        
        # Extract key metrics based on report type
        if config.report_type == ReportType.SALES_PERFORMANCE:
            summary = [
                {"metric": "Total Revenue", "value": "$123,456", "change": "+12.3%"},
                {"metric": "Units Sold", "value": "5,678", "change": "+8.5%"},
                {"metric": "Avg Order Value", "value": "$98.76", "change": "+3.2%"},
                {"metric": "Conversion Rate", "value": "2.34%", "change": "+0.4%"}
            ]
        
        return summary
    
    def _extract_chart_data(self, section: str, data: Dict) -> Optional[pd.DataFrame]:
        """Extract data for charts."""
        if section == "sales_trends" and 'daily_sales' in data:
            return pd.DataFrame(data['daily_sales'])
        elif section == "customer_segments" and 'segment_data' in data:
            return pd.DataFrame(data['segment_data'])
        
        return None
    
    async def _generate_chart_html(self, section: str, data: Dict) -> Optional[str]:
        """Generate HTML for charts."""
        if not HAS_MATPLOTLIB:
            return None
        
        try:
            chart_data = self._extract_chart_data(section, data)
            if chart_data is None or chart_data.empty:
                return None
            
            # Create chart
            plt.figure(figsize=(10, 6))
            
            if section == "sales_trends" and 'date' in chart_data.columns and 'revenue' in chart_data.columns:
                plt.plot(chart_data['date'], chart_data['revenue'], marker='o', linewidth=2)
                plt.title('Sales Trends')
                plt.xlabel('Date')
                plt.ylabel('Revenue ($)')
                plt.grid(True, alpha=0.3)
            
            elif section == "customer_segments" and 'segment' in chart_data.columns and 'value' in chart_data.columns:
                plt.bar(chart_data['segment'], chart_data['value'])
                plt.title('Customer Segments')
                plt.xlabel('Segment')
                plt.ylabel('Value')
                plt.xticks(rotation=45)
            
            # Save chart to buffer
            buffer = BytesIO()
            plt.savefig(buffer, format='png', bbox_inches='tight', dpi=100)
            plt.close()
            
            # Convert to base64
            chart_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
            buffer.close()
            
            return f'<img src="data:image/png;base64,{chart_base64}" alt="Chart" style="max-width: 100%;">'
            
        except Exception as e:
            logger.warning(f"Failed to generate chart: {str(e)}")
            return None
    
    async def generate_chart(
        self,
        chart_config: ChartConfig,
        output_format: str = "png"
    ) -> Dict:
        """
        Generate a standalone chart.
        
        Args:
            chart_config: Chart configuration
            output_format: Output format (png, svg, html)
            
        Returns:
            Dictionary with chart data
        """
        if not HAS_MATPLOTLIB:
            raise ImportError("matplotlib is required for chart generation")
        
        try:
            # Create chart based on type
            plt.figure(figsize=(chart_config.width/100, chart_config.height/100))
            
            if chart_config.chart_type == "line":
                for y_col in chart_config.y_columns:
                    plt.plot(chart_config.data[chart_config.x_column], 
                            chart_config.data[y_col], 
                            label=y_col, 
                            linewidth=2)
                plt.legend()
                
            elif chart_config.chart_type == "bar":
                x = np.arange(len(chart_config.data))
                width = 0.8 / len(chart_config.y_columns)
                
                for i, y_col in enumerate(chart_config.y_columns):
                    offset = width * i
                    plt.bar(x + offset, chart_config.data[y_col], 
                           width, label=y_col)
                
                plt.xticks(x + width * (len(chart_config.y_columns) - 1) / 2, 
                          chart_config.data[chart_config.x_column])
                plt.legend()
                
            elif chart_config.chart_type == "pie":
                plt.pie(chart_config.data[chart_config.y_columns[0]], 
                       labels=chart_config.data[chart_config.x_column],
                       autopct='%1.1f%%')
            
            # Set title and labels
            plt.title(chart_config.title)
            plt.xlabel(chart_config.x_column.replace('_', ' ').title())
            if chart_config.y_columns:
                plt.ylabel(chart_config.y_columns[0].replace('_', ' ').title())
            
            # Apply style
            if chart_config.style == "dark":
                plt.style.use('dark_background')
            
            # Generate output
            if output_format == "png":
                buffer = BytesIO()
                plt.savefig(buffer, format='png', bbox_inches='tight', dpi=150)
                plt.close()
                
                png_bytes = buffer.getvalue()
                buffer.close()
                
                return {
                    "content": base64.b64encode(png_bytes).decode('utf-8'),
                    "content_type": "image/png",
                    "size_bytes": len(png_bytes)
                }
                
            else:
                raise ValueError(f"Unsupported chart format: {output_format}")
            
        except Exception as e:
            logger.error(f"Error generating chart: {str(e)}")
            raise
    
    def clear_cache(self, pattern: Optional[str] = None):
        """Clear report cache."""
        if pattern:
            keys_to_remove = [
                key for key in self.report_cache.keys()
                if pattern in str(key)
            ]
            for key in keys_to_remove:
                del self.report_cache[key]
                del self.cache_timestamps[key]
            logger.info(f"Cleared {len(keys_to_remove)} cached reports matching pattern: {pattern}")
        else:
            self.report_cache.clear()
            self.cache_timestamps.clear()
            logger.info("Cleared all cached reports")
    
    def get_tool_status(self) -> Dict:
        """Get tool status and statistics."""
        return {
            "cached_reports": len(self.report_cache),
            "templates_loaded": len(self.templates),
            "cache_enabled": self.cache_reports,
            "cache_ttl": self.cache_ttl,
            "libraries_available": {
                "reportlab": HAS_REPORTLAB,
                "matplotlib": HAS_MATPLOTLIB
            }
        }