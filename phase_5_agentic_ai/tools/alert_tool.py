"""
Alert Tool - Tool for sending alerts and notifications.
Supports multiple channels: email, Slack, webhook, and in-app notifications.
"""

import json
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
import asyncio
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests
from typing import Callable

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertChannel(Enum):
    """Alert delivery channels."""
    EMAIL = "email"
    SLACK = "slack"
    WEBHOOK = "webhook"
    IN_APP = "in_app"
    LOG = "log"
    ALL = "all"


@dataclass
class Alert:
    """Alert data structure."""
    alert_id: str
    level: AlertLevel
    title: str
    message: str
    category: str
    source: str
    timestamp: datetime
    metadata: Dict[str, Any]
    acknowledged: bool = False
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[datetime] = None


@dataclass
class AlertRule:
    """Alert rule for automatic alerting."""
    rule_id: str
    name: str
    condition: Callable[[Dict], bool]
    channels: List[AlertChannel]
    level: AlertLevel
    template: str
    cooldown_minutes: int = 5
    last_triggered: Optional[datetime] = None


class AlertTool:
    """
    Tool for sending alerts through multiple channels with rate limiting,
    deduplication, and alert management.
    """
    
    def __init__(
        self,
        config: Optional[Dict] = None,
        max_alerts_per_minute: int = 60,
        alert_history_size: int = 1000
    ):
        """
        Initialize Alert Tool.
        
        Args:
            config: Alert configuration for different channels
            max_alerts_per_minute: Rate limiting threshold
            alert_history_size: Maximum number of alerts to keep in history
        """
        self.config = config or self._load_default_config()
        self.max_alerts_per_minute = max_alerts_per_minute
        self.alert_history_size = alert_history_size
        
        # Alert tracking
        self.alerts_sent = []
        self.alert_history = []
        self.alert_rules = []
        
        # Rate limiting
        self.alert_timestamps = []
        
        # Initialize channels
        self._initialize_channels()
        
        logger.info(f"Alert Tool initialized with rate limit: {max_alerts_per_minute}/minute")
    
    def _load_default_config(self) -> Dict:
        """Load default configuration."""
        return {
            "email": {
                "enabled": False,
                "smtp_server": "smtp.gmail.com",
                "smtp_port": 587,
                "username": "your_email@gmail.com",
                "password": "your_password",
                "from_address": "alerts@yourcompany.com",
                "recipients": ["admin@yourcompany.com"]
            },
            "slack": {
                "enabled": False,
                "webhook_url": "https://hooks.slack.com/services/...",
                "channel": "#alerts",
                "username": "Alert Bot",
                "icon_emoji": ":warning:"
            },
            "webhook": {
                "enabled": False,
                "url": "https://your-webhook-endpoint.com/alerts",
                "headers": {"Content-Type": "application/json"},
                "timeout": 5
            },
            "in_app": {
                "enabled": True,
                "storage": "memory"  # or "database"
            }
        }
    
    def _initialize_channels(self):
        """Initialize alert channels."""
        self.channels = {
            AlertChannel.EMAIL: self._send_email_alert,
            AlertChannel.SLACK: self._send_slack_alert,
            AlertChannel.WEBHOOK: self._send_webhook_alert,
            AlertChannel.IN_APP: self._store_in_app_alert,
            AlertChannel.LOG: self._log_alert
        }
    
    async def send_alert(
        self,
        message: str,
        level: Union[str, AlertLevel] = AlertLevel.INFO,
        category: str = "general",
        source: str = "system",
        channels: List[Union[str, AlertChannel]] = None,
        metadata: Optional[Dict] = None,
        deduplicate: bool = True,
        deduplication_window: int = 300  # 5 minutes
    ) -> Dict:
        """
        Send an alert through specified channels.
        
        Args:
            message: Alert message
            level: Alert severity level
            category: Alert category for filtering
            source: Source of the alert
            channels: List of channels to send through
            metadata: Additional alert metadata
            deduplicate: Whether to deduplicate similar alerts
            deduplication_window: Deduplication window in seconds
            
        Returns:
            Dictionary with alert sending results
        """
        # Validate rate limiting
        if not self._check_rate_limit():
            logger.warning("Rate limit exceeded for alerts")
            return {
                "success": False,
                "error": "Rate limit exceeded",
                "alert_id": None
            }
        
        # Parse level
        if isinstance(level, str):
            level = AlertLevel(level.lower())
        
        # Parse channels
        if channels is None:
            channels = [AlertChannel.LOG]  # Default to log only
        
        parsed_channels = []
        for channel in channels:
            if isinstance(channel, str):
                parsed_channels.append(AlertChannel(channel.lower()))
            else:
                parsed_channels.append(channel)
        
        # Check for duplicates
        alert_id = self._generate_alert_id(message, level, category, source)
        
        if deduplicate and self._is_duplicate_alert(alert_id, deduplication_window):
            logger.info(f"Duplicate alert suppressed: {alert_id}")
            return {
                "success": False,
                "error": "Duplicate alert",
                "alert_id": alert_id,
                "suppressed": True
            }
        
        # Create alert object
        alert = Alert(
            alert_id=alert_id,
            level=level,
            title=self._generate_alert_title(level, category),
            message=message,
            category=category,
            source=source,
            timestamp=datetime.now(),
            metadata=metadata or {}
        )
        
        # Send through channels
        results = {}
        for channel in parsed_channels:
            if channel == AlertChannel.ALL:
                # Send through all enabled channels
                for ch_name, ch_func in self.channels.items():
                    if ch_name != AlertChannel.ALL:
                        result = await ch_func(alert)
                        results[ch_name.value] = result
            else:
                channel_func = self.channels.get(channel)
                if channel_func:
                    result = await channel_func(alert)
                    results[channel.value] = result
                else:
                    results[channel.value] = {"success": False, "error": "Channel not implemented"}
        
        # Track alert
        self._track_alert(alert, results)
        
        # Update rate limiting
        self.alert_timestamps.append(datetime.now())
        self._cleanup_old_timestamps()
        
        return {
            "success": any(r.get("success", False) for r in results.values()),
            "alert_id": alert_id,
            "channels": results,
            "alert": asdict(alert)
        }
    
    def _generate_alert_id(self, message: str, level: AlertLevel, category: str, source: str) -> str:
        """Generate unique alert ID."""
        import hashlib
        content = f"{message}:{level.value}:{category}:{source}:{datetime.now().strftime('%Y%m%d%H')}"
        return f"alert_{hashlib.md5(content.encode()).hexdigest()[:16]}"
    
    def _generate_alert_title(self, level: AlertLevel, category: str) -> str:
        """Generate alert title based on level and category."""
        level_icons = {
            AlertLevel.INFO: "ℹ️",
            AlertLevel.WARNING: "⚠️",
            AlertLevel.ERROR: "❌",
            AlertLevel.CRITICAL: "🔥"
        }
        
        icon = level_icons.get(level, "📢")
        return f"{icon} {level.value.upper()}: {category.replace('_', ' ').title()}"
    
    def _check_rate_limit(self) -> bool:
        """Check if rate limit is exceeded."""
        now = datetime.now()
        minute_ago = now - timedelta(minutes=1)
        
        recent_alerts = [t for t in self.alert_timestamps if t > minute_ago]
        return len(recent_alerts) < self.max_alerts_per_minute
    
    def _cleanup_old_timestamps(self):
        """Clean up old rate limiting timestamps."""
        hour_ago = datetime.now() - timedelta(hours=1)
        self.alert_timestamps = [t for t in self.alert_timestamps if t > hour_ago]
    
    def _is_duplicate_alert(self, alert_id: str, window_seconds: int) -> bool:
        """Check if alert is a duplicate within the time window."""
        window_start = datetime.now() - timedelta(seconds=window_seconds)
        
        for alert in self.alerts_sent:
            if alert.alert_id == alert_id and alert.timestamp > window_start:
                return True
        
        return False
    
    def _track_alert(self, alert: Alert, results: Dict):
        """Track sent alert."""
        self.alerts_sent.append(alert)
        
        # Keep history limited
        if len(self.alerts_sent) > self.alert_history_size:
            self.alerts_sent = self.alerts_sent[-self.alert_history_size:]
        
        # Add to history
        history_entry = {
            "alert": asdict(alert),
            "results": results,
            "sent_at": datetime.now().isoformat()
        }
        self.alert_history.append(history_entry)
        
        if len(self.alert_history) > self.alert_history_size:
            self.alert_history = self.alert_history[-self.alert_history_size:]
    
    # Channel implementations
    
    async def _send_email_alert(self, alert: Alert) -> Dict:
        """Send alert via email."""
        email_config = self.config.get("email", {})
        
        if not email_config.get("enabled", False):
            return {"success": False, "error": "Email channel disabled"}
        
        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = email_config['from_address']
            msg['To'] = ", ".join(email_config['recipients'])
            msg['Subject'] = alert.title
            
            # Create HTML content
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    .alert {{ border: 2px solid #ccc; border-radius: 5px; padding: 20px; margin: 10px 0; }}
                    .info {{ border-color: #3498db; background-color: #ebf5fb; }}
                    .warning {{ border-color: #f39c12; background-color: #fef5e7; }}
                    .error {{ border-color: #e74c3c; background-color: #fdedec; }}
                    .critical {{ border-color: #c0392b; background-color: #fadbd8; }}
                    .metadata {{ background-color: #f8f9fa; padding: 10px; border-radius: 3px; margin-top: 10px; }}
                    .timestamp {{ color: #666; font-size: 0.9em; }}
                </style>
            </head>
            <body>
                <div class="alert {alert.level.value}">
                    <h2>{alert.title}</h2>
                    <p>{alert.message}</p>
                    <div class="metadata">
                        <p><strong>Category:</strong> {alert.category}</p>
                        <p><strong>Source:</strong> {alert.source}</p>
                        <p><strong>Alert ID:</strong> {alert.alert_id}</p>
                        <p class="timestamp"><strong>Time:</strong> {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')}</p>
                    </div>
                </div>
            </body>
            </html>
            """
            
            msg.attach(MIMEText(html_content, 'html'))
            
            # Send email
            with smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port']) as server:
                server.starttls()
                server.login(email_config['username'], email_config['password'])
                server.send_message(msg)
            
            logger.info(f"Email alert sent: {alert.alert_id}")
            return {"success": True, "channel": "email"}
            
        except Exception as e:
            logger.error(f"Failed to send email alert: {str(e)}")
            return {"success": False, "error": str(e), "channel": "email"}
    
    async def _send_slack_alert(self, alert: Alert) -> Dict:
        """Send alert via Slack."""
        slack_config = self.config.get("slack", {})
        
        if not slack_config.get("enabled", False):
            return {"success": False, "error": "Slack channel disabled"}
        
        try:
            # Create Slack message
            level_colors = {
                AlertLevel.INFO: "#3498db",
                AlertLevel.WARNING: "#f39c12",
                AlertLevel.ERROR: "#e74c3c",
                AlertLevel.CRITICAL: "#c0392b"
            }
            
            slack_message = {
                "channel": slack_config.get("channel", "#alerts"),
                "username": slack_config.get("username", "Alert Bot"),
                "icon_emoji": slack_config.get("icon_emoji", ":warning:"),
                "attachments": [{
                    "color": level_colors.get(alert.level, "#cccccc"),
                    "title": alert.title,
                    "text": alert.message,
                    "fields": [
                        {"title": "Category", "value": alert.category, "short": True},
                        {"title": "Source", "value": alert.source, "short": True},
                        {"title": "Alert ID", "value": alert.alert_id, "short": True},
                        {"title": "Time", "value": alert.timestamp.strftime('%Y-%m-%d %H:%M:%S'), "short": True}
                    ],
                    "footer": "AI Agent System Alert",
                    "ts": int(alert.timestamp.timestamp())
                }]
            }
            
            # Add metadata if present
            if alert.metadata:
                metadata_text = "\n".join([f"{k}: {v}" for k, v in alert.metadata.items()])
                slack_message["attachments"][0]["fields"].append({
                    "title": "Metadata",
                    "value": metadata_text[:1000],  # Slack field value limit
                    "short": False
                })
            
            # Send to Slack
            response = requests.post(
                slack_config['webhook_url'],
                json=slack_message,
                timeout=5
            )
            
            if response.status_code == 200:
                logger.info(f"Slack alert sent: {alert.alert_id}")
                return {"success": True, "channel": "slack"}
            else:
                logger.error(f"Slack API error: {response.status_code} - {response.text}")
                return {"success": False, "error": f"Slack API error: {response.status_code}", "channel": "slack"}
            
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {str(e)}")
            return {"success": False, "error": str(e), "channel": "slack"}
    
    async def _send_webhook_alert(self, alert: Alert) -> Dict:
        """Send alert via webhook."""
        webhook_config = self.config.get("webhook", {})
        
        if not webhook_config.get("enabled", False):
            return {"success": False, "error": "Webhook channel disabled"}
        
        try:
            # Create webhook payload
            payload = {
                "alert": asdict(alert),
                "sent_at": datetime.now().isoformat(),
                "system": "ai_agent_platform"
            }
            
            # Send webhook
            response = requests.post(
                webhook_config['url'],
                json=payload,
                headers=webhook_config.get('headers', {}),
                timeout=webhook_config.get('timeout', 5)
            )
            
            if response.status_code in [200, 201, 202]:
                logger.info(f"Webhook alert sent: {alert.alert_id}")
                return {"success": True, "channel": "webhook", "status_code": response.status_code}
            else:
                logger.error(f"Webhook error: {response.status_code} - {response.text}")
                return {
                    "success": False,
                    "error": f"Webhook error: {response.status_code}",
                    "channel": "webhook",
                    "status_code": response.status_code
                }
            
        except Exception as e:
            logger.error(f"Failed to send webhook alert: {str(e)}")
            return {"success": False, "error": str(e), "channel": "webhook"}
    
    async def _store_in_app_alert(self, alert: Alert) -> Dict:
        """Store alert for in-app display."""
        in_app_config = self.config.get("in_app", {})
        
        if not in_app_config.get("enabled", True):
            return {"success": False, "error": "In-app channel disabled"}
        
        try:
            # In production, this would store in a database
            # For now, we just log and store in memory
            logger.info(f"In-app alert stored: {alert.alert_id}")
            
            return {
                "success": True,
                "channel": "in_app",
                "storage": in_app_config.get("storage", "memory"),
                "alert": asdict(alert)
            }
            
        except Exception as e:
            logger.error(f"Failed to store in-app alert: {str(e)}")
            return {"success": False, "error": str(e), "channel": "in_app"}
    
    async def _log_alert(self, alert: Alert) -> Dict:
        """Log alert to system logs."""
        try:
            log_levels = {
                AlertLevel.INFO: logging.INFO,
                AlertLevel.WARNING: logging.WARNING,
                AlertLevel.ERROR: logging.ERROR,
                AlertLevel.CRITICAL: logging.CRITICAL
            }
            
            log_message = f"ALERT [{alert.level.value.upper()}] {alert.category}: {alert.message}"
            logger.log(log_levels.get(alert.level, logging.INFO), log_message)
            
            return {"success": True, "channel": "log"}
            
        except Exception as e:
            logger.error(f"Failed to log alert: {str(e)}")
            return {"success": False, "error": str(e), "channel": "log"}
    
    # Alert management methods
    
    def add_alert_rule(self, rule: AlertRule):
        """
        Add an alert rule for automatic alerting.
        
        Args:
            rule: Alert rule to add
        """
        self.alert_rules.append(rule)
        logger.info(f"Added alert rule: {rule.name}")
    
    async def check_alert_rules(self, data: Dict) -> List[Dict]:
        """
        Check data against alert rules and trigger alerts if conditions are met.
        
        Args:
            data: Data to check against rules
            
        Returns:
            List of triggered alerts
        """
        triggered_alerts = []
        
        for rule in self.alert_rules:
            # Check cooldown
            if rule.last_triggered:
                cooldown_end = rule.last_triggered + timedelta(minutes=rule.cooldown_minutes)
                if datetime.now() < cooldown_end:
                    continue
            
            # Check condition
            try:
                if rule.condition(data):
                    # Format message using template
                    message = rule.template.format(**data)
                    
                    # Send alert
                    result = await self.send_alert(
                        message=message,
                        level=rule.level,
                        category=f"rule_{rule.name}",
                        source="alert_rule",
                        channels=rule.channels,
                        metadata={"rule_id": rule.rule_id, "data": data}
                    )
                    
                    if result.get("success"):
                        rule.last_triggered = datetime.now()
                        triggered_alerts.append({
                            "rule_id": rule.rule_id,
                            "rule_name": rule.name,
                            "alert_id": result.get("alert_id"),
                            "message": message
                        })
                    
            except Exception as e:
                logger.error(f"Error checking alert rule {rule.name}: {str(e)}")
        
        return triggered_alerts
    
    def get_recent_alerts(
        self,
        limit: int = 50,
        level: Optional[Union[str, AlertLevel]] = None,
        category: Optional[str] = None,
        source: Optional[str] = None
    ) -> List[Dict]:
        """
        Get recent alerts with optional filtering.
        
        Args:
            limit: Maximum number of alerts to return
            level: Filter by alert level
            category: Filter by category
            source: Filter by source
            
        Returns:
            List of recent alerts
        """
        alerts = self.alerts_sent.copy()
        
        # Apply filters
        if level:
            if isinstance(level, str):
                level = AlertLevel(level.lower())
            alerts = [a for a in alerts if a.level == level]
        
        if category:
            alerts = [a for a in alerts if a.category == category]
        
        if source:
            alerts = [a for a in alerts if a.source == source]
        
        # Sort by timestamp (newest first) and limit
        alerts.sort(key=lambda x: x.timestamp, reverse=True)
        return [asdict(a) for a in alerts[:limit]]
    
    async def acknowledge_alert(
        self,
        alert_id: str,
        acknowledged_by: str,
        notes: Optional[str] = None
    ) -> bool:
        """
        Acknowledge an alert.
        
        Args:
            alert_id: Alert ID to acknowledge
            acknowledged_by: Person or system acknowledging
            notes: Optional notes
            
        Returns:
            True if successful, False otherwise
        """
        for alert in self.alerts_sent:
            if alert.alert_id == alert_id and not alert.acknowledged:
                alert.acknowledged = True
                alert.acknowledged_by = acknowledged_by
                alert.acknowledged_at = datetime.now()
                
                if notes:
                    alert.metadata["acknowledgment_notes"] = notes
                
                logger.info(f"Alert {alert_id} acknowledged by {acknowledged_by}")
                return True
        
        return False
    
    def get_alert_stats(self, hours: int = 24) -> Dict:
        """
        Get alert statistics for the specified time period.
        
        Args:
            hours: Hours to look back
            
        Returns:
            Alert statistics
        """
        cutoff = datetime.now() - timedelta(hours=hours)
        
        recent_alerts = [a for a in self.alerts_sent if a.timestamp > cutoff]
        
        stats = {
            "total_alerts": len(recent_alerts),
            "by_level": {},
            "by_category": {},
            "by_source": {},
            "acknowledged": sum(1 for a in recent_alerts if a.acknowledged),
            "time_period_hours": hours
        }
        
        # Count by level
        for level in AlertLevel:
            stats["by_level"][level.value] = sum(1 for a in recent_alerts if a.level == level)
        
        # Count by category
        categories = set(a.category for a in recent_alerts)
        for category in categories:
            stats["by_category"][category] = sum(1 for a in recent_alerts if a.category == category)
        
        # Count by source
        sources = set(a.source for a in recent_alerts)
        for source in sources:
            stats["by_source"][source] = sum(1 for a in recent_alerts if a.source == source)
        
        return stats
    
    def get_tool_status(self) -> Dict:
        """Get tool status and statistics."""
        return {
            "total_alerts_sent": len(self.alerts_sent),
            "alert_history_size": len(self.alert_history),
            "active_alert_rules": len(self.alert_rules),
            "rate_limit": {
                "max_per_minute": self.max_alerts_per_minute,
                "recent_alerts": len([t for t in self.alert_timestamps 
                                     if t > datetime.now() - timedelta(minutes=1)])
            },
            "channels_enabled": {
                channel.value: self.config.get(channel.value, {}).get("enabled", False)
                for channel in AlertChannel if channel != AlertChannel.ALL
            }
        }