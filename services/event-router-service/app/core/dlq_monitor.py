"""
Dead Letter Queue (DLQ) Monitoring System

Monitors DLQ topics for failed messages, provides alerting,
and implements automated recovery strategies.
"""

import asyncio
import json
import logging
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import re

from pulsar import Client as PulsarClient, Consumer, Message, ConsumerType
from pulsar.schema import JsonSchema
from prometheus_client import Counter, Gauge, Histogram
import httpx

from platformq_shared.config import ConfigLoader
from platformq_shared.notifications import NotificationClient

logger = logging.getLogger(__name__)


class AlertSeverity(str, Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class RecoveryStrategy(str, Enum):
    """Message recovery strategies"""
    RETRY = "retry"  # Retry processing
    SKIP = "skip"    # Skip and acknowledge
    MANUAL = "manual"  # Require manual intervention
    REDIRECT = "redirect"  # Send to different topic


@dataclass
class DLQMessage:
    """Dead letter queue message"""
    message_id: str
    topic: str
    original_topic: str
    payload: Any
    properties: Dict[str, str]
    publish_time: datetime
    delivery_count: int
    error_reason: Optional[str] = None
    stack_trace: Optional[str] = None
    
    
@dataclass 
class DLQAlert:
    """DLQ alert configuration"""
    name: str
    condition: str  # Expression to evaluate
    threshold: float
    window_minutes: int
    severity: AlertSeverity
    recovery_strategy: RecoveryStrategy
    notification_channels: List[str] = field(default_factory=list)
    cooldown_minutes: int = 30
    last_triggered: Optional[datetime] = None
    

@dataclass
class DLQMetrics:
    """DLQ metrics snapshot"""
    topic: str
    message_count: int
    oldest_message_age_seconds: float
    newest_message_age_seconds: float
    average_delivery_count: float
    error_categories: Dict[str, int]
    recovery_success_rate: float
    timestamp: datetime = field(default_factory=datetime.utcnow)


class DLQMonitor:
    """Dead Letter Queue monitoring and recovery system"""
    
    def __init__(self,
                 pulsar_url: str = "pulsar://localhost:6650",
                 config_loader: Optional[ConfigLoader] = None):
        """
        Initialize DLQ Monitor
        
        Args:
            pulsar_url: Pulsar broker URL
            config_loader: Configuration loader
        """
        self.config_loader = config_loader or ConfigLoader()
        self.pulsar_client = PulsarClient(pulsar_url)
        self.notification_client = NotificationClient(config_loader)
        
        # DLQ patterns to monitor
        self.dlq_patterns = [
            "persistent://platformq/*/DLQ",
            "persistent://platformq/*/dlq",
            "persistent://platformq/*/dead-letter",
            "persistent://platformq/errors/*" # Added for redirect
        ]
        
        # Alert configurations
        self.alerts: Dict[str, DLQAlert] = self._load_alert_configs()
        
        # Recovery handlers
        self.recovery_handlers: Dict[RecoveryStrategy, Callable] = {
            RecoveryStrategy.RETRY: self._retry_message,
            RecoveryStrategy.SKIP: self._skip_message,
            RecoveryStrategy.REDIRECT: self._redirect_message,
            RecoveryStrategy.MANUAL: self._mark_for_manual_review
        }
        
        # Metrics
        self.dlq_messages_total = Counter(
            'dlq_messages_total',
            'Total DLQ messages',
            ['topic', 'error_type']
        )
        self.dlq_message_age = Histogram(
            'dlq_message_age_seconds',
            'Age of messages in DLQ',
            ['topic']
        )
        self.dlq_size = Gauge(
            'dlq_size',
            'Current size of DLQ',
            ['topic']
        )
        self.recovery_attempts = Counter(
            'dlq_recovery_attempts_total',
            'DLQ recovery attempts',
            ['topic', 'strategy', 'status']
        )
        
        # Monitoring state
        self._consumers: Dict[str, Consumer] = {}
        self._metrics_cache: Dict[str, DLQMetrics] = {}
        self._running = True
        self._tasks: List[asyncio.Task] = []
        
    def _load_alert_configs(self) -> Dict[str, DLQAlert]:
        """Load alert configurations"""
        return {
            "high_dlq_volume": DLQAlert(
                name="High DLQ Volume",
                condition="message_count > threshold",
                threshold=1000,
                window_minutes=5,
                severity=AlertSeverity.WARNING,
                recovery_strategy=RecoveryStrategy.RETRY,
                notification_channels=["slack", "email"]
            ),
            "old_messages": DLQAlert(
                name="Old Messages in DLQ",
                condition="oldest_message_age_seconds > threshold",
                threshold=3600,  # 1 hour
                window_minutes=10,
                severity=AlertSeverity.ERROR,
                recovery_strategy=RecoveryStrategy.MANUAL,
                notification_channels=["pagerduty", "slack"]
            ),
            "high_failure_rate": DLQAlert(
                name="High Failure Rate",
                condition="failure_rate > threshold",
                threshold=0.1,  # 10%
                window_minutes=15,
                severity=AlertSeverity.CRITICAL,
                recovery_strategy=RecoveryStrategy.MANUAL,
                notification_channels=["pagerduty", "slack", "email"]
            ),
            "repeated_failures": DLQAlert(
                name="Repeated Message Failures",
                condition="average_delivery_count > threshold",
                threshold=5,
                window_minutes=30,
                severity=AlertSeverity.ERROR,
                recovery_strategy=RecoveryStrategy.SKIP,
                notification_channels=["slack"]
            )
        }
        
    async def start(self):
        """Start DLQ monitoring"""
        logger.info("Starting DLQ monitor")
        
        # Discover DLQ topics
        dlq_topics = await self._discover_dlq_topics()
        logger.info(f"Found {len(dlq_topics)} DLQ topics to monitor")
        
        # Start monitoring each topic
        for topic in dlq_topics:
            task = asyncio.create_task(self._monitor_topic(topic))
            self._tasks.append(task)
            
        # Start metrics collection
        metrics_task = asyncio.create_task(self._collect_metrics())
        self._tasks.append(metrics_task)
        
        # Start alert evaluation
        alert_task = asyncio.create_task(self._evaluate_alerts())
        self._tasks.append(alert_task)
        
        # Start recovery processor
        recovery_task = asyncio.create_task(self._process_recovery_queue())
        self._tasks.append(recovery_task)
        
        logger.info("DLQ monitor started")
        
    async def stop(self):
        """Stop DLQ monitoring"""
        logger.info("Stopping DLQ monitor")
        self._running = False
        
        # Cancel all tasks
        for task in self._tasks:
            task.cancel()
            
        # Close consumers
        for consumer in self._consumers.values():
            consumer.close()
            
        # Close Pulsar client
        self.pulsar_client.close()
        
        logger.info("DLQ monitor stopped")
        
    async def _discover_dlq_topics(self) -> List[str]:
        """Discover DLQ topics matching patterns"""
        dlq_topics = []
        
        # Note: Pulsar Python client doesn't have topic discovery
        # In production, use Pulsar Admin API
        
        # For now, return known DLQ topics
        known_dlqs = [
            "persistent://platformq/events/DLQ",
            "persistent://platformq/compute/DLQ",
            "persistent://platformq/settlement/DLQ",
            "persistent://platformq/analytics/DLQ",
            "persistent://platformq/notifications/DLQ"
        ]
        
        return known_dlqs
        
    async def _monitor_topic(self, topic: str):
        """Monitor a specific DLQ topic"""
        logger.info(f"Starting monitor for DLQ topic: {topic}")
        
        # Create consumer
        consumer = self.pulsar_client.subscribe(
            topic,
            subscription_name=f"dlq-monitor-{topic.split('/')[-1]}",
            consumer_type=ConsumerType.Shared
        )
        self._consumers[topic] = consumer
        
        while self._running:
            try:
                # Receive message with timeout
                msg = consumer.receive(timeout_millis=1000)
                
                try:
                    # Parse DLQ message
                    dlq_msg = self._parse_dlq_message(msg)
                    
                    # Update metrics
                    self._update_metrics(topic, dlq_msg)
                    
                    # Check if message should be recovered
                    strategy = self._determine_recovery_strategy(dlq_msg)
                    
                    if strategy != RecoveryStrategy.MANUAL:
                        # Attempt recovery
                        success = await self._attempt_recovery(dlq_msg, strategy)
                        
                        if success:
                            # Acknowledge message
                            consumer.acknowledge(msg)
                            self.recovery_attempts.labels(
                                topic=topic,
                                strategy=strategy.value,
                                status="success"
                            ).inc()
                        else:
                            # Negative acknowledge for retry
                            consumer.negative_acknowledge(msg)
                            self.recovery_attempts.labels(
                                topic=topic,
                                strategy=strategy.value,
                                status="failed"
                            ).inc()
                    else:
                        # Manual intervention required
                        await self._mark_for_manual_review(dlq_msg)
                        # Don't acknowledge - leave in DLQ
                        
                except Exception as e:
                    logger.error(f"Error processing DLQ message: {e}")
                    consumer.negative_acknowledge(msg)
                    
            except Exception:
                # Timeout - continue
                await asyncio.sleep(0.1)
                
    def _parse_dlq_message(self, msg: Message) -> DLQMessage:
        """Parse Pulsar message into DLQ message"""
        properties = msg.properties()
        
        # Extract original topic from properties
        original_topic = properties.get("ORIGINAL_TOPIC", "unknown")
        
        # Parse error information
        error_reason = properties.get("EXCEPTION_MESSAGE", "Unknown error")
        stack_trace = properties.get("EXCEPTION_STACKTRACE")
        
        # Get delivery count
        delivery_count = int(properties.get("DELIVERY_COUNT", "1"))
        
        return DLQMessage(
            message_id=msg.message_id().serialize(),
            topic=msg.topic_name(),
            original_topic=original_topic,
            payload=msg.data(),
            properties=properties,
            publish_time=datetime.fromtimestamp(msg.publish_timestamp() / 1000),
            delivery_count=delivery_count,
            error_reason=error_reason,
            stack_trace=stack_trace
        )
        
    def _update_metrics(self, topic: str, dlq_msg: DLQMessage):
        """Update DLQ metrics"""
        # Categorize error
        error_category = self._categorize_error(dlq_msg.error_reason)
        
        # Update counters
        self.dlq_messages_total.labels(
            topic=topic,
            error_type=error_category
        ).inc()
        
        # Update message age
        age_seconds = (datetime.utcnow() - dlq_msg.publish_time).total_seconds()
        self.dlq_message_age.labels(topic=topic).observe(age_seconds)
        
        # Update cache for metrics calculation
        if topic not in self._metrics_cache:
            self._metrics_cache[topic] = DLQMetrics(
                topic=topic,
                message_count=0,
                oldest_message_age_seconds=0,
                newest_message_age_seconds=float('inf'),
                average_delivery_count=0,
                error_categories={},
                recovery_success_rate=0
            )
            
        metrics = self._metrics_cache[topic]
        metrics.message_count += 1
        metrics.oldest_message_age_seconds = max(
            metrics.oldest_message_age_seconds,
            age_seconds
        )
        metrics.newest_message_age_seconds = min(
            metrics.newest_message_age_seconds,
            age_seconds
        )
        
        # Update error categories
        if error_category not in metrics.error_categories:
            metrics.error_categories[error_category] = 0
        metrics.error_categories[error_category] += 1
        
    def _categorize_error(self, error_reason: Optional[str]) -> str:
        """Categorize error message"""
        if not error_reason:
            return "unknown"
            
        error_lower = error_reason.lower()
        
        # Common error patterns
        patterns = {
            "timeout": r"timeout|timed out",
            "connection": r"connection|connect|network",
            "serialization": r"serializ|deserializ|json|parse",
            "validation": r"validat|invalid|required",
            "authorization": r"auth|forbidden|unauthorized",
            "rate_limit": r"rate|limit|throttl",
            "resource": r"resource|memory|disk|cpu",
            "database": r"database|db|sql|cassandra|mongo",
            "service": r"service|unavailable|503"
        }
        
        for category, pattern in patterns.items():
            if re.search(pattern, error_lower):
                return category
                
        return "other"
        
    def _determine_recovery_strategy(self, dlq_msg: DLQMessage) -> RecoveryStrategy:
        """Determine recovery strategy for message"""
        error_category = self._categorize_error(dlq_msg.error_reason)
        
        # Strategy based on error type and delivery count
        if dlq_msg.delivery_count > 10:
            # Too many retries
            return RecoveryStrategy.MANUAL
            
        elif error_category in ["timeout", "connection", "service"]:
            # Transient errors - retry
            return RecoveryStrategy.RETRY
            
        elif error_category in ["serialization", "validation"]:
            # Data errors - skip or manual
            if dlq_msg.delivery_count > 3:
                return RecoveryStrategy.SKIP
            else:
                return RecoveryStrategy.RETRY
                
        elif error_category in ["authorization", "rate_limit"]:
            # Need manual intervention
            return RecoveryStrategy.MANUAL
            
        else:
            # Default strategy based on delivery count
            if dlq_msg.delivery_count <= 3:
                return RecoveryStrategy.RETRY
            else:
                return RecoveryStrategy.MANUAL
                
    async def _attempt_recovery(self, 
                               dlq_msg: DLQMessage, 
                               strategy: RecoveryStrategy) -> bool:
        """Attempt to recover a DLQ message"""
        handler = self.recovery_handlers.get(strategy)
        
        if not handler:
            logger.error(f"No handler for recovery strategy: {strategy}")
            return False
            
        try:
            return await handler(dlq_msg)
        except Exception as e:
            logger.error(f"Recovery failed for message {dlq_msg.message_id}: {e}")
            return False
            
    async def _retry_message(self, dlq_msg: DLQMessage) -> bool:
        """Retry processing the message"""
        try:
            # Create producer for original topic
            producer = self.pulsar_client.create_producer(dlq_msg.original_topic)
            
            # Prepare message with updated properties
            properties = dict(dlq_msg.properties)
            properties["RETRY_COUNT"] = str(dlq_msg.delivery_count + 1)
            properties["RETRY_TIMESTAMP"] = datetime.utcnow().isoformat()
            properties["DLQ_RECOVERY"] = "true"
            
            # Send message
            producer.send(
                dlq_msg.payload,
                properties=properties
            )
            
            producer.close()
            
            logger.info(f"Retried message {dlq_msg.message_id} to {dlq_msg.original_topic}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to retry message: {e}")
            return False
            
    async def _skip_message(self, dlq_msg: DLQMessage) -> bool:
        """Skip the message and log it"""
        try:
            # Log skipped message for audit
            skip_record = {
                "message_id": dlq_msg.message_id,
                "original_topic": dlq_msg.original_topic,
                "error_reason": dlq_msg.error_reason,
                "delivery_count": dlq_msg.delivery_count,
                "skipped_at": datetime.utcnow().isoformat(),
                "payload_sample": str(dlq_msg.payload)[:200]  # First 200 chars
            }
            
            # In production, write to audit log
            logger.warning(f"Skipping DLQ message: {json.dumps(skip_record)}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to skip message: {e}")
            return False
            
    async def _redirect_message(self, dlq_msg: DLQMessage) -> bool:
        """Redirect message to different topic"""
        try:
            # Determine redirect topic based on error
            redirect_topic = self._determine_redirect_topic(dlq_msg)
            
            if not redirect_topic:
                return False
                
            # Create producer for redirect topic
            producer = self.pulsar_client.create_producer(redirect_topic)
            
            # Send with redirect metadata
            properties = dict(dlq_msg.properties)
            properties["REDIRECTED_FROM"] = dlq_msg.original_topic
            properties["REDIRECT_REASON"] = dlq_msg.error_reason
            
            producer.send(
                dlq_msg.payload,
                properties=properties
            )
            
            producer.close()
            
            logger.info(f"Redirected message {dlq_msg.message_id} to {redirect_topic}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to redirect message: {e}")
            return False
            
    async def _mark_for_manual_review(self, dlq_msg: DLQMessage) -> bool:
        """Mark message for manual review"""
        try:
            # Create manual review record
            review_record = {
                "message_id": dlq_msg.message_id,
                "topic": dlq_msg.topic,
                "original_topic": dlq_msg.original_topic,
                "error_reason": dlq_msg.error_reason,
                "stack_trace": dlq_msg.stack_trace,
                "delivery_count": dlq_msg.delivery_count,
                "publish_time": dlq_msg.publish_time.isoformat(),
                "marked_at": datetime.utcnow().isoformat(),
                "payload": str(dlq_msg.payload)
            }
            
            # Store in manual review queue
            # In production, write to database or dedicated topic
            review_topic = "persistent://platformq/dlq/manual-review"
            producer = self.pulsar_client.create_producer(review_topic)
            
            producer.send(
                json.dumps(review_record).encode('utf-8'),
                properties={"message_id": dlq_msg.message_id}
            )
            
            producer.close()
            
            # Send notification
            await self.notification_client.send_notification(
                channels=["slack"],
                title="DLQ Message Requires Manual Review",
                message=f"Message {dlq_msg.message_id} from {dlq_msg.original_topic} "
                       f"requires manual intervention. Error: {dlq_msg.error_reason}",
                severity=AlertSeverity.WARNING.value
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to mark for manual review: {e}")
            return False
            
    def _determine_redirect_topic(self, dlq_msg: DLQMessage) -> Optional[str]:
        """Determine redirect topic based on error"""
        error_category = self._categorize_error(dlq_msg.error_reason)
        
        # Redirect mapping
        redirect_map = {
            "serialization": "persistent://platformq/errors/serialization",
            "validation": "persistent://platformq/errors/validation",
            "rate_limit": "persistent://platformq/errors/rate-limited"
        }
        
        return redirect_map.get(error_category)
        
    async def _collect_metrics(self):
        """Periodically collect and aggregate metrics"""
        while self._running:
            try:
                await asyncio.sleep(60)  # Collect every minute
                
                for topic, consumer in self._consumers.items():
                    # Get consumer stats
                    # Note: Python client doesn't expose all stats
                    # In production, use Pulsar Admin API
                    
                    # Update gauge
                    if topic in self._metrics_cache:
                        self.dlq_size.labels(topic=topic).set(
                            self._metrics_cache[topic].message_count
                        )
                        
            except Exception as e:
                logger.error(f"Error collecting metrics: {e}")
                
    async def _evaluate_alerts(self):
        """Evaluate alert conditions"""
        while self._running:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                for alert_name, alert in self.alerts.items():
                    # Check cooldown
                    if alert.last_triggered:
                        cooldown_elapsed = (
                            datetime.utcnow() - alert.last_triggered
                        ).total_seconds() / 60
                        
                        if cooldown_elapsed < alert.cooldown_minutes:
                            continue
                            
                    # Evaluate condition for each topic
                    for topic, metrics in self._metrics_cache.items():
                        if self._evaluate_alert_condition(alert, metrics):
                            await self._trigger_alert(alert, topic, metrics)
                            alert.last_triggered = datetime.utcnow()
                            
            except Exception as e:
                logger.error(f"Error evaluating alerts: {e}")
                
    def _evaluate_alert_condition(self, 
                                 alert: DLQAlert, 
                                 metrics: DLQMetrics) -> bool:
        """Evaluate if alert condition is met"""
        # Simple evaluation - in production use expression parser
        if alert.condition == "message_count > threshold":
            return metrics.message_count > alert.threshold
        elif alert.condition == "oldest_message_age_seconds > threshold":
            return metrics.oldest_message_age_seconds > alert.threshold
        elif alert.condition == "average_delivery_count > threshold":
            return metrics.average_delivery_count > alert.threshold
        else:
            return False
            
    async def _trigger_alert(self, 
                           alert: DLQAlert, 
                           topic: str, 
                           metrics: DLQMetrics):
        """Trigger an alert"""
        logger.warning(f"Alert triggered: {alert.name} for topic {topic}")
        
        # Build alert message
        message = f"""
        DLQ Alert: {alert.name}
        Topic: {topic}
        Severity: {alert.severity.value}
        
        Metrics:
        - Message Count: {metrics.message_count}
        - Oldest Message Age: {metrics.oldest_message_age_seconds:.0f}s
        - Error Categories: {json.dumps(metrics.error_categories)}
        
        Recovery Strategy: {alert.recovery_strategy.value}
        """
        
        # Send notifications
        await self.notification_client.send_notification(
            channels=alert.notification_channels,
            title=f"DLQ Alert: {alert.name}",
            message=message,
            severity=alert.severity.value
        )
        
        # Trigger automatic recovery if configured
        if alert.recovery_strategy != RecoveryStrategy.MANUAL:
            logger.info(f"Triggering automatic recovery: {alert.recovery_strategy}")
            # Recovery will be handled by the monitor loop
            
    async def _process_recovery_queue(self):
        """Process messages queued for recovery"""
        # This would process a dedicated recovery queue
        # For now, recovery is handled inline in the monitor
        while self._running:
            await asyncio.sleep(60) 