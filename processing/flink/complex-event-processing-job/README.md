# Complex Event Processing (CEP) with Apache Flink

Real-time pattern detection and fraud prevention system using Apache Flink's CEP library, integrated with Apache Druid for analytics and Apache Ignite for caching.

## 🎯 Overview

This Flink job provides comprehensive real-time pattern detection across multiple event streams:

- **Fraud Detection**: Velocity checks, account takeover, money laundering, pump & dump schemes
- **Security Monitoring**: Brute force attacks, privilege escalation, data exfiltration
- **System Anomaly Detection**: Cascading failures, resource exhaustion, latency spikes  
- **Business Rule Enforcement**: Compliance violations, trading manipulation, duplicate submissions

## 🏗️ Architecture

```
Event Streams → Flink CEP → Pattern Detection → Alert Enrichment → Multi-Channel Output
      ↓                           ↓                    ↓                    ↓
   Pulsar                    State Store          Ignite Cache      Druid Analytics
```

### Components

1. **Event Ingestion**: Consumes from multiple Pulsar topics
2. **Pattern Libraries**: Modular pattern definitions for different threat types
3. **CEP Engine**: Flink's pattern matching with time windows
4. **Alert Enrichment**: Entity profiles and historical context from Ignite
5. **Risk Scoring**: Multi-factor risk assessment
6. **Output Routing**: Severity-based routing to different channels

## 🚀 Key Features

### Pattern Detection Capabilities

#### Fraud Patterns
- **Velocity Check**: Detects unusual transaction velocity (10+ transactions in 5 minutes)
- **Account Takeover**: 3 failed logins → success → suspicious action within 30 minutes
- **Money Laundering**: Large deposit followed by multiple small transfers
- **Pump and Dump**: Asset promotion → price spike → large sell within 48 hours
- **Sybil Attack**: Multiple account creations followed by coordinated actions

#### Security Patterns
- **Brute Force**: 10+ authentication failures within 5 minutes
- **Privilege Escalation**: Repeated attempts to access higher privileges
- **Data Exfiltration**: Bulk data access followed by external API calls

#### System Anomaly Patterns
- **Cascading Failures**: Error propagation across services
- **Resource Exhaustion**: Sustained high usage → critical threshold
- **Latency Spikes**: Normal latency → 10x spike patterns

#### Business Rule Patterns
- **Compliance Violations**: High-risk actions without compliance checks
- **Trading Manipulation**: Large buy → price increase → large sell
- **Duplicate Submissions**: Multiple form submissions within 1 minute

### Alert Enrichment

Each detected pattern is enriched with:
- Entity risk profile from Ignite cache
- Historical alert count and trends
- Trust scores and account age
- Device fingerprints and behavior metrics
- Recommended remediation actions

### Risk Scoring

Multi-factor risk assessment considering:
- Base pattern risk score
- Entity trust score (inversely weighted)
- Historical alert frequency
- Account age and verification status
- Pattern severity and potential impact

## 📡 Integration Points

### Apache Druid Analytics

The CEP system integrates with Druid through the analytics-service:

```python
# Real-time alert ingestion
- Alerts → Druid datasource: 'cep_alerts'
- Metrics → Druid datasource: 'cep_metrics'
- Pattern statistics → 'pattern_statistics'

# Analytics queries available:
- Alert trends by pattern/severity
- Entity risk profiles over time
- Pattern performance metrics
- OLAP cube for multi-dimensional analysis
```

### Apache Ignite Cache

Used for:
- Entity profile caching
- Alert history storage
- Real-time aggregations
- Pattern match state

### Apache Pulsar

Input topics:
- `user-activity-events`
- `transaction-events`
- `system-events`
- `security-events`
- `api-events`

Output topics:
- `cep-alerts` (all alerts)
- `critical-alerts` (severity = CRITICAL)
- `cep-metrics` (aggregated metrics)

## 🔧 Configuration

### Python Job Configuration

```python
# Flink settings
env.set_parallelism(8)
env.enable_checkpointing(30000)  # 30 seconds

# Time windows
Pattern.within(Time.minutes(5))  # Configurable per pattern

# Ignite nodes
IGNITE_NODES = [
    ('ignite-0.ignite', 10800),
    ('ignite-1.ignite', 10800),
    ('ignite-2.ignite', 10800)
]
```

### Java Job Configuration

```java
// Checkpointing
env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);

// State backend
env.setStateBackend(new FsStateBackend("hdfs://namenode:9000/flink/checkpoints"));

// Parallelism
env.setParallelism(params.getInt("parallelism", 8));
```

## 📊 Monitoring & Observability

### Metrics Available

- **Processing Rate**: Events processed per second
- **Pattern Match Rate**: Patterns detected per minute
- **Alert Distribution**: By severity and pattern type
- **Latency**: Event time vs processing time lag
- **State Size**: Memory usage for pattern state

### Dashboards

Access real-time dashboards via analytics-service:

```bash
# Get real-time stats
GET /api/v1/analytics/cep/stats

# Query historical alerts
POST /api/v1/analytics/cep/alerts
{
  "time_range": "24h",
  "dimensions": ["pattern_name", "severity"],
  "filters": {"severity": ["CRITICAL", "HIGH"]}
}

# Get entity risk profile
GET /api/v1/analytics/cep/entity/{entity_id}/risk
```

## 🚦 Alert Response

### Recommended Actions by Pattern

| Pattern | Severity | Automated Actions | Manual Actions |
|---------|----------|------------------|----------------|
| Account Takeover | CRITICAL | Force logout, Reset 2FA | Review access logs |
| Money Laundering | CRITICAL | Freeze account | File SAR, Compliance review |
| Brute Force | HIGH | Block IP, Enable captcha | Review source IPs |
| Resource Exhaustion | MEDIUM | Scale resources | Investigate root cause |
| Velocity Check | HIGH | Temporary freeze | Manual transaction review |

### Alert Lifecycle

1. **Detection**: Pattern matched in real-time
2. **Enrichment**: Add context from Ignite/Druid
3. **Risk Scoring**: Calculate final risk score
4. **Routing**: Send to appropriate channels
5. **Action**: Automated response or manual review
6. **Analytics**: Store in Druid for analysis

## 🛠️ Development

### Running Locally

```bash
# Python version
cd processing/flink/complex-event-processing-job
python src/main.py

# Java version
mvn clean package
flink run -c com.platformq.flink.cep.ComplexEventProcessingJob target/complex-event-processing-job-1.0.0.jar
```

### Adding New Patterns

1. Define pattern in appropriate library:
```python
@staticmethod
def new_pattern() -> Pattern:
    return Pattern.begin("start").where(
        lambda x: x.get('event_type') == 'SUSPICIOUS_EVENT'
    ).followed_by("escalation").where(
        lambda x: x.get('severity') == 'HIGH'
    ).within(Time.minutes(10))
```

2. Add pattern to CEP job:
```python
new_pattern_stream = CEP.pattern(keyed_stream, new_pattern)
alerts = new_pattern_stream.select(PatternMatchProcessor("new_pattern", "HIGH"))
```

3. Define recommended actions in PatternMatchProcessor

### Testing Patterns

Use the event generator to test patterns:
```python
# Generate test events
test_events = [
    {"event_type": "LOGIN_FAILED", "entity_id": "user123", "timestamp": "..."},
    {"event_type": "LOGIN_FAILED", "entity_id": "user123", "timestamp": "..."},
    {"event_type": "LOGIN_SUCCESS", "entity_id": "user123", "timestamp": "..."},
    {"event_type": "PASSWORD_CHANGE", "entity_id": "user123", "timestamp": "..."}
]
```

## 🔍 Troubleshooting

### Common Issues

1. **High Memory Usage**: Adjust pattern time windows or increase heap size
2. **Late Events**: Configure watermark strategy for out-of-order events
3. **State Growth**: Enable state TTL for pattern states
4. **Alert Storms**: Implement rate limiting and deduplication

### Performance Tuning

- **Parallelism**: Set based on input volume and pattern complexity
- **Checkpointing**: Balance between recovery time and overhead
- **State Backend**: Use RocksDB for large state
- **Time Windows**: Shorter windows = less memory but may miss patterns

## 📈 Success Metrics

Track these KPIs to measure effectiveness:

1. **Detection Rate**: Patterns detected / Total suspicious activities
2. **False Positive Rate**: Invalid alerts / Total alerts
3. **Response Time**: Time from event to alert
4. **Coverage**: Event types covered by patterns
5. **Business Impact**: Fraud prevented, attacks blocked

## 🔗 Related Documentation

- [Apache Flink CEP Documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/libs/cep/)
- [Analytics Service Integration](../../../services/analytics-service/README.md)
- [Event Schema Documentation](../../../docs/EVENT_SCHEMAS.md)
- [Alert Response Playbooks](../../../docs/ALERT_PLAYBOOKS.md) 