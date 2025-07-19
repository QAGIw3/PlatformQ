# Derivatives Trading CEP (Complex Event Processing)

Real-time pattern detection and risk monitoring for the derivatives trading platform using Apache Flink's CEP capabilities.

## 🎯 Overview

This Flink job provides comprehensive real-time monitoring for derivatives trading activities:

- **Fraud Detection**: Wash trading, spoofing, layering, front-running
- **Anomaly Detection**: Price spikes, volume anomalies, liquidation cascades
- **Compliance Monitoring**: Position limits, concentration risk, rapid position changes
- **Risk Aggregation**: Real-time risk scoring across all patterns

## 🏗️ Architecture

```
Trading Events → Flink CEP → Pattern Detection → Risk Aggregation → Multi-Channel Alerts
       ↓                           ↓                    ↓                    ↓
    Pulsar                    State Store          Risk Scores        Alert Routing
```

## 🚀 Key Features

### Fraud Detection Patterns

#### Wash Trading Detection
- **Pattern**: Buy order followed by sell order from same user within 5 minutes
- **Risk Score**: 0.8
- **Actions**: Account freeze, manual review, compliance reporting

#### Spoofing Detection
- **Pattern**: Large order → Multiple cancellations → Small opposite order
- **Risk Score**: 0.9
- **Actions**: Cancel orders, temporary ban, increased monitoring

#### Layering Detection
- **Pattern**: Multiple orders at different price levels within 10 seconds
- **Risk Score**: 0.85
- **Actions**: Cancel orders, warning notice, increased fees

#### Front-Running Detection
- **Pattern**: Small scout order → Large victim order → Exit order
- **Risk Score**: 0.95
- **Actions**: Investigation, account suspension, regulatory filing

### Anomaly Detection Patterns

#### Price Spike Detection
- **Pattern**: Price movement >5% within 1 minute
- **Risk Score**: 0.7
- **Actions**: Trading halt, price band widening, market maker alerts

#### Volume Anomaly Detection
- **Pattern**: Volume >10x average in single period
- **Risk Score**: 0.6
- **Actions**: Enhanced monitoring, liquidity assessment

#### Liquidation Cascade Detection
- **Pattern**: 3+ liquidations within 5 minutes
- **Risk Score**: 0.9
- **Actions**: Trading halt, margin increase, circuit breakers

#### Funding Rate Anomaly
- **Pattern**: Funding rate change >0.1% in single update
- **Risk Score**: 0.5
- **Actions**: Rate adjustment review, market analysis

### Compliance Monitoring Patterns

#### Position Limit Breach
- **Pattern**: Position size exceeds configured limit
- **Risk Score**: 0.8
- **Actions**: Block new orders, force reduction, risk team notification

#### Concentration Risk
- **Pattern**: Single entity controls >25% of market
- **Risk Score**: 0.85
- **Actions**: Position limits, enhanced monitoring, regulatory reporting

#### Rapid Position Changes
- **Pattern**: Position change >50% within 1 hour
- **Risk Score**: 0.7
- **Actions**: Risk assessment, margin review, manual verification

## 📊 Risk Aggregation

The system maintains real-time risk scores for each user and market:

```python
Risk Score Calculation:
- Base score from pattern type
- Adjusted by event frequency
- Weighted by historical behavior
- Aggregated across all patterns
```

Risk Levels:
- **LOW**: Score < 0.5
- **MEDIUM**: Score 0.5-0.7
- **HIGH**: Score 0.7-0.8
- **CRITICAL**: Score > 0.8

## 🔔 Alert Management

### Alert Structure
```json
{
  "alert_id": "alert_1234567890",
  "pattern_name": "spoofing",
  "severity": "CRITICAL",
  "risk_score": 0.9,
  "timestamp": "2024-01-15T10:30:00Z",
  "user_id": "user123",
  "market_id": "BTC-PERP",
  "event_count": 15,
  "recommended_actions": ["cancel_orders", "temporary_ban"],
  "events": [...]
}
```

### Alert Routing
- **All Alerts**: `derivatives-cep-alerts` topic
- **Critical Alerts**: `critical-derivatives-alerts` topic
- **Risk Scores**: `aggregated-risk-scores` topic

## 🛠️ Configuration

### Pattern Thresholds

```python
# Fraud Detection
WASH_TRADING_WINDOW = 5 minutes
SPOOFING_CANCELLATION_THRESHOLD = 3 orders
LAYERING_ORDER_COUNT = 4-20 orders

# Anomaly Detection
PRICE_SPIKE_THRESHOLD = 5%
VOLUME_SPIKE_MULTIPLIER = 10x
LIQUIDATION_CASCADE_MIN = 3

# Compliance
POSITION_LIMIT = Configurable per market
CONCENTRATION_THRESHOLD = 25%
RAPID_CHANGE_THRESHOLD = 50%
```

### Performance Tuning

```python
# Flink Configuration
Parallelism: 8
Checkpoint Interval: 30 seconds
Watermark Delay: 10 seconds
State Backend: RocksDB (for large state)

# Pattern Optimization
- Use keyed streams for user-specific patterns
- Separate pattern streams for different event types
- Union streams only after pattern matching
```

## 📡 Integration

### Input Events

The job consumes from multiple Pulsar topics:
- `trading-events`: Order placements and executions
- `order-events`: Order updates and cancellations
- `position-events`: Position updates and liquidations
- `market-data-events`: Price and volume updates
- `liquidation-events`: Liquidation triggers
- `compliance-events`: Limit updates and violations

### Event Schema

```json
{
  "event_type": "ORDER_PLACED",
  "timestamp": "2024-01-15T10:30:00Z",
  "user_id": "user123",
  "market_id": "BTC-PERP",
  "side": "buy",
  "size": 10.5,
  "price": 45000,
  "order_id": "order456",
  "metadata": {...}
}
```

## 🚦 Operational Procedures

### Response Playbook

| Pattern | Immediate Action | Investigation | Long-term |
|---------|-----------------|---------------|-----------|
| Wash Trading | Freeze account | Trade analysis | Ban/Report |
| Spoofing | Cancel orders | Pattern history | Increase fees |
| Liquidation Cascade | Halt trading | Risk assessment | Adjust parameters |
| Position Limit | Block orders | Portfolio review | Adjust limits |

### Monitoring Metrics

- **Pattern Detection Rate**: Patterns detected per minute
- **Alert Volume**: Alerts generated per severity level
- **Processing Latency**: Event time vs processing time
- **State Size**: Memory usage for pattern state
- **Checkpoint Duration**: Time to complete checkpoints

## 🔧 Development

### Running Locally

```bash
cd processing/flink/derivatives-cep-job
python src/main.py
```

### Adding New Patterns

1. Create pattern class in appropriate category:
```python
@staticmethod
def new_fraud_pattern() -> Pattern:
    return Pattern.begin("start").where(
        lambda x: x.get('event_type') == 'SUSPICIOUS_EVENT'
    ).followed_by("confirmation").where(
        lambda x: x.get('confirmation_criteria') == True
    ).within(Time.minutes(10))
```

2. Add to pattern list in main job:
```python
fraud_patterns.append(
    ('new_pattern', NewPatternClass.new_fraud_pattern(), 'HIGH')
)
```

3. Update risk scoring and actions in PatternMatchProcessor

### Testing Patterns

Use the event simulator to test pattern detection:
```python
test_events = [
    {
        "event_type": "ORDER_PLACED",
        "user_id": "test_user",
        "side": "buy",
        "size": 100,
        "timestamp": "2024-01-15T10:00:00Z"
    },
    {
        "event_type": "ORDER_PLACED",
        "user_id": "test_user",
        "side": "sell",
        "size": 100,
        "timestamp": "2024-01-15T10:02:00Z"
    }
]
# This should trigger wash trading detection
```

## 🔍 Troubleshooting

### Common Issues

1. **High Memory Usage**: 
   - Reduce pattern time windows
   - Increase checkpoint frequency
   - Use state TTL for old patterns

2. **Late Events**:
   - Adjust watermark strategy
   - Increase bounded out-of-orderness time
   - Monitor event time lag

3. **False Positives**:
   - Tune pattern thresholds
   - Add additional filtering conditions
   - Implement pattern correlation

4. **Performance Issues**:
   - Increase parallelism
   - Optimize keying strategy
   - Use side outputs for filtering 