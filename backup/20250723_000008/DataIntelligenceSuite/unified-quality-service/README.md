# Unified Quality Service

A comprehensive data quality management service that consolidates all quality operations across the DataIntelligenceSuite.

## Overview

This enhanced service consolidates quality operations from multiple services and provides a unified approach to data quality management using the new `QualityProcessor` from the common library.

## Features

### Multi-Dimensional Quality Assessment
- **Completeness**: Missing value detection and analysis
- **Accuracy**: Data accuracy validation against business rules
- **Consistency**: Cross-dataset consistency checks
- **Timeliness**: Data freshness and latency monitoring
- **Validity**: Format and constraint validation
- **Uniqueness**: Duplicate detection and resolution

### Quality Check Types
- Null/missing value checks
- Duplicate detection (row and column level)
- Range and boundary checks
- Format and pattern validation
- Referential integrity checks
- Business rule validation
- Statistical anomaly detection
- ML-based quality assessment

### Advanced Capabilities
- Real-time and batch quality processing
- Data profiling and statistics
- Anomaly detection with configurable sensitivity
- Auto-remediation strategies
- Quality trend analysis
- SLA monitoring and alerting
- Data lineage integration

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Data Sources   │────▶│ Quality Engine   │────▶│ Quality Store   │
│  (Batch/Stream) │     │ (Rule-based/ML)  │     │ (Metrics/Logs)  │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                               │                          │
                               ▼                          ▼
                        ┌──────────────────┐     ┌─────────────────┐
                        │ Remediation      │     │ Monitoring      │
                        │ Engine           │     │ Dashboard       │
                        └──────────────────┘     └─────────────────┘
```

## Quick Start

### Prerequisites
- Python 3.8+
- Apache Ignite (for caching)
- Cassandra (for quality metrics storage)
- Elasticsearch (for quality logs)

### Installation

```bash
cd services/DataIntelligenceSuite/unified-quality-service
pip install -r requirements.txt
```

### Configuration

```yaml
# config.yaml
service:
  name: unified-quality-service
  port: 8086

quality:
  overall_threshold: 0.95
  dimension_thresholds:
    completeness: 0.95
    accuracy: 0.98
    consistency: 0.99
    timeliness: 0.95
    validity: 0.99
    uniqueness: 0.999
    
  fail_on_breach: false
  enable_profiling: true
  enable_anomaly_detection: true
  enable_auto_remediation: false
  
storage:
  metrics:
    type: cassandra
    hosts: ["localhost"]
  logs:
    type: elasticsearch
    hosts: ["localhost:9200"]
```

### Running the Service

```bash
python -m app.main
```

## API Endpoints

### Quality Assessment
- `POST /api/v1/quality/assess` - Run quality assessment on data
- `GET /api/v1/quality/results/{job_id}` - Get assessment results
- `GET /api/v1/quality/profile/{dataset_id}` - Get data profile

### Rule Management
- `GET /api/v1/quality/rules` - List quality rules
- `POST /api/v1/quality/rules` - Create quality rule
- `PUT /api/v1/quality/rules/{rule_id}` - Update rule
- `DELETE /api/v1/quality/rules/{rule_id}` - Delete rule

### Monitoring
- `GET /api/v1/quality/metrics` - Get quality metrics
- `GET /api/v1/quality/trends` - Get quality trends
- `GET /api/v1/quality/sla` - Get SLA compliance

### Remediation
- `GET /api/v1/quality/remediation/strategies` - List remediation strategies
- `POST /api/v1/quality/remediation/apply` - Apply remediation

## Usage Examples

### Basic Quality Assessment

```python
from data_intelligence_common import QualityProcessor, QualityConfig, QualityRule
from data_intelligence_common import QualityDimension, QualityCheckType

# Configure quality processor
config = QualityConfig(
    name="customer_data_quality",
    overall_quality_threshold=0.95,
    enable_profiling=True,
    enable_anomaly_detection=True
)

# Create processor
processor = QualityProcessor(config)

# Add quality rules
processor.add_quality_rule(QualityRule(
    rule_id="null_check_email",
    name="Email Null Check",
    check_type=QualityCheckType.NULL_CHECK,
    dimension=QualityDimension.COMPLETENESS,
    column="email",
    threshold=0.99,
    severity="critical"
))

processor.add_quality_rule(QualityRule(
    rule_id="format_check_phone",
    name="Phone Format Check",
    check_type=QualityCheckType.FORMAT_CHECK,
    dimension=QualityDimension.VALIDITY,
    column="phone",
    metadata={"pattern": r"^\+?1?\d{10,14}$"},
    threshold=0.95,
    severity="error"
))

# Run assessment
result = await processor.process(customer_data)
print(f"Overall Quality Score: {result.metadata['overall_quality_score']:.2%}")
```

### Quality Pipeline

```python
from data_intelligence_common import PipelineBuilder

# Build quality pipeline
pipeline = PipelineBuilder("data_quality_pipeline")
    .source(batch_processor, "s3://raw-data/customers/")
    .quality(quality_processor, [
        NullCheck(["email", "phone", "address"]),
        DuplicateCheck(["customer_id"]),
        RangeCheck("age", min=0, max=120),
        FormatCheck("email", pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$"),
        BusinessRule("credit_limit <= income * 3")
    ])
    .transform(lambda df: df.filter("quality_score >= 0.95"))
    .sink(batch_processor, "s3://clean-data/customers/")
    .build()

# Execute pipeline
results = await pipeline.execute()
```

### Real-time Quality Monitoring

```python
# Create streaming quality processor
stream_config = QualityConfig(
    name="real_time_quality",
    mode=ProcessingMode.STREAM,
    checkpoint_interval=timedelta(seconds=30)
)

stream_processor = QualityProcessor(stream_config)

# Process streaming data
async def monitor_quality(event):
    result = await stream_processor.process(event)
    
    if result.metadata['overall_quality_score'] < 0.9:
        # Trigger alert
        await alert_manager.send_alert(
            severity="warning",
            message=f"Quality degradation detected: {result.metadata['overall_quality_score']:.2%}"
        )
    
    return result

# Subscribe to event stream
await event_bus.subscribe("data_events", monitor_quality)
```

### Custom Quality Rules

```python
# Define custom business rule
def validate_customer_segment(df):
    """Validate customer segmentation logic"""
    invalid = df[
        (df['segment'] == 'premium') & 
        (df['annual_spend'] < 10000)
    ]
    
    return len(invalid) == 0

# Add as business rule
processor.add_quality_rule(QualityRule(
    rule_id="segment_validation",
    name="Customer Segment Validation",
    check_type=QualityCheckType.BUSINESS_RULE,
    dimension=QualityDimension.ACCURACY,
    condition=validate_customer_segment,
    severity="error"
))
```

## Quality Dimensions

### Completeness
Measures the extent to which data is not missing.
- Null value ratio
- Required field coverage
- Optional field fill rate

### Accuracy
Measures how closely data reflects real-world values.
- Business rule compliance
- Reference data matching
- Calculated field accuracy

### Consistency
Measures uniformity of data across datasets.
- Cross-table consistency
- Temporal consistency
- Format consistency

### Timeliness
Measures how current the data is.
- Data age analysis
- Update frequency
- Processing latency

### Validity
Measures conformance to syntax rules.
- Format validation
- Type checking
- Constraint validation

### Uniqueness
Measures absence of duplicates.
- Primary key uniqueness
- Natural key uniqueness
- Fuzzy duplicate detection

## Monitoring and Alerting

### Metrics
The service exposes comprehensive metrics:
- `quality_score_by_dimension` - Score breakdown by dimension
- `quality_checks_total` - Total quality checks performed
- `quality_failures_by_rule` - Failures grouped by rule
- `quality_processing_duration` - Processing time histogram

### Dashboards
Pre-built Grafana dashboards for:
- Real-time quality monitoring
- Historical quality trends
- Rule effectiveness analysis
- SLA compliance tracking

### Alerts
Configurable alerts for:
- Quality threshold breaches
- Anomaly detection
- SLA violations
- System health issues

## Remediation Strategies

### Automatic Remediation
- **Null Handling**: Fill with defaults, interpolation, or ML predictions
- **Duplicate Resolution**: Keep first/last, merge, or custom logic
- **Format Correction**: Auto-formatting for common patterns
- **Outlier Treatment**: Capping, transformation, or removal

### Manual Remediation
- Generate remediation reports
- Provide fix suggestions
- Track remediation actions
- Validate fixes

## Integration

### Data Catalog Integration
- Automatic quality metadata updates
- Quality-based data classification
- Lineage quality propagation

### ML Platform Integration
- Quality gates for ML pipelines
- Feature quality monitoring
- Model input validation

### Orchestration Integration
- Quality checkpoints in workflows
- Conditional branching based on quality
- Quality-driven retries

## Advanced Features

### ML-Based Quality Assessment

```python
from sklearn.ensemble import IsolationForest

# Train anomaly detection model
model = IsolationForest(contamination=0.1)
model.fit(historical_data)

# Create ML-based quality rule
def ml_anomaly_check(df):
    predictions = model.predict(df[['feature1', 'feature2']])
    anomaly_rate = (predictions == -1).sum() / len(predictions)
    return anomaly_rate < 0.15

processor.add_quality_rule(QualityRule(
    rule_id="ml_anomaly",
    name="ML Anomaly Detection",
    check_type=QualityCheckType.ML_BASED,
    dimension=QualityDimension.ACCURACY,
    condition=ml_anomaly_check,
    threshold=0.85
))
```

### Quality Trend Analysis

```python
# Analyze quality trends
trends = await quality_service.analyze_trends(
    dataset="customers",
    period="30d",
    dimensions=["completeness", "accuracy"]
)

# Predict future quality
forecast = await quality_service.forecast_quality(
    dataset="customers",
    horizon="7d"
)
```

## Performance Optimization

- Parallel quality check execution
- Intelligent sampling for large datasets
- Caching of quality results
- Incremental quality assessment
- Distributed processing with Spark

## Migration Guide

### From Individual Service Quality Checks

1. Consolidate quality rules into unified format
2. Migrate custom validators to new rule types
3. Update quality thresholds and SLAs
4. Integrate with new quality API

## License

Copyright (c) 2024 PlatformQ. All rights reserved.