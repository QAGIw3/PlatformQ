# Unified Quality Service

A comprehensive, ML-powered data quality management platform that consolidates all quality-related functionality into a single, powerful service.

## Overview

The Unified Quality Service combines the best features from data-quality-service and quality-engine-service, providing:

- **Autonomous Quality Management**: Self-healing data quality with ML-driven detection and remediation
- **Comprehensive Profiling**: Statistical analysis, pattern detection, and data understanding
- **Advanced Anomaly Detection**: Multiple detection methods including statistical, ML-based, and time-series
- **Intelligent Rule Engine**: Flexible rule management with SQL, Python, and regex support
- **Automated Remediation**: ML-powered issue correction with learning capabilities
- **Real-time Monitoring**: Continuous quality tracking with alerting and trending
- **SeaTunnel Integration**: Leverages Apache SeaTunnel for efficient data movement and quality checks

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                   Unified Quality Service                     │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────────┐  ┌─────────────────┐  ┌─────────────┐│
│  │ Quality Engine    │  │ ML Components   │  │ Monitoring  ││
│  │ - Validation      │  │ - Anomaly Det.  │  │ - Metrics   ││
│  │ - Profiling       │  │ - Root Cause    │  │ - Alerts    ││
│  │ - Rule Execution  │  │ - Auto-Fix      │  │ - Trends    ││
│  └──────────────────┘  └─────────────────┘  └─────────────┘│
│                                                              │
│  ┌──────────────────────────────────────────────────────────┐│
│  │               SeaTunnel Integration                       ││
│  │ - Data Pipeline Quality Gates                             ││
│  │ - Streaming Quality Checks                                ││
│  │ - Cross-System Data Movement                              ││
│  └──────────────────────────────────────────────────────────┘│
│                                                              │
│  Storage: Ignite | ElasticSearch | Cassandra | MinIO        │
│  Events: Apache Pulsar | Monitoring: Prometheus/Grafana     │
└──────────────────────────────────────────────────────────────┘
```

## Key Features

### 1. Autonomous Quality Management
- **Self-Healing**: Automatically detects and fixes quality issues
- **ML-Driven**: Learns from past fixes to improve over time
- **Root Cause Analysis**: Identifies underlying causes of quality problems
- **Predictive Quality**: Predicts potential quality issues before they occur

### 2. Advanced Profiling
- **Statistical Analysis**: Comprehensive statistics for all data types
- **Pattern Recognition**: Detects formats, ranges, and business patterns
- **Data Drift Detection**: Monitors changes in data distribution
- **Correlation Analysis**: Identifies relationships between fields
- **Great Expectations Integration**: Enterprise-grade profiling

### 3. Multi-Method Anomaly Detection
- **Statistical Methods**: Z-score, IQR, Grubbs test
- **ML-Based**: Isolation Forest, LOF, One-Class SVM
- **Time Series**: Prophet, LSTM-based detection
- **Ensemble Methods**: Combines multiple detectors for accuracy
- **Real-time Detection**: Streaming anomaly detection via SeaTunnel

### 4. Intelligent Rule Engine
- **Multiple Rule Types**: SQL, Python expressions, regex patterns
- **Rule Learning**: Suggests new rules based on data patterns
- **Performance Optimization**: Rule execution optimization
- **Version Control**: Full rule versioning and rollback
- **A/B Testing**: Test rule effectiveness before deployment

### 5. Smart Remediation
- **Auto-Fix Strategies**: Missing value imputation, format standardization
- **ML-Powered Decisions**: Learns best remediation strategies
- **Simulation Mode**: Preview changes before applying
- **Rollback Support**: Full audit trail and rollback capability
- **Custom Strategies**: Plugin architecture for custom remediation

### 6. SeaTunnel Integration
- **Quality Gates**: Embedded quality checks in data pipelines
- **Stream Processing**: Real-time quality validation
- **Cross-System Movement**: Quality-assured data transfers
- **Pipeline Templates**: Pre-built quality pipeline configurations

## API Endpoints

### Core Quality Operations
- `POST /api/v1/quality/check` - Comprehensive quality check
- `POST /api/v1/quality/validate` - Validate against specific rules
- `POST /api/v1/quality/profile` - Deep data profiling
- `POST /api/v1/quality/remediate` - Auto-remediate issues
- `GET /api/v1/quality/score/{dataset}` - Quality scoring

### Anomaly Detection
- `POST /api/v1/anomalies/detect` - Run anomaly detection
- `GET /api/v1/anomalies/monitor/{dataset}` - Real-time monitoring
- `POST /api/v1/anomalies/train` - Train custom models

### Rule Management
- `POST /api/v1/rules` - Create rule
- `GET /api/v1/rules` - List rules with advanced filtering
- `PUT /api/v1/rules/{id}` - Update rule
- `POST /api/v1/rules/suggest` - ML-suggested rules
- `POST /api/v1/rules/test` - Test rule effectiveness

### Monitoring & Analytics
- `GET /api/v1/monitoring/dashboard/{dataset}` - Quality dashboard
- `GET /api/v1/monitoring/trends` - Quality trends
- `POST /api/v1/monitoring/alerts` - Configure alerts
- `GET /api/v1/monitoring/predictions` - Predictive analytics

### SeaTunnel Integration
- `POST /api/v1/seatunnel/pipeline` - Create quality pipeline
- `GET /api/v1/seatunnel/gates` - List quality gates
- `POST /api/v1/seatunnel/stream` - Stream quality check

## Configuration

```yaml
# Service Configuration
SERVICE_NAME: unified-quality-service
SERVICE_PORT: 8000
ENVIRONMENT: production

# ML Configuration
ML_ANOMALY_MODELS: ["isolation_forest", "prophet", "lstm"]
ML_AUTO_RETRAIN: true
ML_RETRAIN_INTERVAL: 86400  # 24 hours

# SeaTunnel Configuration
SEATUNNEL_HOME: /opt/seatunnel
SEATUNNEL_API_URL: http://seatunnel-api:8080
SEATUNNEL_QUALITY_TEMPLATES: /config/quality-templates

# Storage Configuration
IGNITE_HOST: ignite
IGNITE_PORT: 10800
ELASTICSEARCH_HOSTS: ["elasticsearch:9200"]
CASSANDRA_HOSTS: ["cassandra:9042"]

# Monitoring
PROMETHEUS_ENABLED: true
METRICS_PORT: 9090
ALERT_CHANNELS: ["pulsar", "email", "slack"]

# Performance
PARALLEL_WORKERS: 8
BATCH_SIZE: 10000
CACHE_TTL: 3600
```

## Usage Examples

### Comprehensive Quality Check with SeaTunnel
```python
# Create a SeaTunnel quality pipeline
response = requests.post('http://quality-service:8000/api/v1/seatunnel/pipeline', json={
    "name": "customer_data_quality_pipeline",
    "source": {
        "type": "postgresql",
        "config": {
            "host": "postgres",
            "database": "customers",
            "table": "customer_data"
        }
    },
    "quality_checks": [
        "completeness",
        "anomaly_detection",
        "business_rules"
    ],
    "sink": {
        "type": "elasticsearch",
        "config": {
            "index": "quality_results"
        }
    }
})
```

### ML-Powered Anomaly Detection
```python
# Detect anomalies with ensemble methods
response = requests.post('http://quality-service:8000/api/v1/anomalies/detect', json={
    "dataset_id": "transactions_2024",
    "methods": ["isolation_forest", "prophet", "lstm"],
    "ensemble_strategy": "voting",
    "sensitivity": 0.95,
    "auto_remediate": true
})
```

### Smart Rule Suggestion
```python
# Get ML-suggested rules based on data patterns
response = requests.post('http://quality-service:8000/api/v1/rules/suggest', json={
    "dataset_id": "sales_data",
    "analyze_patterns": true,
    "suggest_validations": true,
    "confidence_threshold": 0.8
})
```

## Quality Scoring

Enhanced multi-dimensional quality scoring:

1. **Completeness** (15%) - Non-null values, required fields
2. **Accuracy** (20%) - Correctness, business rule compliance
3. **Consistency** (20%) - Cross-field, cross-dataset consistency
4. **Timeliness** (10%) - Data freshness, update frequency
5. **Validity** (15%) - Format, range, type compliance
6. **Uniqueness** (10%) - Duplicate detection
7. **Integrity** (10%) - Referential, relationship integrity

## Performance Optimizations

- **Ignite Caching**: Hot data and rule caching
- **Parallel Processing**: Multi-threaded validation
- **Incremental Checks**: Only validate changed data
- **Rule Optimization**: Query plan optimization
- **Batch Processing**: Efficient batch operations

## Monitoring & Observability

### Prometheus Metrics
```
# Quality metrics
unified_quality_score{dataset="sales", dimension="accuracy"} 0.95
unified_quality_checks_total{status="passed"} 1234567
unified_quality_anomalies_detected{severity="high"} 42
unified_quality_remediation_success_rate 0.87

# Performance metrics
unified_quality_processing_time_seconds{operation="profile"} 2.5
unified_quality_rule_execution_duration_seconds{rule="email_validation"} 0.05
```

### Health Endpoints
- `/health` - Basic health check
- `/ready` - Readiness with dependency checks
- `/metrics` - Prometheus metrics
- `/api/v1/monitoring/health/detailed` - Detailed component health

## Migration from Legacy Services

### From data-quality-service
```bash
# Export rules and configurations
python scripts/migrate_quality.py export --service data-quality

# Import to unified service
python scripts/migrate_quality.py import --target unified-quality
```

### From quality-engine-service
```bash
# Migrate with automatic mapping
python scripts/migrate_quality.py migrate --source quality-engine --target unified-quality
```

## Best Practices

1. **Start with Profiling**: Always profile data before setting rules
2. **Use ML Suggestions**: Let the system suggest rules based on patterns
3. **Test Remediation**: Always use dry-run mode first
4. **Monitor Trends**: Track quality scores over time
5. **Leverage SeaTunnel**: Use for efficient data movement with quality checks

## License

Copyright (c) 2024 PlatformQ. All rights reserved. 