# Cost Optimization Service

The Cost Optimization Service provides intelligent cost analysis, optimization recommendations, and budget management across all Platform Q resources.

## Overview

This service helps organizations:
- Monitor and control cloud spending
- Identify cost optimization opportunities
- Enforce budget limits
- Generate cost reports and forecasts
- Automate cost-saving actions

## Features

- **Real-time Cost Tracking**: Monitor costs across all providers and services
- **Budget Management**: Set and enforce spending limits per tenant
- **Optimization Recommendations**: AI-powered suggestions for cost reduction
- **Cost Forecasting**: Predict future spending based on usage patterns
- **Automated Actions**: Implement cost-saving measures automatically
- **Multi-provider Support**: Unified cost view across AWS, CloudStack, Kubernetes, etc.

## API Endpoints

### Cost Analysis
- `GET /api/v1/costs/current` - Get current cost breakdown
- `GET /api/v1/costs/history` - Get historical cost data
- `GET /api/v1/costs/forecast` - Get cost forecast
- `GET /api/v1/costs/by-service` - Cost breakdown by service
- `GET /api/v1/costs/by-tenant` - Cost breakdown by tenant

### Budget Management
- `POST /api/v1/budgets` - Create budget
- `GET /api/v1/budgets/{budget_id}` - Get budget details
- `PUT /api/v1/budgets/{budget_id}` - Update budget
- `DELETE /api/v1/budgets/{budget_id}` - Delete budget
- `GET /api/v1/budgets/{budget_id}/alerts` - Get budget alerts

### Recommendations
- `GET /api/v1/recommendations` - Get optimization recommendations
- `POST /api/v1/recommendations/{id}/apply` - Apply recommendation
- `POST /api/v1/recommendations/{id}/dismiss` - Dismiss recommendation

### Reports
- `GET /api/v1/reports/summary` - Get cost summary report
- `GET /api/v1/reports/detailed` - Get detailed cost report
- `POST /api/v1/reports/generate` - Generate custom report

### Health & Metrics
- `GET /health` - Health check endpoint
- `GET /ready` - Readiness check endpoint
- `GET /metrics` - Prometheus metrics

## Configuration

Environment variables:
- `SERVICE_PORT` - Service port (default: 8002)
- `CASSANDRA_HOSTS` - Cassandra contact points
- `PULSAR_URL` - Pulsar broker URL
- `COST_UPDATE_INTERVAL` - Cost data update interval (minutes)
- `RECOMMENDATION_ENGINE_ENABLED` - Enable recommendation engine
- `BUDGET_CHECK_INTERVAL` - Budget check interval (minutes)

## Cost Optimization Strategies

### Instance Right-sizing
- Analyze CPU, memory, and storage utilization
- Recommend optimal instance types
- Identify over-provisioned resources

### Reserved Instance Planning
- Analyze usage patterns
- Recommend RI purchases
- Track RI utilization

### Spot Instance Optimization
- Identify spot-friendly workloads
- Recommend spot instance usage
- Monitor spot price trends

### Resource Cleanup
- Identify unused resources
- Detect orphaned storage volumes
- Find idle compute instances

### Scheduling Optimization
- Recommend resource scheduling
- Identify off-hours opportunities
- Suggest auto-scaling configurations

## Budget Management

### Budget Types
- **Fixed Budget**: Hard spending limit
- **Forecasted Budget**: Based on historical trends
- **Recurring Budget**: Monthly/quarterly reset
- **Project Budget**: Tied to specific projects

### Alert Thresholds
- 50% - Information alert
- 75% - Warning alert
- 90% - Critical alert
- 100% - Budget exceeded

### Budget Actions
- Send notifications
- Restrict resource provisioning
- Implement cost-saving measures
- Generate exception reports

## Recommendation Engine

The AI-powered recommendation engine analyzes:
- Historical usage patterns
- Current resource utilization
- Cost trends
- Industry best practices

### Recommendation Categories
1. **Quick Wins**: Immediate savings with minimal effort
2. **Strategic Changes**: Long-term optimization requiring planning
3. **Architecture Improvements**: Design changes for cost efficiency
4. **Process Optimizations**: Workflow improvements

## Cost Allocation

### Tagging Strategy
- Mandatory tags for cost allocation
- Department/team attribution
- Project/application tracking
- Environment classification

### Chargeback Models
- Direct cost allocation
- Shared service distribution
- Usage-based charging
- Fixed allocation

## Development

### Running Locally
```bash
cd services/cost-optimization-service
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8002
```

### Running Tests
```bash
pytest tests/
```

### Building Docker Image
```bash
docker build -t cost-optimization-service:latest -f services/cost-optimization-service/Dockerfile .
```

## Architecture

The service consists of:

- **Cost Analyzer**: Collects and processes cost data
- **Budget Manager**: Manages budgets and alerts
- **Recommendation Engine**: Generates optimization suggestions
- **Report Generator**: Creates cost reports
- **Action Executor**: Implements cost-saving actions

## Data Flow

1. **Collection**: Gather cost data from all providers
2. **Normalization**: Standardize cost data format
3. **Analysis**: Process and analyze cost trends
4. **Storage**: Store in time-series database
5. **Recommendation**: Generate optimization suggestions
6. **Reporting**: Create dashboards and reports

## Integration

### Provider Integration
- AWS Cost Explorer API
- CloudStack billing API
- Kubernetes metrics
- Custom provider adapters

### Service Integration
- Compute Allocation Service for resource data
- Resource Monitoring Service for utilization metrics
- Quota Management Service for limit enforcement

## Monitoring

The service exposes Prometheus metrics for:
- Cost data collection latency
- Recommendation generation performance
- Budget alert frequency
- API endpoint response times
- Cost savings achieved 