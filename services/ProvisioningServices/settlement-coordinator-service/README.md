# Settlement Coordinator Service

A high-performance settlement orchestration service that integrates CloudKitty for billing and OpenMeter for real-time usage tracking, specifically designed for compute market physical settlement risk calculation.

## Overview

The Settlement Coordinator Service manages the complete settlement lifecycle for compute resource transactions, implementing multi-layered risk assessment for physical settlement (non-delivery of provisioned resources).

## Features

- **Real-time Settlement Processing**: Low-latency settlement coordination with gRPC endpoints
- **Physical Settlement Risk Calculation**: Multi-tiered risk assessment from basic probabilistic to advanced Monte Carlo
- **Billing Integration**: CloudKitty for cost rating and invoice generation
- **Usage Tracking**: OpenMeter for real-time resource consumption metrics
- **Risk Mitigation**: Automated risk-adjusted pricing and escrow management
- **State Management**: Apache Ignite for distributed caching and state persistence

## Architecture

### Risk Calculation Layers

1. **Basic Probabilistic Model**
   - Risk Score = (1 - SLA uptime %) × (resold capacity value × downtime penalty factor)
   - Quick assessment for low-value transactions

2. **Adapted SA-CCR Method**
   - Exposure = α(1.4) × (replacement cost + potential future exposure)
   - Based on volatility forecasts and market conditions

3. **Monte Carlo Simulations**
   - Scenario-based loss distributions at 95% confidence levels
   - For high-value or volatile compute futures

### Integration Points

- **CloudKitty**: Rating engine for compute resource pricing
- **OpenMeter**: Real-time usage ingestion and aggregation
- **Ignite**: Distributed cache for settlement state
- **Pulsar**: Event streaming for settlement notifications
- **compute-market-service**: Market data and order information
- **risk-management-service**: Risk models and thresholds

## API Reference

### gRPC Services

```protobuf
service SettlementCoordinator {
  // Process settlement for completed trades
  rpc ProcessSettlement(SettlementRequest) returns (SettlementResponse);
  
  // Calculate physical settlement risk
  rpc CalculateRisk(RiskRequest) returns (RiskResponse);
  
  // Get settlement status
  rpc GetSettlementStatus(StatusRequest) returns (StatusResponse);
  
  // Stream settlement updates
  rpc StreamSettlements(StreamRequest) returns (stream SettlementUpdate);
}
```

### REST Endpoints

- `POST /api/v1/settlements/process` - Process batch settlements
- `GET /api/v1/settlements/{id}` - Get settlement details
- `POST /api/v1/risk/calculate` - Calculate settlement risk
- `GET /api/v1/settlements/pending` - List pending settlements
- `POST /api/v1/settlements/{id}/reconcile` - Manual reconciliation

## Configuration

```yaml
# CloudKitty Integration
cloudkitty:
  url: "http://cloudkitty:8889"
  rating_module: "compute-futures"
  
# OpenMeter Integration  
openmeter:
  url: "http://openmeter:8080"
  meters:
    - compute_hours
    - gpu_hours
    - memory_gb_hours
    
# Risk Parameters
risk:
  basic_threshold: 1000  # USD - transactions below use basic model
  saccr_threshold: 10000 # USD - transactions below use SA-CCR
  monte_carlo_iterations: 10000
  confidence_level: 0.95
  
# Settlement Configuration
settlement:
  batch_size: 100
  processing_interval: "5m"
  reconciliation_window: "24h"
```

## Deployment

The service is designed to run as a horizontally scalable stateless deployment with Ignite providing distributed state management.

```bash
docker-compose -f docker-compose.settlement.yml up -d
```

## Monitoring

- Prometheus metrics exposed at `/metrics`
- Grafana dashboards for settlement tracking
- Alert rules for settlement failures and risk threshold breaches 