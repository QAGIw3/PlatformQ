# Market Aggregator Service

Unified interface for quantum, AI, and network compute resource markets with bundle optimization and arbitrage detection.

## Overview

The Market Aggregator Service provides:
- **Resource Bundling**: Combine quantum, AI, and network resources with optimized allocation
- **Arbitrage Detection**: Identify and execute price/quality arbitrage opportunities
- **Market Comparison**: Compare prices across spot, futures, and reserved markets
- **Workload Templates**: Pre-configured resource combinations for common use cases

## Architecture

### Core Components
- **Bundle Optimizer**: Multi-objective optimization using genetic algorithms, simulated annealing, or greedy approaches
- **Arbitrage Detector**: Real-time monitoring for price differentials, quality arbitrage, and time arbitrage
- **Market Client**: Unified interface to quantum, AI, and network market services
- **Workload Templates**: Pre-defined combinations for quantum-ML hybrid, distributed training, real-time inference

### Optimization Algorithms
- **Genetic Algorithm**: Population-based optimization for complex multi-resource bundles
- **Simulated Annealing**: Probabilistic optimization for escaping local optima
- **Greedy**: Fast allocation for simple requirements

## API Endpoints

### Resource Bundles
- `POST /api/v1/bundles` - Create resource bundle
- `GET /api/v1/bundles/{bundle_id}` - Get bundle details
- `POST /api/v1/bundles/{bundle_id}/allocate` - Optimize and allocate bundle
- `POST /api/v1/bundles/{bundle_id}/execute/{allocation_id}` - Execute allocation

### Arbitrage
- `POST /api/v1/arbitrage/search` - Search for arbitrage opportunities
- `GET /api/v1/arbitrage/opportunities` - List active opportunities
- `POST /api/v1/arbitrage/execute/{opportunity_id}` - Execute arbitrage

### Market Comparison
- `POST /api/v1/markets/compare` - Compare prices across markets
- `GET /api/v1/markets/workload-templates` - Get workload templates
- `POST /api/v1/markets/optimize-workload/{template_id}` - Optimize workload
- `GET /api/v1/markets/market-stats` - Get market statistics

## Key Features

1. **Bundle Optimization**
   - Multi-objective optimization (cost, performance, latency)
   - Constraint satisfaction (budget, quality thresholds)
   - Cross-resource discounts (5% base + 3% for multi-type bundles)

2. **Arbitrage Types**
   - Price Differential: Spot vs futures markets
   - Quality Arbitrage: Underpriced high-quality resources
   - Time Arbitrage: Reserved vs spot instances
   - Cross-Market: Different pricing across providers

3. **Workload Templates**
   - Quantum-ML Hybrid: QPU + GPU + Low-latency network
   - Distributed Training: Multi-TPU + High-bandwidth network
   - Real-time Inference: NPU + Ultra-low latency network
   - Quantum Simulation: Large QPU + GPU + Medium bandwidth

## Running the Service

```bash
cd services/market-aggregator-service
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8028
```

## Configuration

Key environment variables:
- `QUANTUM_MARKET_URL`: Quantum market service endpoint
- `AI_MARKET_URL`: AI compute market service endpoint
- `NETWORK_MARKET_URL`: Network bandwidth market service endpoint
- `OPTIMIZATION_ALGORITHM`: Algorithm choice (genetic, simulated_annealing, greedy)
- `ARBITRAGE_DETECTION_ENABLED`: Enable automatic arbitrage detection 