# Market Making Service

A high-performance automated market making and liquidity provision service for the PlatformQ ecosystem. This service consolidates all AMM functionality, market making strategies, and liquidity management features.

## Overview

The Market Making Service provides:
- Automated Market Maker (AMM) functionality with multiple pool types
- Professional market making strategies and bots
- Liquidity provision and management
- Dynamic fee optimization
- Cross-market arbitrage
- Concentrated liquidity support
- Impermanent loss protection

## Architecture

### Core Components

1. **AMM Engine**: Core automated market maker functionality
   - Constant Product (x*y=k)
   - Stableswap curves
   - Concentrated liquidity (Uniswap V3 style)
   - Dynamic fee management

2. **Market Making Strategies**: Professional trading algorithms
   - Grid trading
   - Cross-market arbitrage
   - Delta-neutral strategies
   - Volatility arbitrage

3. **Liquidity Management**: Advanced liquidity features
   - Multi-tier liquidity pools
   - Impermanent loss protection
   - Liquidity mining programs
   - Cross-chain liquidity aggregation

4. **Risk Management**: Comprehensive risk controls
   - Position limits
   - Exposure monitoring
   - Drawdown protection
   - Dynamic hedging

### Technology Stack

- **FastAPI**: High-performance REST APIs
- **Apache Ignite**: In-memory state management
- **Apache Pulsar**: Event streaming
- **Apache Flink**: Stream processing for real-time analytics
- **Redis**: Caching layer
- **NumPy/SciPy**: Numerical computations

## Features

### AMM Features
- **Multiple Pool Types**: Constant product, stableswap, concentrated liquidity
- **Dynamic Fees**: Volatility-based fee adjustment
- **Compliant Pools**: KYC/AML integrated liquidity pools
- **Vault Strategies**: Covered calls, put selling, delta-neutral vaults
- **IL Protection**: Automated impermanent loss hedging

### Market Making Features
- **Strategy Templates**: Pre-built strategies for common scenarios
- **Custom Strategies**: Framework for building custom algorithms
- **Multi-Market**: Support for spot, futures, options markets
- **Inventory Management**: Automated position rebalancing
- **Performance Analytics**: Real-time P&L and risk metrics

### Liquidity Features
- **Liquidity Mining**: Incentive programs for liquidity providers
- **Cross-Chain Aggregation**: Unified liquidity across chains
- **Smart Routing**: Optimal execution across liquidity sources
- **MEV Protection**: Sandwich attack prevention

## API Endpoints

### Pool Management
- `POST /api/v1/pools/create` - Create new liquidity pool
- `GET /api/v1/pools` - List all pools
- `GET /api/v1/pools/{pool_id}` - Get pool details
- `POST /api/v1/pools/{pool_id}/add-liquidity` - Add liquidity
- `POST /api/v1/pools/{pool_id}/remove-liquidity` - Remove liquidity
- `POST /api/v1/pools/{pool_id}/swap` - Execute swap

### Market Making
- `POST /api/v1/strategies/deploy` - Deploy market making strategy
- `GET /api/v1/strategies` - List active strategies
- `PUT /api/v1/strategies/{strategy_id}` - Update strategy parameters
- `DELETE /api/v1/strategies/{strategy_id}` - Stop strategy
- `GET /api/v1/strategies/{strategy_id}/performance` - Get performance metrics

### Liquidity Mining
- `POST /api/v1/mining/programs` - Create liquidity mining program
- `GET /api/v1/mining/programs` - List active programs
- `GET /api/v1/mining/rewards/{user_id}` - Get user rewards
- `POST /api/v1/mining/claim` - Claim rewards

### Analytics
- `GET /api/v1/analytics/tvl` - Total value locked
- `GET /api/v1/analytics/volume` - Trading volume statistics
- `GET /api/v1/analytics/fees` - Fee analytics
- `GET /api/v1/analytics/il` - Impermanent loss metrics

## Configuration

### Environment Variables
```bash
# Service Configuration
SERVICE_NAME=market-making-service
SERVICE_PORT=8000

# Dependencies
IGNITE_HOST=ignite
PULSAR_URL=pulsar://pulsar:6650
REDIS_URL=redis://redis:6379

# Market Making
DEFAULT_SPREAD_BPS=20
MAX_POSITION_SIZE=1000000
REBALANCE_INTERVAL=60

# Risk Limits
MAX_DRAWDOWN_PERCENT=10
POSITION_LIMIT_USD=5000000
```

## Integration

### Trading Core Service
- Order execution
- Position management
- Market data feeds

### Risk Engine Service
- Risk assessment
- Margin calculations
- Liquidation monitoring

### Oracle Service
- Price feeds
- Volatility data
- Market metrics

### Analytics Service
- Performance tracking
- Market intelligence
- User analytics

## Monitoring

- Health endpoint: `/health`
- Metrics endpoint: `/metrics`
- WebSocket feeds: `/ws/pools`, `/ws/strategies`

### Key Metrics
- Pool TVL and volume
- Strategy P&L
- Execution quality
- Slippage statistics
- Fee revenue

## Development

### Local Setup
```bash
cd services/MarketServices/market-making-service
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m app.main
```

### Testing
```bash
pytest tests/
```

### Docker
```bash
docker build -t platformq/market-making-service .
docker run -p 8000:8000 platformq/market-making-service
```

## Security

- JWT authentication
- API rate limiting
- MEV protection
- Audit logging
- Position limits
- Slippage controls 