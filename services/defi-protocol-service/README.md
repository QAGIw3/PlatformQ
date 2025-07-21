# DeFi Protocol Service

A comprehensive decentralized finance (DeFi) service providing lending, borrowing, yield farming, liquidity pools, auctions, and insurance mechanisms across multiple blockchains.

## Features

### 🏦 Lending & Borrowing
- NFT-backed loans with customizable terms
- Peer-to-peer lending marketplace
- Automatic liquidation with insurance coverage
- Dynamic interest rate models
- Multi-chain support

### 🎯 NFT Auctions
- **Dutch Auctions**: Declining price over time
- **English Auctions**: Traditional bidding with incremental bids
- Reserve price support
- Platform fee collection
- Cross-chain NFT support

### 🌾 Yield Farming
- Liquidity mining pools
- Staking rewards distribution
- Lock period bonuses
- Early withdrawal penalties
- Auto-compounding options

### 💧 Liquidity Pools
- Automated Market Maker (AMM) functionality
- LP token management
- Impermanent loss tracking
- Dynamic fee tiers
- Price oracle integration

### 🛡️ Insurance Pools (New!)
- **Three-tier risk system**:
  - **Stable Pool**: Low risk/reward (5% base APY)
  - **Balanced Pool**: Medium risk/reward (12% base APY)
  - **Aggressive Pool**: High risk/reward (25% base APY)
- **Dynamic APY** based on pool utilization
- **Waterfall loss distribution** (aggressive pools take losses first)
- **Lock period bonuses** up to 15% additional APY
- **Automatic liquidation coverage** for lending protocol
- **Claims processing** for:
  - Liquidation losses
  - Protocol hacks (governance approval required)
  - Impermanent loss

### 🔐 Security Features
- Vault integration for secure key management
- Multi-signature treasury wallets
- Oracle signature verification
- Flash loan protection
- Transaction limit validation

## API Endpoints

### Lending
- `POST /api/v1/lending/offers/create` - Create loan offer
- `POST /api/v1/lending/borrow` - Borrow against NFT
- `POST /api/v1/lending/loans/{loan_id}/repay` - Repay loan
- `POST /api/v1/lending/loans/{loan_id}/liquidate` - Liquidate overdue loan
- `GET /api/v1/lending/loans/{loan_id}` - Get loan details
- `GET /api/v1/lending/offers` - List loan offers

### Auctions
- `POST /api/v1/auctions/dutch/create` - Create Dutch auction
- `POST /api/v1/auctions/english/create` - Create English auction
- `POST /api/v1/auctions/{auction_id}/bid` - Place bid
- `POST /api/v1/auctions/{auction_id}/buy` - Buy from Dutch auction
- `GET /api/v1/auctions/{auction_id}` - Get auction details

### Insurance
- `POST /api/v1/insurance/stake` - Stake liquidity in insurance pool
- `POST /api/v1/insurance/unstake` - Unstake liquidity
- `POST /api/v1/insurance/claim-rewards/{position_id}` - Claim staking rewards
- `POST /api/v1/insurance/claims/submit` - Submit insurance claim
- `GET /api/v1/insurance/pools/stats` - Get pool statistics
- `GET /api/v1/insurance/pools/apy` - Get current APYs
- `GET /api/v1/insurance/positions` - Get user positions
- `GET /api/v1/insurance/coverage/available` - Check available coverage

### Yield Farming
- `GET /api/v1/yield-farming/pools` - List yield farming pools

### Liquidity
- `GET /api/v1/liquidity/pools` - List liquidity pools

### Analytics
- `GET /api/v1/analytics/tvl` - Get total value locked
- `GET /api/v1/analytics/overview` - Get protocol overview

## Supported Blockchains

- Ethereum
- Polygon
- Arbitrum
- Optimism
- Avalanche
- Binance Smart Chain
- Solana
- Cosmos
- Polkadot

## Configuration

### Environment Variables

```bash
# Service Configuration
DEBUG=false
SERVICE_NAME=defi-protocol-service

# Blockchain RPC URLs
ETHEREUM_RPC_URL=http://ethereum-node:8545
POLYGON_RPC_URL=http://polygon-node:8545
ARBITRUM_RPC_URL=http://arbitrum-node:8545
OPTIMISM_RPC_URL=http://optimism-node:8545
AVALANCHE_RPC_URL=http://avalanche-node:9650
BSC_RPC_URL=http://bsc-node:8545
SOLANA_RPC_URL=http://solana-node:8899
COSMOS_RPC_URL=http://cosmos-node:26657
POLKADOT_RPC_URL=ws://polkadot-node:9944

# Database
DATABASE_URL=postgresql://defi:defi123@postgres:5432/defi_protocol

# Redis
REDIS_URL=redis://redis:6379/2

# Vault/Consul
VAULT_ADDR=http://vault:8200
VAULT_TOKEN=your-vault-token
CONSUL_ADDR=http://consul:8500

# Price Oracle Providers
PRICE_PROVIDERS=coingecko,chainlink,uniswap
PRICE_CACHE_TTL=300

# Risk Management
MAX_LEVERAGE=3.0
LIQUIDATION_THRESHOLD=0.8
MIN_COLLATERAL_RATIO=1.5
VOLATILITY_WINDOW=30
```

## Insurance Pool Details

### Risk Tiers

| Tier | Base APY | Min Stake | Max Leverage | Coverage Ratio | Loss Priority |
|------|----------|-----------|--------------|----------------|---------------|
| Stable | 5% | $100 | 10x | 20% | Last |
| Balanced | 12% | $1,000 | 50x | 50% | Second |
| Aggressive | 25% | $10,000 | 100x | 100% | First |

### APY Calculation

The actual APY is dynamic and calculated as:
```
Current APY = Base APY × Utilization Multiplier × (1 + Risk Premium)
```

Where the utilization multiplier follows this curve:
- 0% utilization: 0.5x multiplier
- 50% utilization: 1.0x multiplier  
- 80% utilization: 2.0x multiplier
- 95% utilization: 5.0x multiplier

### Lock Period Bonuses

| Lock Period | Bonus APY |
|-------------|-----------|
| 0 days | 0% |
| 1-30 days | +1% |
| 31-90 days | +3% |
| 91-180 days | +5% |
| 181-365 days | +10% |
| 365+ days | +15% |

## Development

### Running Locally

```bash
# Install dependencies
pip install -r requirements.in

# Run the service
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### Testing

```bash
# Run tests
pytest

# Run with coverage
pytest --cov=app --cov-report=html
```

## Architecture

The service follows a modular architecture:

```
app/
├── api/            # API endpoints
├── core/           # Core components (DeFi Manager, Config)
├── models/         # Data models
├── protocols/      # Protocol implementations
│   ├── lending.py
│   ├── auctions.py
│   ├── yield_farming.py
│   ├── liquidity.py
│   └── insurance.py  # New consolidated insurance protocol
├── services/       # Support services (Price Oracle, Risk Calculator)
└── vault_consul_integration.py  # Secure key management
```

## Monitoring

The service exposes Prometheus metrics:

- `defi_transactions_total` - Total transactions by chain/protocol/operation
- `defi_transaction_duration_seconds` - Transaction latency
- `defi_tvl_usd` - Total value locked by chain/protocol
- `defi_apy_percent` - Current APY for yield pools

## Security Considerations

1. **Key Management**: All private keys are stored in HashiCorp Vault
2. **Access Control**: OAuth2/OIDC integration for API access
3. **Rate Limiting**: Configurable per-minute rate limits
4. **Transaction Limits**: Protocol-specific transaction size limits
5. **Oracle Security**: Signed oracle data with on-chain verification
6. **Multi-sig Treasury**: Protocol treasury uses multi-signature wallets

## License

Proprietary - All rights reserved 