# Infrastructure Derivatives Platform

## Overview

The Infrastructure Derivatives Platform provides advanced financial instruments for infrastructure resources including options, perpetual futures, and automated market making for derivatives.

## Features

### 1. Options Trading

- **European & American Options**: Support for both exercise styles
- **Call & Put Options**: Hedge against price movements or speculate
- **NFT-based Options**: Each option is an ERC721 NFT for easy transfer
- **Automated Pricing**: Black-Scholes based pricing with volatility smile
- **Greeks Calculation**: Real-time delta, gamma, theta, vega calculations

### 2. Perpetual Futures

- **No Expiry**: Trade infrastructure resources without expiration dates
- **High Leverage**: Up to 20x leverage for capital efficiency
- **Funding Rate Mechanism**: Keeps perps aligned with spot prices
- **Cross-Margin**: Efficient margin usage across positions
- **Liquidation Engine**: Automated liquidation for under-collateralized positions

### 3. Options AMM

- **Automated Market Making**: Provide liquidity for options markets
- **Dynamic Pricing**: Volatility-based pricing with utilization adjustments
- **LP Rewards**: Earn fees from option premiums
- **Risk Management**: Built-in hedging and position limits

## Smart Contracts

### ResourceOptions.sol
- Write and trade options on infrastructure resources
- Collateral management for writers
- Exercise and settlement logic

### ResourcePerpetuals.sol
- Perpetual futures with funding rates
- Position management and liquidations
- Insurance fund for bad debt

### OptionsAMM.sol
- Liquidity pools for automated options trading
- Premium calculation and risk management
- LP token management

## API Endpoints

### Options
- `POST /api/v1/derivatives/options/write` - Write a new option
- `POST /api/v1/derivatives/options/buy` - Buy an option
- `POST /api/v1/derivatives/options/exercise` - Exercise an option
- `GET /api/v1/derivatives/options/{id}` - Get option details
- `GET /api/v1/derivatives/options/{id}/greeks` - Calculate Greeks

### Perpetuals
- `POST /api/v1/derivatives/perpetuals/open` - Open position
- `POST /api/v1/derivatives/perpetuals/close` - Close position
- `POST /api/v1/derivatives/perpetuals/add-margin` - Add margin
- `GET /api/v1/derivatives/perpetuals/position/{resource}` - Get position info
- `GET /api/v1/derivatives/perpetuals/markets` - List all markets

### Options AMM
- `POST /api/v1/derivatives/amm/create-pool` - Create liquidity pool
- `POST /api/v1/derivatives/amm/add-liquidity` - Add liquidity
- `POST /api/v1/derivatives/amm/quote` - Get option premium quote
- `GET /api/v1/derivatives/amm/pools` - List all pools

### Market Data
- `GET /api/v1/derivatives/market/{resource}` - Get market data
- `GET /api/v1/derivatives/stats` - Platform statistics

## Usage Examples

### Writing a Call Option

```python
# Write a CPU call option
POST /api/v1/derivatives/options/write
{
    "resource_token_id": 0,  # CPU
    "strike_price": "60000000000000000",  # 0.06 ETH
    "expiry": "2024-12-31T00:00:00Z",
    "option_type": "call",
    "style": "european",
    "amount": "1000000000000000000"  # 1 CPU hour
}
```

### Opening a Perpetual Long

```python
# Open long position on GPU resources
POST /api/v1/derivatives/perpetuals/open
{
    "resource_token_id": 1,  # GPU
    "size": "100000000000000000",  # 0.1 GPU hour
    "margin": "10000000000000000",  # 0.01 ETH margin
    "is_long": true
}
```

### Creating Options Pool

```python
# Create options AMM pool for storage
POST /api/v1/derivatives/amm/create-pool
{
    "resource_token_id": 2,  # Storage
    "resource_amount": "1000000000000000000000",  # 1000 GB
    "stablecoin_amount": "1000000000",  # 1000 USDC
    "base_iv": 5000  # 50% implied volatility
}
```

## Risk Management

### Options Risk
- **Collateral Requirements**: 100% collateral for both calls and puts
- **Early Exercise**: Only for American options
- **Settlement**: Automatic at expiry for European options

### Perpetuals Risk
- **Minimum Margin**: 5% initial margin requirement
- **Liquidation Threshold**: 2.5% maintenance margin
- **Insurance Fund**: Covers bad debt from liquidations
- **Funding Payments**: Every 8 hours to align with spot

### AMM Risk
- **Utilization Limits**: Prevents over-exposure
- **Dynamic IV**: Adjusts with market conditions
- **Fee Structure**: LP fees and protocol fees

## Security Features

1. **Role-Based Access**: Market makers, oracles, liquidators
2. **Emergency Shutdown**: Guardian can pause in emergencies
3. **Slippage Protection**: For large trades
4. **Oracle Security**: Multiple price sources
5. **Reentrancy Guards**: On all state-changing functions

## Deployment

1. Deploy contracts using the deployment script:
```bash
npx hardhat run scripts/deploy_infrastructure_defi.js --network <network>
```

2. Configure environment variables:
```env
OPTIONS_CONTRACT_ADDRESS=<deployed_address>
PERPETUALS_CONTRACT_ADDRESS=<deployed_address>
OPTIONS_AMM_ADDRESS=<deployed_address>
SETTLEMENT_TOKEN_ADDRESS=<USDC_address>
```

3. Initialize oracle prices and create markets

4. Start the DeFi protocol service with derivatives enabled

## Integration

The derivatives platform integrates with:
- **Resource AMM**: For spot price discovery
- **Infrastructure Vaults**: For yield strategies using options
- **Staking System**: Collateral can be staked resources
- **Flash Loans**: For arbitrage and liquidations

## Future Enhancements

1. **Structured Products**: Covered calls, protective puts
2. **Exotic Options**: Barriers, lookbacks, Asians
3. **Portfolio Margin**: Cross-margining between options and perps
4. **Social Trading**: Copy trading for derivatives
5. **Analytics Dashboard**: Advanced risk metrics and P&L tracking 