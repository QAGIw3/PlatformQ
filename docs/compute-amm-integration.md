# Compute Resource AMM Integration Guide

## Overview

The Compute Resource Automated Market Maker (AMM) provides liquid markets for trading compute resources (Quantum, AI, Network) against stablecoins and between resource types. It enables price discovery, instant swaps, and yield generation for liquidity providers.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Compute Resource AMM                              │
├─────────────────────┬─────────────────────┬────────────────────────────┤
│   Stable Pools      │  Volatile Pools     │  Concentrated Pools       │
│   (Resource/USDC)   │  (Resource/Resource)│  (V3-style)               │
│   • 0.05% fee       │  • 0.3% fee         │  • 0.1% fee               │
│   • Low slippage    │  • Cross-resource   │  • Capital efficiency     │
│   • Price stability │  • Higher yields    │  • Custom ranges          │
└─────────────────────┴─────────────────────┴────────────────────────────┘
                                │
                    ┌───────────┴───────────┐
                    │   Smart Router        │
                    │   • Path finding      │
                    │   • Split routing     │
                    │   • Gas optimization  │
                    └───────────────────────┘
```

## Pool Types

### 1. Stable Pools (Resource/USDC)
Designed for low slippage trades between compute resources and stablecoins.

```python
# Create a Quantum/USDC pool
{
    "token0": "0xQuantumToken",
    "token1": "0xUSDC",
    "pool_type": "stable",
    "initial_price": 100,  # 1 Quantum = 100 USDC
    "fee_tier": 0.0005    # 0.05%
}
```

### 2. Volatile Pools (Cross-Resource)
Enable direct swaps between different compute resource types.

```python
# Create a Quantum/AI pool
{
    "token0": "0xQuantumToken",
    "token1": "0xAIToken",
    "pool_type": "volatile",
    "initial_price": 2.5,  # 1 Quantum = 2.5 AI
    "fee_tier": 0.003     # 0.3%
}
```

### 3. Concentrated Liquidity Pools
V3-style pools for capital-efficient liquidity provision.

```python
# Create concentrated pool with custom fee
{
    "token0": "0xNetworkToken",
    "token1": "0xUSDC",
    "pool_type": "concentrated",
    "initial_price": 50,
    "fee_tier": 0.001    # 0.1%
}
```

## Liquidity Provision

### Adding Liquidity

```python
# Example: Add liquidity to Quantum/USDC pool
import requests

# 1. Get optimal amounts
pool_info = requests.get(
    "http://localhost:8015/api/v1/compute-amm/pools/0xPoolAddress"
).json()

# 2. Add liquidity
response = requests.post(
    "http://localhost:8015/api/v1/compute-amm/liquidity/add",
    json={
        "pool_address": "0xPoolAddress",
        "amount0": "100",      # 100 Quantum tokens
        "amount1": "10000",    # 10,000 USDC
        "min_amount0": "99",   # Slippage protection
        "min_amount1": "9900"
    },
    headers={"Authorization": "Bearer YOUR_TOKEN"}
)

result = response.json()
print(f"LP tokens received: {result['liquidity']['lp_tokens']}")
print(f"Share of pool: {result['liquidity']['share_of_pool']:.2%}")
```

### LP Position Management

```python
# Get all positions for a user
positions = requests.get(
    "http://localhost:8015/api/v1/compute-amm/liquidity/positions/0xUserAddress"
).json()

for position in positions['positions']:
    print(f"Pool: {position['pool_address']}")
    print(f"Value: ${position['current_value']['usd']:.2f}")
    print(f"Impermanent Loss: {position['impermanent_loss']:.2%}")
    print(f"Fees Earned: ${position['estimated_fees_earned']:.2f}")
```

## Token Swaps

### Simple Swap

```python
# Swap 10 Quantum tokens for USDC
quote = requests.post(
    "http://localhost:8015/api/v1/compute-amm/swap/quote",
    json={
        "token_in": "0xQuantumToken",
        "token_out": "0xUSDC",
        "amount_in": "10"
    }
).json()

print(f"Expected output: {quote['amount_out']} USDC")
print(f"Price impact: {quote['price_impact']:.2%}")
print(f"Route: {quote['route']}")

# Execute swap if acceptable
if quote['price_impact'] < 2:  # Less than 2% impact
    swap_result = requests.post(
        "http://localhost:8015/api/v1/compute-amm/swap",
        json={
            "token_in": "0xQuantumToken",
            "token_out": "0xUSDC",
            "amount": "10",
            "direction": "exact_in",
            "max_slippage": 0.01
        },
        headers={"Authorization": "Bearer YOUR_TOKEN"}
    ).json()
```

### Multi-Hop Swaps

The router automatically finds the best path through multiple pools:

```
Quantum → USDC → AI (better liquidity)
vs
Quantum → AI (direct but less liquidity)
```

## Integration with Other DeFi Protocols

### 1. Vault Integration

Vaults use the AMM for yield strategies:

```python
# Vault arbitrage strategy
class AMMArbitrageStrategy:
    async def execute(self, vault_funds):
        # Find price discrepancies
        quantum_price_amm = await get_amm_price("quantum", "usdc")
        quantum_price_market = await get_market_price("quantum", "usdc")
        
        if quantum_price_market > quantum_price_amm * 1.01:
            # Buy from AMM, sell on market
            await amm.swap("usdc", "quantum", vault_funds)
            await market.sell("quantum", vault_funds)
```

### 2. Lending Protocol

Use AMM for liquidations:

```python
# Liquidate collateral through AMM
async def liquidate_position(loan_id):
    collateral = await get_loan_collateral(loan_id)
    
    # Swap collateral to repayment token
    await amm.swap(
        token_in=collateral.token,
        token_out="USDC",
        amount=collateral.amount,
        max_slippage=0.05  # Allow 5% slippage for liquidation
    )
```

### 3. Insurance Protocol

AMM provides liquidity for claim payouts:

```python
# Convert insurance pool assets to payout currency
async def process_claim_payout(claim_amount, payout_token):
    pool_assets = await get_insurance_pool_assets()
    
    for asset in pool_assets:
        if asset.token != payout_token:
            await amm.swap(
                token_in=asset.token,
                token_out=payout_token,
                amount=asset.amount
            )
```

### 4. Price Oracle Integration

The AMM uses the Price Aggregator Oracle for:
- Initial price validation
- Manipulation detection
- TWAP calculations

```python
# AMM checks oracle price before allowing large trades
oracle_price = await price_oracle.get_price("quantum", "usdc")
amm_price = pool.get_spot_price()

if abs(amm_price - oracle_price) / oracle_price > 0.05:
    # More than 5% deviation - possible manipulation
    raise PriceManipulationError()
```

## Analytics and Monitoring

### Pool Analytics

```python
# Get pool performance metrics
analytics = requests.get(
    "http://localhost:8015/api/v1/compute-amm/analytics/liquidity"
).json()

print(f"Total TVL: ${analytics['total_tvl_usd']:,.2f}")
print(f"Average APR: {analytics['average_apr']:.2f}%")
print(f"Pool Count: {analytics['pool_count']}")

# Resource breakdown
for resource, tvl in analytics['tvl_by_resource'].items():
    print(f"{resource.upper()} TVL: ${tvl:,.2f}")
```

### Volume Analytics

```python
# Get 24h volume
volume = requests.get(
    "http://localhost:8015/api/v1/compute-amm/analytics/volume?period_hours=24"
).json()

print(f"24h Volume: ${volume['total_volume']:,.2f}")
for pool in volume['pools']:
    print(f"{pool['token0']}/{pool['token1']}: ${pool['volume_24h']:,.2f}")
```

### Price Impact Analysis

```python
# Analyze price impact for different amounts
impact_analysis = requests.get(
    "http://localhost:8015/api/v1/compute-amm/analytics/price-impact",
    params={
        "token_in": "0xQuantumToken",
        "token_out": "0xUSDC",
        "test_amounts": [100, 1000, 10000]
    }
).json()

for analysis in impact_analysis['impact_analysis']:
    print(f"Amount: {analysis['amount_in']}")
    print(f"Price Impact: {analysis['price_impact']:.2f}%")
    print(f"Execution Price: {analysis['execution_price']}")
    print("---")
```

## Fee Structure

| Pool Type | Swap Fee | Protocol Fee | LP Fee |
|-----------|----------|--------------|--------|
| Stable | 0.05% | 0.01% | 0.04% |
| Volatile | 0.30% | 0.05% | 0.25% |
| Concentrated | 0.10% | 0.02% | 0.08% |

## Security Considerations

1. **Price Manipulation Protection**
   - Maximum 10% price impact per trade
   - Oracle price validation
   - Multi-block TWAP for large trades

2. **Sandwich Attack Prevention**
   - Commit-reveal scheme for large trades
   - Maximum slippage enforcement
   - Priority fee auction for MEV protection

3. **Liquidity Protections**
   - Minimum initial liquidity requirements
   - Lock period for new pools
   - Emergency pause mechanism

## Best Practices

### For Liquidity Providers

1. **Diversify Positions**: Spread liquidity across multiple pools
2. **Monitor IL**: Track impermanent loss, especially in volatile pools
3. **Harvest Rewards**: Regularly claim trading fees
4. **Use Stable Pools**: For lower risk, focus on resource/USDC pools

### For Traders

1. **Check Price Impact**: Always review quotes before swapping
2. **Set Slippage**: Use appropriate slippage tolerance
3. **Compare Routes**: Check if direct or multi-hop is better
4. **Time Trades**: Avoid high congestion periods

### For Integrators

1. **Use Quote Endpoint**: Always get quotes before executing
2. **Handle Failures**: Implement retry logic with exponential backoff
3. **Monitor Gas**: Adjust gas prices based on network conditions
4. **Cache Pool Data**: Reduce API calls by caching pool information

## Future Enhancements

1. **Flash Swaps**: Borrow tokens within a transaction
2. **Limit Orders**: Place orders at specific prices
3. **Auto-Routing**: ML-based optimal path finding
4. **Cross-Chain Swaps**: Trade across different blockchains
5. **Options Integration**: Use AMM liquidity for options market making
6. **Yield Aggregation**: Auto-compound LP rewards
7. **NFT Pools**: Fractional NFT compute resource trading

## Example Integration

```python
from typing import Decimal
import asyncio
from compute_amm_client import ComputeAMM

async def main():
    # Initialize AMM client
    amm = ComputeAMM(
        api_url="http://localhost:8015",
        api_key="YOUR_API_KEY"
    )
    
    # Create a new pool
    pool = await amm.create_pool(
        token0="0xQuantumToken",
        token1="0xUSDC",
        pool_type="stable",
        initial_price=Decimal("100")
    )
    
    print(f"Created pool: {pool['pool_address']}")
    
    # Add liquidity
    liquidity = await amm.add_liquidity(
        pool_address=pool['pool_address'],
        amount0=Decimal("10"),
        amount1=Decimal("1000")
    )
    
    print(f"Added liquidity, received {liquidity['lp_tokens']} LP tokens")
    
    # Perform swap
    swap = await amm.swap(
        token_in="0xQuantumToken",
        token_out="0xUSDC",
        amount=Decimal("1"),
        max_slippage=Decimal("0.01")
    )
    
    print(f"Swapped 1 Quantum for {swap['amount_out']} USDC")
    
    # Check position
    positions = await amm.get_positions("0xMyAddress")
    for position in positions:
        print(f"Position value: ${position['current_value']['usd']}")
        print(f"Impermanent loss: {position['impermanent_loss']:.2%}")

if __name__ == "__main__":
    asyncio.run(main())
```

## Conclusion

The Compute Resource AMM provides essential liquidity infrastructure for the PlatformQ ecosystem. By enabling efficient price discovery and low-slippage trades, it creates a foundation for advanced DeFi applications on compute resources. The integration with oracles, multi-pool routing, and comprehensive analytics makes it a robust solution for decentralized compute resource trading. 