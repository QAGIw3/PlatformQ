# Resource Staking & Infrastructure Vaults

This document describes the new Resource Staking & Delegation System and Infrastructure Vaults (Yearn-style) features added to the PlatformQ DeFi Protocol Service.

## Overview

### Resource Staking & Delegation System
A comprehensive staking system that enables:
- **Provider Staking**: Lock resources to earn guaranteed yields
- **Delegation Pools**: Users delegate tokens to professional operators
- **Slashing Mechanisms**: Penalize poor performance/downtime
- **Compound Yields**: Stake LP tokens from AMM pools
- **Auto-compounding**: Reinvest rewards automatically

### Infrastructure Vaults (Yearn-style)
Automated yield optimization strategies:
- **Resource Arbitrage Vault**: Exploit price differences across regions/tiers
- **Flash Provisioning Vault**: Automated flash loan strategies
- **Lending Optimizer**: Move resources between lending pools for best rates
- **Hedged Mining Vault**: Provide liquidity while hedging impermanent loss
- **Multi-strategy Vault**: Combine multiple strategies based on market conditions

## Architecture

### Smart Contracts

#### ResourceStaking.sol
- Manages staking pools for resource tokens and LP tokens
- Implements delegation pools with operator fees
- Supports slashing for poor performance
- Auto-compound functionality with fees
- Lock periods with guaranteed yields

#### InfrastructureVault.sol
- ERC20 vault shares representing deposited resources
- Multiple strategy support with configurable allocations
- Management and performance fees
- Emergency shutdown mechanism
- Harvest cycles for compounding gains

#### Strategy Contracts
- **ResourceArbitrageVault**: Flash loan arbitrage between pools
- **LendingOptimizerVault**: Optimizes between lending and staking yields
- Additional strategies can be added following the IStrategy interface

### Backend Services

#### StakingProtocol (`app/protocols/staking_protocol.py`)
- Manages staking operations and delegation
- Monitors stakes for expiration and rewards
- Auto-compound worker for enabled users
- Slashing enforcement

#### VaultProtocol (`app/protocols/vault_protocol.py`)
- Manages vault lifecycle and strategies
- Harvest automation for strategies
- Performance tracking and APY calculations
- Emergency shutdown handling

## API Endpoints

### Staking Endpoints (`/api/v1/staking`)

#### POST `/pools`
Create a new staking pool (requires OPERATOR role)

#### POST `/delegation-pools`
Create a delegation pool for professional operators

#### POST `/stake`
Stake tokens in a pool with lock duration

#### POST `/delegate`
Delegate a stake to an operator pool

#### POST `/withdraw`
Withdraw staked tokens (after lock period)

#### POST `/claim-rewards`
Claim accumulated rewards

#### POST `/auto-compound`
Enable/disable auto-compounding

#### GET `/stats`
Get overall staking statistics

#### GET `/user/stakes`
Get all stakes for the current user

### Vault Endpoints (`/api/v1/vaults`)

#### POST `/`
Create a new infrastructure vault (requires VAULT_CREATOR role)

#### POST `/{vault_address}/strategies`
Add a strategy to a vault (requires STRATEGIST role)

#### POST `/deposit`
Deposit resources into a vault

#### POST `/withdraw`
Withdraw from a vault with slippage protection

#### POST `/strategies/{strategy_address}/harvest`
Harvest a strategy (keepers or strategists)

#### GET `/{vault_address}/stats`
Get vault statistics including TVL and APY

#### GET `/{vault_address}/performance`
Get detailed performance metrics

## Usage Examples

### Staking Resources

```python
# Create a staking pool
POST /api/v1/staking/pools
{
    "token_id": 1,  # GPU resources
    "min_stake_amount": 1000000000000000000,  # 1 token
    "is_lp": false
}

# Stake tokens
POST /api/v1/staking/stake
{
    "pool_id": 1,
    "amount": 10000000000000000000,  # 10 tokens
    "lock_duration": 2592000  # 30 days
}

# Enable auto-compound
POST /api/v1/staking/auto-compound
{
    "enable": true
}
```

### Using Vaults

```python
# Create a vault
POST /api/v1/vaults
{
    "resource_token_id": 0,  # CPU resources
    "name": "CPU Yield Optimizer",
    "symbol": "yvCPU",
    "management_fee": 200,  # 2%
    "performance_fee": 1000  # 10%
}

# Add arbitrage strategy
POST /api/v1/vaults/{vault_address}/strategies
{
    "strategy_type": "arbitrage",
    "strategy_config": {
        "min_profit_bps": 50,
        "max_slippage_bps": 100
    },
    "debt_ratio": 3000  # 30% allocation
}

# Deposit resources
POST /api/v1/vaults/deposit
{
    "vault_address": "0x...",
    "amount": 5000000000000000000  # 5 CPU tokens
}
```

## Security Considerations

### Staking Security
- Lock periods enforced on-chain
- Slashing limited to 50% maximum
- Delegation pool operators vetted
- Time delays for fee changes

### Vault Security
- Emergency shutdown by guardians
- Strategy debt limits
- Slippage protection on withdrawals
- Multi-sig for critical operations

## Integration with Existing Systems

### Resource Token Integration
- Both systems use the existing ResourceToken (ERC-1155)
- Compatible with AMM liquidity pools
- Works with lending markets

### Settlement Integration
- Flash settlements can use staked resources
- Vault strategies can participate in flash loans
- Integrated with settlement coordinator

## Deployment

1. Deploy contracts using the updated script:
```bash
npx hardhat run scripts/deploy_infrastructure_defi.js --network <network>
```

2. Set environment variables for the service:
```bash
STAKING_CONTRACT_ADDRESS=0x...
VAULT_FACTORY_ADDRESS=0x...
```

3. Grant necessary roles to operators and strategists

## Monitoring

### Staking Metrics
- Total value staked
- Active stakers count
- Average APY across pools
- Slashing events

### Vault Metrics
- TVL per vault
- Strategy performance
- Harvest frequency
- User returns

## Future Enhancements

1. **Cross-chain Staking**: Stake on one chain, earn on another
2. **Options Strategies**: Covered calls on staked resources
3. **Governance Integration**: veToken model for voting power
4. **Risk Tranches**: Junior/Senior tranches for vaults
5. **Automated Market Makers**: Concentrated liquidity for resource pairs 