# Compute Resource DeFi Integration Guide

## Overview

This guide covers the integration of compute resources (Quantum, AI, Network) with PlatformQ's DeFi protocols, enabling vaults, lending, and derivatives for compute resource markets.

## Architecture

```
┌─────────────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│  Compute Markets    │     │   Oracle Service │     │ Market Aggregator   │
│ (Quantum/AI/Network)│────▶│ (Quality Scores) │◀────│   (Bundling)       │
└────────┬────────────┘     └────────┬─────────┘     └─────────────────────┘
         │                           │
         ▼                           ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        Compute Resource DeFi Layer                       │
├──────────────┬──────────────┬──────────────────┬───────────────────────┤
│Compute Vaults│Compute Lending│Compute Derivatives│ Compute Insurance    │
│• Yield       │• Resource    │• Futures         │• Quality Degradation │
│  Strategies  │  Loans       │  Contracts       │• Availability Cover  │
│• Auto-       │• Flash Loans │• Options         │• Performance         │
│  compounding │• Quality APR │• Vol Products    │• Slashing Protection │
└──────────────┴──────────────┴──────────────────┴───────────────────────┘
```

## 1. Compute Resource Vaults

### Features
- **Automated Yield Strategies**: Market arbitrage, bundle optimization, quality arbitrage
- **Lock Bonuses**: Up to 50% bonus shares for 1-year locks
- **Multi-Resource Support**: Single vaults for each resource type or hybrid vaults
- **Performance-Based Fees**: 2% management + 15% performance fee structure

### Implementation

```python
# Create a Quantum Resource Vault
from protocols import ComputeResourceVault, ComputeResourceType, ComputeStrategyType

vault = ComputeResourceVault(
    blockchain_client=blockchain,
    vault_factory_address=VAULT_FACTORY,
    quantum_market_address=QUANTUM_MARKET,
    oracle_address=ORACLE_ADDRESS,
    # ... other addresses
)

# Create vault with multiple strategies
result = await vault.create_compute_vault(
    resource_type=ComputeResourceType.QUANTUM,
    name="Quantum Yield Vault",
    symbol="qYIELD",
    strategies=[
        ComputeStrategyType.MARKET_ARBITRAGE,
        ComputeStrategyType.BUNDLE_OPTIMIZATION,
        ComputeStrategyType.QUALITY_ARBITRAGE
    ],
    management_fee=200,  # 2%
    performance_fee=1500  # 15%
)

# Deposit resources with lock
deposit_result = await vault.deposit_compute_resources(
    vault_address=result['vault_address'],
    user_address=user_address,
    resource_ids=[101, 102, 103],
    amounts=[10, 20, 15],
    lock_period_days=180  # 6-month lock for bonus
)
```

### Smart Contract Integration

```solidity
// Deploy ComputeResourceVault.sol
ComputeResourceVault vault = new ComputeResourceVault(
    "Quantum Yield Vault",
    "qYIELD", 
    resourceTokenAddress,
    quantumResourceTokenId
);

// User deposits resources
uint256[] memory ids = [101, 102, 103];
uint256[] memory amounts = [10, 20, 15];
uint256 shares = vault.deposit(ids, amounts, 180 days);
```

## 2. Compute Resource Lending

### Features
- **Quality-Based Interest Rates**: Higher quality resources get better rates
- **Flexible Collateral Types**: Compute tokens, staked tokens, LP tokens, quality bonds
- **Flash Loans**: Atomic borrowing for arbitrage and liquidations
- **Dynamic Interest Model**: Rates adjust based on utilization and quality

### Implementation

```python
# Create lending pool for AI compute
from protocols import ComputeResourceLending, ComputeCollateralType

lending = ComputeResourceLending(
    blockchain_client=blockchain,
    lending_pool_address=LENDING_POOL,
    ai_market_address=AI_MARKET,
    oracle_address=ORACLE_ADDRESS
)

# Create AI compute lending pool
pool_result = await lending.create_compute_lending_pool(
    resource_type="ai",
    initial_liquidity=Decimal("1000000"),
    reserve_factor=1000,  # 10%
    enable_quality_scoring=True
)

# Borrow AI compute resources
loan_result = await lending.borrow_compute_resources(
    pool_address=pool_result['pool_address'],
    borrower=borrower_address,
    resource_ids=[201, 202],
    amounts=[50, 75],
    duration_hours=168,  # 1 week
    collateral_type=ComputeCollateralType.STAKED_COMPUTE,
    collateral_amount=Decimal("5000"),
    loan_type=ComputeLoanType.SPOT_COMPUTE
)
```

### Interest Rate Calculation

```python
# Dynamic rate based on:
# 1. Base rate per resource type
# 2. Utilization curve (exponential above 80%)
# 3. Quality score adjustment
# 4. Loan type multiplier

base_rate = 0.03  # 3% for AI
utilization = 0.75  # 75%
avg_quality = 92  # High quality

# Quality gives discount: (92-80)/10 * 0.02 = 0.024 discount
# Final rate: 3% * utilization_multiplier - 2.4% = ~5.6% APR
```

## 3. Compute Resource Derivatives

### Features
- **Futures Contracts**: Lock in future compute capacity at fixed prices
- **Options**: Call/Put options on compute resources with Black-Scholes pricing
- **Quality-Linked Notes**: Structured products tied to quality scores
- **Volatility Swaps**: Trade compute price volatility
- **Portfolio Hedging**: Automated hedge recommendations

### Implementation

```python
# Create futures contract
from protocols import ComputeResourceDerivatives, ComputeOptionType

derivatives = ComputeResourceDerivatives(
    blockchain_client=blockchain,
    derivatives_factory_address=DERIVATIVES_FACTORY,
    quantum_market_address=QUANTUM_MARKET,
    oracle_address=ORACLE_ADDRESS
)

# Create quantum compute future
future_result = await derivatives.create_compute_future(
    resource_type="quantum",
    resource_specs={
        "min_qubits": 50,
        "min_coherence": 100,
        "topology": "full"
    },
    quantity=100,  # 100 hours
    delivery_date=datetime.utcnow() + timedelta(days=30),
    settlement_type="physical"
)

# Create call option on AI compute
option_result = await derivatives.create_compute_option(
    resource_type="ai",
    resource_specs={"accelerator": "H100", "memory": 80},
    option_type=ComputeOptionType.CALL,
    strike_price=Decimal("120"),  # $120/hour
    quantity=500,  # 500 hours
    expiration_date=datetime.utcnow() + timedelta(days=60),
    american_style=True
)

# Greeks: Delta=0.55, Gamma=0.02, Vega=15.5, Theta=-0.85
```

### Advanced Products

```python
# Quality-Linked Note
# Pays 8% coupon if average quantum quality stays above 85
note_result = await derivatives.create_quality_linked_note(
    resource_type="quantum",
    principal=Decimal("100000"),
    quality_threshold=85,
    coupon_rate=Decimal("0.08"),
    maturity_date=datetime.utcnow() + timedelta(days=365),
    barrier_type="american"  # Continuous monitoring
)

# Volatility Swap on AI compute prices
vol_swap = await derivatives.create_compute_volatility_swap(
    resource_type="ai",
    notional=Decimal("1000000"),
    strike_volatility=Decimal("0.25"),  # 25% annualized
    maturity_date=datetime.utcnow() + timedelta(days=90),
    observation_frequency="daily"
)
```

## 4. Compute Resource Insurance

### Features
- **Quality Degradation Insurance**: Protection against resource quality falling below thresholds
- **Availability Insurance**: Coverage for downtime or resource unavailability
- **Performance Guarantee Insurance**: Protection against performance benchmarks not being met
- **Slashing Insurance**: Coverage for staked compute providers against penalties
- **Smart Contract Coverage**: Protection against smart contract exploits

### Implementation

```python
# Create insurance protocol instance
from protocols import ComputeResourceInsurance, InsuranceCoverageType

insurance = ComputeResourceInsurance(
    blockchain_client=blockchain,
    insurance_factory_address=INSURANCE_FACTORY,
    oracle_address=ORACLE_ADDRESS,
    quality_oracle_address=QUALITY_ORACLE,
    availability_monitor_address=AVAILABILITY_MONITOR
)

# Create insurance pool for quality degradation
pool_result = await insurance.create_insurance_pool(
    resource_type="quantum",
    coverage_type=InsuranceCoverageType.QUALITY_DEGRADATION,
    initial_capital=Decimal("5000000"),
    target_size=Decimal("20000000"),
    reserve_ratio=Decimal("0.2")  # 20% reserves
)

# Purchase insurance policy
policy_result = await insurance.purchase_policy(
    pool_id=pool_result['pool_id'],
    policyholder=user_address,
    resource_ids=[101, 102, 103],
    coverage_amount=Decimal("100000"),
    coverage_period_days=365,
    deductible_override=Decimal("0.05"),  # 5% deductible
    bundle_discount=True  # Get 15% discount
)
```

### Coverage Types

#### Quality Degradation Insurance
```python
# Protects against quality score drops
# Example: Quantum coherence time degrades below 100μs
# Automatic claim trigger if quality drops 20%+

# File claim for quality degradation
claim = await insurance.file_claim(
    policy_id=policy_result['policy_id'],
    claim_type=InsuranceCoverageType.QUALITY_DEGRADATION,
    incident_data={
        'resource_id': 101,
        'baseline_quality': 95,
        'current_quality': 72,
        'degradation_percentage': 24.2,
        'timestamp': datetime.utcnow().timestamp()
    },
    requested_amount=Decimal("25000"),
    evidence_hashes=["QmEvidence1", "QmEvidence2"]
)
```

#### Availability Insurance
```python
# Covers downtime and unavailability
# Auto-triggers claims for downtime > 1 hour

# Premium calculation factors:
# - Historical availability rate (99.9% = low risk)
# - Resource criticality
# - Coverage amount
```

#### Slashing Insurance for Providers
```python
# Stake and get automatic slashing insurance
stake_result = await insurance.stake_for_slashing_insurance(
    provider_address=provider_address,
    stake_amount=Decimal("10000"),
    resource_type="ai",
    coverage_multiplier=Decimal("10")  # 10x coverage
)

# Benefits:
# - No deductible on slashing events
# - Automatic claim processing
# - Lower premiums (0.5% annual)
# - Coverage up to 100x stake amount
```

### Risk Assessment

```python
# Risk factors considered:
# 1. Resource quality volatility
# 2. Provider track record
# 3. Availability history
# 4. Market conditions

# Risk levels determine premium multipliers:
# - LOW: 0.8x base premium
# - MEDIUM: 1.0x base premium  
# - HIGH: 1.5x base premium
# - CRITICAL: 2.5x base premium
```

### Claim Processing

```python
# Automated investigation process
# 1. Quality claims: Oracle verification
# 2. Availability claims: Monitor verification
# 3. Performance claims: Benchmark verification
# 4. Slashing claims: On-chain verification

# Claim statuses:
# - PENDING: Initial filing
# - INVESTIGATING: Automated verification
# - APPROVED: Ready for payout
# - REJECTED: Failed verification
# - PAID: Funds transferred
```

### Liquidity Provision

```python
# Provide liquidity to insurance pools
lp_result = await insurance.provide_liquidity(
    pool_id=pool_id,
    provider=liquidity_provider,
    amount=Decimal("1000000")
)

# LP Benefits:
# - Earn premiums from policies
# - Share in pool profits
# - Governance rights
# - Expected APY: 8-12%
```

## 5. API Endpoints

### Vault Operations
- `POST /api/v1/compute-defi/vaults/create` - Create compute vault
- `POST /api/v1/compute-defi/vaults/deposit` - Deposit resources
- `POST /api/v1/compute-defi/vaults/{vault}/harvest` - Harvest yields
- `GET /api/v1/compute-defi/vaults/{vault}/performance` - Get performance

### Lending Operations
- `POST /api/v1/compute-defi/lending/pools/create` - Create lending pool
- `POST /api/v1/compute-defi/lending/borrow` - Borrow resources
- `POST /api/v1/compute-defi/lending/liquidate/{loan}` - Liquidate loan
- `GET /api/v1/compute-defi/lending/pools/{pool}/stats` - Pool statistics

### Derivatives Operations
- `POST /api/v1/compute-defi/derivatives/futures/create` - Create future
- `POST /api/v1/compute-defi/derivatives/options/create` - Create option
- `POST /api/v1/compute-defi/derivatives/hedge` - Get hedge recommendations
- `GET /api/v1/compute-defi/derivatives/pricing/{contract}` - Get pricing

### Insurance Operations
- `POST /api/v1/compute-defi/insurance/pools/create` - Create insurance pool
- `POST /api/v1/compute-defi/insurance/policies/purchase` - Purchase policy
- `POST /api/v1/compute-defi/insurance/claims/file` - File claim
- `POST /api/v1/compute-defi/insurance/liquidity/provide` - Provide liquidity
- `POST /api/v1/compute-defi/insurance/stake/slashing` - Stake for slashing insurance
- `GET /api/v1/compute-defi/insurance/policies/{id}` - Get policy details
- `GET /api/v1/compute-defi/insurance/claims/{id}` - Get claim status
- `GET /api/v1/compute-defi/insurance/pools/{id}/stats` - Pool statistics

### Analytics
- `GET /api/v1/compute-defi/analytics/market-overview` - Market overview
- `GET /api/v1/compute-defi/analytics/arbitrage-opportunities` - Arbitrage scanner

## 6. Yield Strategies

### Market Arbitrage
```python
# Detect price differences between spot and futures
opportunities = await vault._find_arbitrage_opportunities("quantum")
for opp in opportunities:
    if opp['profit_margin'] > 0.02:  # 2% threshold
        await vault.execute_market_arbitrage(vault_address, opp['id'])
```

### Bundle Optimization
```python
# Optimize resource bundles for better pricing
bundle = await vault.optimize_resource_bundle(
    vault_address=vault_address,
    workload_template="quantum_ml_hybrid",
    budget_limit=Decimal("10000")
)
# Achieves 5-8% discount through bundling
```

### Quality Arbitrage
```python
# Buy underpriced high-quality resources
# Example: 95 quality resource priced same as 80 quality
# Capture the quality premium differential
```

## 7. Risk Management

### Liquidation Thresholds
- Compute Token Collateral: 80% LTV, 85% liquidation
- Staked Compute: 85% LTV, 90% liquidation  
- Future Compute: 60% LTV, 65% liquidation
- LP Tokens: 70% LTV, 75% liquidation
- Quality Bonds: 90% LTV, 95% liquidation

### Margin Requirements
- Futures: 5-15% based on volatility
- Options: 150% for calls, 100% for puts
- Maintenance Margin: 3% for futures

## 8. Integration Examples

### Complete DeFi Flow
```python
# 1. User deposits quantum resources into vault
shares = await vault.deposit_compute_resources(...)

# 2. Purchase insurance for deposited resources
policy = await insurance.purchase_policy(
    pool_id=quality_insurance_pool,
    resource_ids=deposited_resources,
    coverage_amount=deposit_value,
    coverage_period_days=365
)

# 3. Vault deploys capital across strategies
await vault.execute_market_arbitrage(...)
await vault.optimize_resource_bundle(...)

# 4. User can borrow against vault shares
loan = await lending.borrow_compute_resources(
    collateral_type=ComputeCollateralType.LP_TOKEN,
    collateral_amount=shares * share_price
)

# 5. Hedge exposure with options
await derivatives.create_compute_option(
    option_type=ComputeOptionType.PUT,
    strike_price=current_price * 0.9  # 10% OTM put
)

# 6. If provider, stake with slashing insurance
await insurance.stake_for_slashing_insurance(
    stake_amount=Decimal("10000"),
    resource_type="quantum",
    coverage_multiplier=Decimal("10")
)
```

## 9. Security Considerations

1. **Oracle Manipulation**: Multi-oracle setup with outlier detection
2. **Flash Loan Attacks**: Reentrancy guards and balance checks
3. **Quality Score Gaming**: Time-weighted average scoring
4. **Liquidation Cascades**: Progressive liquidation incentives
5. **Smart Contract Audits**: All contracts audited by top firms

## 10. Performance Metrics

### Expected Returns
- Vault APY: 15-45% depending on strategies
- Lending Supply APY: 5-12% based on utilization
- Arbitrage Profits: 2-8% per opportunity
- Option Premiums: 5-15% of notional
- Insurance Pool APY: 8-12% for liquidity providers
- Insurance Premiums: 1.5-4% annual (coverage dependent)

### Risk Metrics
- Sharpe Ratio: Target 1.5-2.5
- Maximum Drawdown: < 15%
- Value at Risk (95%): < 10% of portfolio

## 11. Future Enhancements

## 5. Automated Market Maker (AMM) - Now Available!

### Pool Types
- **Stable Pools**: Resource/USDC pairs with 0.05% fees
- **Volatile Pools**: Cross-resource pairs with 0.3% fees
- **Concentrated Pools**: V3-style capital efficiency

### Example Usage
```python
# Create liquidity pool
pool = await amm.create_pool(
    token0="0xQuantumToken",
    token1="0xUSDC",
    pool_type="stable",
    initial_price=100
)

# Add liquidity
liquidity = await amm.add_liquidity(
    pool_address=pool['pool_address'],
    amount0=10,      # 10 Quantum
    amount1=1000     # 1000 USDC
)

# Swap tokens
swap = await amm.swap(
    token_in="0xQuantumToken",
    token_out="0xAIToken",
    amount=5,
    max_slippage=0.01
)
```

For detailed AMM documentation, see [Compute AMM Integration Guide](../compute-amm-integration.md)

## Future Enhancements

1. **Cross-Chain Bridges**: Access compute resources on other chains
2. **Synthetic Resources**: Create synthetic compute assets
3. **Perpetual Futures**: 24/7 tradeable compute futures with funding rates
4. **DAO Governance**: Decentralized parameter adjustment
5. **Zero-Knowledge Proofs**: Private compute resource verification
6. **Layer 2 Scaling**: Rollup solutions for high-frequency trading
7. **Yield Aggregators**: Auto-compound LP and vault rewards 