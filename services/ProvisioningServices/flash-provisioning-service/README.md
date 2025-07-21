# Flash Provisioning Service

Enables instant resource provisioning using flash loans for just-in-time scaling and atomic resource swaps.

## Overview

The Flash Provisioning Service provides:
- **Instant Resource Access**: Provision resources immediately without upfront payment
- **Atomic Resource Swaps**: Convert between resource types in a single transaction
- **Burst Capacity**: Handle sudden demand spikes with automatic scaling
- **Just-in-Time Scaling**: Automatic capacity adjustment based on utilization

## Key Features

### Flash Provisioning
- Borrow resources instantly using flash loans
- Pay only fees (0.05% - 0.2% depending on resource type)
- Resources must be returned within the same transaction
- Ideal for short-term computational tasks

### Atomic Swaps
- Exchange one resource type for another atomically
- No intermediate holding of assets
- Protected against slippage
- Leverages AMM pools for pricing

### JIT Scaling
- Monitor resource utilization in real-time
- Automatically provision additional capacity when needed
- Scale down when demand decreases
- Configurable thresholds and cooldown periods

## API Endpoints

### Flash Provision
```bash
POST /api/v1/flash/provision
{
  "resource_type": "cpu",
  "amount": 100,
  "tier": "premium",
  "duration": 3600,
  "region": "us-east-1",
  "receiver_address": "0x...",
  "max_price": 0.10
}
```

### Flash Swap
```bash
POST /api/v1/flash/swap
{
  "from_token_id": 1001,
  "from_amount": 100,
  "to_resource_type": "gpu",
  "to_amount": 10,
  "max_slippage": 0.03
}
```

### Burst Provisioning
```bash
POST /api/v1/flash/burst
{
  "resource_type": "gpu",
  "burst_amount": 50,
  "duration": 1800,
  "max_price": 1.0
}
```

### JIT Scaling Configuration
```bash
POST /api/v1/flash/jit-scaling/cpu
{
  "resource_type": "cpu",
  "enabled": true,
  "min_capacity": 100,
  "max_capacity": 10000,
  "scale_up_threshold": 0.8,
  "scale_down_threshold": 0.2,
  "cooldown_period": 300
}
```

## Smart Contracts

### FlashResourceProvider
- Implements ERC-3156 flash loan standard for ERC-1155 tokens
- Supports batch flash loans
- Enables atomic swaps through AMM integration
- Configurable fees per resource type

## Configuration

```yaml
# Environment variables
BLOCKCHAIN_RPC_URL: "https://eth-mainnet.alchemyapi.io/v2/..."
BLOCKCHAIN_CHAIN_ID: 1
FLASH_PROVIDER_ADDRESS: "0x..."
RESOURCE_TOKEN_ADDRESS: "0x..."
```

## Monitoring

The service provides comprehensive metrics:
- Active provisions count
- Resource utilization by type
- Flash loan volume and fees
- JIT scaling events
- Capacity predictions

## Security Considerations

1. **Flash Loan Safety**: All borrowed resources must be returned within the same transaction
2. **Trusted Receivers**: Only whitelisted contracts can receive flash provisions
3. **Fee Management**: Fees are automatically calculated and enforced
4. **Slippage Protection**: Atomic swaps include configurable slippage limits

## Integration Example

```python
from platformq_flash_client import FlashProvisioningClient

client = FlashProvisioningClient(api_url="https://api.platformq.io")

# Flash provision GPU resources
result = await client.flash_provision(
    resource_type="gpu",
    amount=10,
    duration=3600,
    receiver_address="0x...",
    callback_data=b"job-123"
)

# The receiver contract must implement IFlashResourceReceiver
# and return the resources + fee within the same transaction
```

## Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
pytest tests/

# Start service
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

## Architecture

The service integrates with:
- **Resource Token Contract**: ERC-1155 tokens representing infrastructure
- **Resource AMM**: Liquidity pools for resource trading
- **Settlement Coordinator**: Handles flash settlements
- **Capacity Monitor**: Tracks utilization and triggers scaling

## License

Apache 2.0 