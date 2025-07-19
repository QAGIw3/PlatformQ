# Blockchain Service - Vault & Consul Integration Guide

## Overview
This guide covers integrating blockchain services with HashiCorp Vault and Consul, focusing on secure key management, RPC endpoint configuration, and transaction signing.

## Critical Security Note
⚠️ **Private keys NEVER leave Vault** - All signing operations use Vault's Transit engine

## Vault Integration

### 1. Secret Structure

```yaml
# Vault path structure for blockchain service
blockchain-gateway/
├── transit/                      # Keys stored in Transit engine (never exportable)
│   ├── keys/
│   │   ├── ethereum-hot-wallet   # Main hot wallet
│   │   ├── ethereum-cold-wallet  # Cold storage (requires MFA)
│   │   ├── polygon-hot-wallet
│   │   ├── arbitrum-hot-wallet
│   │   ├── solana-wallet
│   │   └── cosmos-wallet
│   └── policies/
│       ├── transaction-limits    # Per-key transaction limits
│       └── signing-policies      # MFA requirements, time windows
├── api-keys/
│   ├── infura/
│   │   ├── project-id
│   │   └── project-secret
│   ├── alchemy/
│   │   └── api-key
│   ├── quicknode/
│   │   └── endpoint-token
│   └── etherscan/
│       └── api-key
├── gas-management/
│   ├── gas-wallet-keys/         # Separate wallets for gas
│   │   ├── ethereum-gas
│   │   ├── polygon-gas
│   │   └── arbitrum-gas
│   └── gas-limits/              # Maximum gas prices
│       ├── ethereum-max-gwei
│       └── polygon-max-gwei
└── oracle/
    ├── chainlink-node-key
    └── price-feed-api-key
```

### 2. Implementation Code

```python
# blockchain_service/vault_integration.py
from typing import Dict, Any, Optional, List
from decimal import Decimal
import asyncio
from datetime import datetime, timedelta
from platformq_shared.vault.vault_client import VaultClient
from web3 import Web3
from eth_account.messages import encode_defunct
import logging

logger = logging.getLogger(__name__)

class BlockchainVaultIntegration:
    """
    Vault integration for blockchain services with hardware-level security.
    Private keys NEVER leave Vault - all signing happens in Transit engine.
    """
    
    def __init__(self, vault_client: VaultClient, service_name: str = "blockchain-gateway"):
        self.vault = vault_client
        self.service_name = service_name
        self._rpc_cache: Dict[str, Any] = {}
        self._gas_limits: Dict[str, Decimal] = {}
        
    async def initialize(self):
        """Initialize Vault integration with Transit engine setup"""
        # Ensure Transit engine is mounted
        await self._ensure_transit_mount()
        
        # Create signing keys if they don't exist
        await self._ensure_signing_keys()
        
        # Load gas limits
        await self._load_gas_limits()
        
        # Set up key policies
        await self._setup_key_policies()
        
    async def _ensure_transit_mount(self):
        """Ensure Transit secrets engine is mounted"""
        try:
            mounts = await self.vault.list_auth_methods()
            if "transit/" not in mounts:
                await self.vault.enable_secrets_engine("transit", "transit")
                logger.info("Enabled Transit secrets engine")
        except Exception as e:
            logger.error(f"Failed to ensure Transit mount: {e}")
            
    async def _ensure_signing_keys(self):
        """Create blockchain signing keys in Transit engine"""
        chains = ["ethereum", "polygon", "arbitrum", "solana", "cosmos"]
        key_types = ["hot-wallet", "cold-wallet", "gas"]
        
        for chain in chains:
            for key_type in key_types:
                if key_type == "gas" and chain not in ["ethereum", "polygon", "arbitrum"]:
                    continue  # Only EVM chains need gas wallets
                    
                key_name = f"{chain}-{key_type}"
                
                try:
                    # Check if key exists
                    await self.vault.read_transit_key(key_name)
                except:
                    # Create key if it doesn't exist
                    key_config = {
                        "type": "secp256k1" if chain != "solana" else "ed25519",
                        "exportable": False,  # NEVER allow export
                        "allow_plaintext_backup": False
                    }
                    
                    await self.vault.create_transit_key(key_name, **key_config)
                    logger.info(f"Created Transit key: {key_name}")
                    
    async def _setup_key_policies(self):
        """Set up policies for key usage"""
        # Cold wallet policy - requires MFA
        cold_wallet_policy = {
            "min_decryption_version": 1,
            "min_encryption_version": 1,
            "deletion_allowed": False,
            "require_mfa": True,
            "allowed_uses": ["sign"],
            "sign_rate_limit": "10/hour"  # Max 10 signatures per hour
        }
        
        # Hot wallet policy - rate limited
        hot_wallet_policy = {
            "min_decryption_version": 1,
            "min_encryption_version": 1,
            "deletion_allowed": False,
            "require_mfa": False,
            "allowed_uses": ["sign"],
            "sign_rate_limit": "1000/hour"  # Max 1000 signatures per hour
        }
        
        # Apply policies
        for chain in ["ethereum", "polygon", "arbitrum"]:
            await self.vault.update_transit_key_config(
                f"{chain}-cold-wallet",
                cold_wallet_policy
            )
            await self.vault.update_transit_key_config(
                f"{chain}-hot-wallet",
                hot_wallet_policy
            )
            
    async def get_wallet_address(self, chain: str, wallet_type: str = "hot-wallet") -> str:
        """Get wallet address for a chain (derives from public key)"""
        key_name = f"{chain}-{wallet_type}"
        
        # Get public key from Transit
        key_info = await self.vault.read_transit_key(key_name)
        public_key = key_info["keys"]["1"]["public_key"]
        
        if chain in ["ethereum", "polygon", "arbitrum"]:
            # Derive Ethereum address from public key
            from eth_keys import keys
            pub_key = keys.PublicKey(bytes.fromhex(public_key))
            return pub_key.to_checksum_address()
        elif chain == "solana":
            # Solana address is the public key
            return public_key
        else:
            return public_key
            
    async def sign_transaction(self, 
                             chain: str,
                             transaction: Dict[str, Any],
                             wallet_type: str = "hot-wallet",
                             mfa_token: Optional[str] = None) -> Dict[str, Any]:
        """
        Sign a transaction using Vault Transit engine.
        The private key NEVER leaves Vault.
        """
        key_name = f"{chain}-{wallet_type}"
        
        # Check transaction limits
        await self._check_transaction_limits(chain, transaction)
        
        # Serialize transaction for signing
        if chain in ["ethereum", "polygon", "arbitrum"]:
            # EVM transaction
            tx_hash = self._serialize_evm_transaction(transaction)
        elif chain == "solana":
            tx_hash = self._serialize_solana_transaction(transaction)
        else:
            tx_hash = self._serialize_generic_transaction(transaction)
            
        # Sign with Transit engine
        sign_params = {
            "key_name": key_name,
            "hash_algorithm": "sha2-256",
            "signature_algorithm": "ecdsa-p256k1" if chain != "solana" else "ed25519",
            "input": tx_hash.hex()
        }
        
        if mfa_token and wallet_type == "cold-wallet":
            sign_params["mfa_token"] = mfa_token
            
        signature = await self.vault.sign_data(**sign_params)
        
        # Apply signature to transaction
        if chain in ["ethereum", "polygon", "arbitrum"]:
            return self._apply_evm_signature(transaction, signature)
        elif chain == "solana":
            return self._apply_solana_signature(transaction, signature)
        else:
            return {"tx": transaction, "signature": signature}
            
    def _serialize_evm_transaction(self, tx: Dict[str, Any]) -> bytes:
        """Serialize EVM transaction for signing"""
        from eth_account._utils.transactions import serializable_unsigned_transaction_from_dict
        from eth_utils import keccak
        
        # Create serializable transaction
        serializable_tx = serializable_unsigned_transaction_from_dict(tx)
        
        # Return keccak hash
        return keccak(serializable_tx.rawTransaction)
        
    async def _check_transaction_limits(self, chain: str, tx: Dict[str, Any]):
        """Check transaction against limits"""
        # Get limits from Vault
        limits_path = f"{self.service_name}/transit/policies/transaction-limits"
        limits = await self.vault.get_secret(limits_path)
        
        chain_limits = limits.get(chain, {})
        
        # Check value limit
        if "value" in tx:
            value_wei = int(tx["value"])
            max_wei = int(chain_limits.get("max_value_wei", 10**19))  # 10 ETH default
            
            if value_wei > max_wei:
                raise ValueError(f"Transaction value exceeds limit: {value_wei} > {max_wei}")
                
        # Check gas price
        if "gasPrice" in tx:
            gas_price = int(tx["gasPrice"])
            max_gas = int(self._gas_limits.get(f"{chain}-max-gwei", 1000)) * 10**9
            
            if gas_price > max_gas:
                raise ValueError(f"Gas price exceeds limit: {gas_price} > {max_gas}")
                
    async def get_rpc_endpoints(self, chain: str) -> List[Dict[str, Any]]:
        """Get RPC endpoints with credentials from Vault"""
        endpoints = []
        
        # Get provider credentials
        providers = ["infura", "alchemy", "quicknode"]
        
        for provider in providers:
            try:
                creds = await self.vault.get_secret(
                    f"{self.service_name}/api-keys/{provider}"
                )
                
                if provider == "infura":
                    endpoints.append({
                        "url": f"https://{chain}.infura.io/v3/{creds['project-id']}",
                        "priority": 1,
                        "provider": "infura"
                    })
                elif provider == "alchemy":
                    endpoints.append({
                        "url": f"https://{chain}.g.alchemy.com/v2/{creds['api-key']}",
                        "priority": 2,
                        "provider": "alchemy"
                    })
                    
            except Exception as e:
                logger.warning(f"Could not get {provider} credentials: {e}")
                
        return sorted(endpoints, key=lambda x: x["priority"])
        
    async def rotate_gas_wallets(self):
        """Rotate gas wallet keys (create new ones, transfer funds)"""
        chains = ["ethereum", "polygon", "arbitrum"]
        
        for chain in chains:
            old_key = f"{chain}-gas"
            new_key = f"{chain}-gas-new"
            
            # Create new gas wallet
            await self.vault.create_transit_key(new_key, type="secp256k1")
            
            # Get addresses
            old_address = await self.get_wallet_address(chain, "gas")
            new_address = await self.get_wallet_address(chain, "gas-new")
            
            # TODO: Trigger fund transfer from old to new
            logger.info(f"Created new gas wallet for {chain}: {new_address}")
            
            # After funds are transferred, rotate the key names
            # This would be done after confirming transfer
            
    async def sign_message(self, 
                          chain: str,
                          message: str,
                          wallet_type: str = "hot-wallet") -> str:
        """Sign a message for authentication/verification"""
        key_name = f"{chain}-{wallet_type}"
        
        # Prepare message
        if chain in ["ethereum", "polygon", "arbitrum"]:
            # EIP-191 personal message
            message_hash = encode_defunct(text=message)
            input_data = Web3.keccak(message_hash.body).hex()
        else:
            # Generic message signing
            input_data = Web3.keccak(text=message).hex()
            
        # Sign with Transit
        signature = await self.vault.sign_data(
            key_name=key_name,
            input=input_data,
            hash_algorithm="sha2-256",
            signature_algorithm="ecdsa-p256k1"
        )
        
        return signature["signature"]

# Additional helper for secure transaction broadcasting
class SecureTransactionBroadcaster:
    """Broadcasts signed transactions with security checks"""
    
    def __init__(self, vault_integration: BlockchainVaultIntegration):
        self.vault = vault_integration
        self._broadcast_history: Dict[str, datetime] = {}
        
    async def broadcast_transaction(self, 
                                  chain: str,
                                  signed_tx: Dict[str, Any],
                                  check_nonce: bool = True) -> str:
        """Broadcast transaction with security checks"""
        # Prevent double-spending
        tx_hash = signed_tx.get("hash")
        if tx_hash in self._broadcast_history:
            last_broadcast = self._broadcast_history[tx_hash]
            if datetime.utcnow() - last_broadcast < timedelta(minutes=5):
                raise ValueError("Transaction recently broadcast")
                
        # Get RPC endpoints
        endpoints = await self.vault.get_rpc_endpoints(chain)
        
        # Try broadcasting to multiple endpoints
        errors = []
        for endpoint in endpoints:
            try:
                result = await self._broadcast_to_endpoint(
                    endpoint["url"],
                    signed_tx,
                    chain
                )
                
                # Record successful broadcast
                self._broadcast_history[tx_hash] = datetime.utcnow()
                
                return result
                
            except Exception as e:
                errors.append(f"{endpoint['provider']}: {str(e)}")
                continue
                
        raise Exception(f"Failed to broadcast to any endpoint: {errors}")
```

## Consul Integration

### 1. Configuration Structure

```yaml
# Consul KV structure for blockchain service
services/blockchain-gateway/
├── config/
│   ├── chains/
│   │   ├── ethereum/
│   │   │   ├── enabled              # true
│   │   │   ├── network             # mainnet/goerli/sepolia
│   │   │   ├── chain-id            # 1
│   │   │   ├── confirmations       # 12
│   │   │   └── gas-strategy        # fast/medium/slow
│   │   ├── polygon/
│   │   │   ├── enabled             # true
│   │   │   ├── network            # mainnet/mumbai
│   │   │   ├── chain-id           # 137
│   │   │   └── confirmations      # 128
│   │   └── arbitrum/
│   │       ├── enabled            # true
│   │       └── chain-id           # 42161
│   ├── transaction-policies/
│   │   ├── max-value-eth          # 10
│   │   ├── max-gas-price-gwei     # 500
│   │   ├── require-multisig       # 1
│   │   └── daily-limit-usd        # 100000
│   └── monitoring/
│       ├── alert-on-large-tx      # true
│       ├── alert-threshold-eth    # 1
│       └── slack-webhook          # https://...
├── rpc-endpoints/
│   ├── ethereum/
│   │   ├── primary                # infura
│   │   ├── secondary              # alchemy
│   │   └── fallback               # quicknode
│   └── polygon/
│       ├── primary                # alchemy
│       └── secondary              # infura
└── circuit-breakers/
    ├── ethereum-signing           # closed/open/half-open
    ├── polygon-signing            # closed/open/half-open
    └── broadcast-service          # closed/open/half-open
```

### 2. Implementation Code

```python
# blockchain_service/consul_integration.py
from typing import Dict, Any, Optional, List
import asyncio
from decimal import Decimal
from platformq_shared.consul.consul_client import ConsulClient
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject all
    HALF_OPEN = "half-open"  # Testing recovery

@dataclass
class ChainConfig:
    """Blockchain chain configuration"""
    enabled: bool
    network: str  # mainnet, testnet, etc
    chain_id: int
    confirmations: int
    gas_strategy: str
    max_gas_price_gwei: int

@dataclass
class TransactionPolicy:
    """Transaction security policies"""
    max_value_eth: Decimal
    max_gas_price_gwei: int
    require_multisig_above: Decimal
    daily_limit_usd: Decimal
    alert_threshold_eth: Decimal

class BlockchainConsulIntegration:
    """Consul integration for blockchain service"""
    
    def __init__(self, consul_client: ConsulClient, service_name: str = "blockchain-gateway"):
        self.consul = consul_client
        self.service_name = service_name
        self._chain_configs: Dict[str, ChainConfig] = {}
        self._tx_policy: Optional[TransactionPolicy] = None
        self._circuit_breakers: Dict[str, CircuitState] = {}
        self._watchers: Dict[str, asyncio.Task] = {}
        
    async def initialize(self):
        """Initialize Consul integration"""
        # Register service
        await self._register_service()
        
        # Load configurations
        await self.reload_chain_configs()
        await self.reload_transaction_policies()
        
        # Initialize circuit breakers
        await self._init_circuit_breakers()
        
        # Start configuration watchers
        await self._start_config_watchers()
        
    async def _register_service(self):
        """Register blockchain service with Consul"""
        from platformq_shared.consul.consul_client import ServiceDefinition
        
        service = ServiceDefinition(
            name=self.service_name,
            port=8000,
            tags=["blockchain", "critical", "financial"],
            meta={
                "version": "1.0.0",
                "capabilities": "ethereum,polygon,arbitrum,solana"
            },
            check={
                "http": "http://localhost:8000/health",
                "interval": "5s",  # More frequent for critical service
                "timeout": "3s",
                "deregister_critical_service_after": "15s"
            }
        )
        
        await self.consul.register_service(service)
        
    async def reload_chain_configs(self):
        """Reload chain configurations from Consul"""
        base_path = f"services/{self.service_name}/config/chains"
        
        chains = ["ethereum", "polygon", "arbitrum", "solana", "cosmos"]
        
        for chain in chains:
            chain_path = f"{base_path}/{chain}"
            config_data = await self.consul.kv_get_prefix(chain_path)
            
            if config_data.get("enabled", "false").lower() == "true":
                # Load gas limits
                gas_limit_path = f"services/{self.service_name}/config/transaction-policies/max-gas-price-gwei"
                max_gas = await self.consul.kv_get(gas_limit_path, default="1000")
                
                self._chain_configs[chain] = ChainConfig(
                    enabled=True,
                    network=config_data.get("network", "mainnet"),
                    chain_id=int(config_data.get("chain-id", "1")),
                    confirmations=int(config_data.get("confirmations", "12")),
                    gas_strategy=config_data.get("gas-strategy", "medium"),
                    max_gas_price_gwei=int(max_gas)
                )
                
        logger.info(f"Loaded configurations for {len(self._chain_configs)} chains")
        
    async def reload_transaction_policies(self):
        """Reload transaction policies from Consul"""
        base_path = f"services/{self.service_name}/config/transaction-policies"
        
        policies = await self.consul.kv_get_prefix(base_path)
        
        self._tx_policy = TransactionPolicy(
            max_value_eth=Decimal(policies.get("max-value-eth", "10")),
            max_gas_price_gwei=int(policies.get("max-gas-price-gwei", "500")),
            require_multisig_above=Decimal(policies.get("require-multisig", "100")),
            daily_limit_usd=Decimal(policies.get("daily-limit-usd", "100000")),
            alert_threshold_eth=Decimal(policies.get("alert-threshold-eth", "1"))
        )
        
        logger.info("Reloaded transaction policies")
        
    async def get_chain_config(self, chain: str) -> Optional[ChainConfig]:
        """Get configuration for a specific chain"""
        return self._chain_configs.get(chain)
        
    async def get_transaction_policy(self) -> TransactionPolicy:
        """Get current transaction policy"""
        if not self._tx_policy:
            await self.reload_transaction_policies()
        return self._tx_policy
        
    async def select_rpc_endpoint(self, chain: str) -> Optional[str]:
        """Select best RPC endpoint based on health and priority"""
        base_path = f"services/{self.service_name}/rpc-endpoints/{chain}"
        
        # Get endpoint priorities
        endpoints = await self.consul.kv_get_prefix(base_path)
        
        # Check health of each endpoint
        for priority in ["primary", "secondary", "fallback"]:
            provider = endpoints.get(priority)
            if provider:
                health_key = f"rpc-health/{chain}/{provider}"
                health = await self.consul.kv_get(health_key, default="unknown")
                
                if health == "healthy":
                    logger.info(f"Selected {provider} endpoint for {chain}")
                    return provider
                    
        logger.error(f"No healthy RPC endpoints found for {chain}")
        return None
        
    async def check_circuit_breaker(self, operation: str) -> bool:
        """Check if circuit breaker allows operation"""
        breaker_key = f"services/{self.service_name}/circuit-breakers/{operation}"
        state = await self.consul.kv_get(breaker_key, default="closed")
        
        circuit_state = CircuitState(state)
        self._circuit_breakers[operation] = circuit_state
        
        if circuit_state == CircuitState.OPEN:
            logger.warning(f"Circuit breaker OPEN for {operation}")
            return False
        elif circuit_state == CircuitState.HALF_OPEN:
            # Allow limited requests for testing
            return await self._test_circuit_breaker(operation)
        else:
            return True
            
    async def trip_circuit_breaker(self, operation: str):
        """Trip circuit breaker after failures"""
        breaker_key = f"services/{self.service_name}/circuit-breakers/{operation}"
        
        await self.consul.kv_put(breaker_key, "open")
        self._circuit_breakers[operation] = CircuitState.OPEN
        
        # Schedule reset to half-open after cooldown
        asyncio.create_task(self._reset_circuit_breaker(operation, delay=60))
        
        logger.error(f"Tripped circuit breaker for {operation}")
        
    async def _reset_circuit_breaker(self, operation: str, delay: int):
        """Reset circuit breaker to half-open after delay"""
        await asyncio.sleep(delay)
        
        breaker_key = f"services/{self.service_name}/circuit-breakers/{operation}"
        await self.consul.kv_put(breaker_key, "half-open")
        self._circuit_breakers[operation] = CircuitState.HALF_OPEN
        
        logger.info(f"Reset circuit breaker to HALF-OPEN for {operation}")
        
    async def record_transaction_metrics(self, chain: str, tx_hash: str, metrics: Dict[str, Any]):
        """Record transaction metrics in Consul"""
        metrics_key = f"services/{self.service_name}/metrics/transactions/{chain}/{tx_hash}"
        
        await self.consul.kv_put(metrics_key, {
            "timestamp": datetime.utcnow().isoformat(),
            "gas_used": metrics.get("gas_used"),
            "gas_price": metrics.get("gas_price"),
            "value": metrics.get("value"),
            "status": metrics.get("status"),
            "confirmations": metrics.get("confirmations", 0)
        }, ttl=86400)  # Keep for 24 hours
        
    async def get_daily_volume(self, chain: str) -> Decimal:
        """Get daily transaction volume from Consul metrics"""
        today = datetime.utcnow().strftime("%Y%m%d")
        volume_key = f"services/{self.service_name}/volumes/{chain}/{today}"
        
        volume = await self.consul.kv_get(volume_key, default="0")
        return Decimal(volume)
        
    async def update_daily_volume(self, chain: str, amount: Decimal):
        """Update daily transaction volume"""
        today = datetime.utcnow().strftime("%Y%m%d")
        volume_key = f"services/{self.service_name}/volumes/{chain}/{today}"
        
        # Atomic increment
        current = await self.get_daily_volume(chain)
        new_volume = current + amount
        
        await self.consul.kv_put(volume_key, str(new_volume), ttl=172800)  # 48 hours
        
        # Check daily limit
        policy = await self.get_transaction_policy()
        if new_volume > policy.daily_limit_usd:
            await self._trigger_volume_alert(chain, new_volume)
            
    async def coordinate_multisig_transaction(self, 
                                            tx_id: str,
                                            required_signatures: int) -> bool:
        """Coordinate multi-signature transaction approval"""
        coord_key = f"services/{self.service_name}/multisig/{tx_id}"
        
        # Create coordination entry
        await self.consul.kv_put(coord_key, {
            "created_at": datetime.utcnow().isoformat(),
            "required_signatures": required_signatures,
            "signatures": [],
            "status": "pending"
        }, ttl=3600)  # 1 hour to collect signatures
        
        # Watch for signature updates
        async for event in self.consul.watch_key(coord_key):
            data = event.get("Value", {})
            if len(data.get("signatures", [])) >= required_signatures:
                data["status"] = "ready"
                await self.consul.kv_put(coord_key, data)
                return True
                
        return False

# Usage in blockchain service
class SecureBlockchainService:
    def __init__(self):
        self.vault = BlockchainVaultIntegration(vault_client)
        self.consul = BlockchainConsulIntegration(consul_client)
        
    async def send_transaction(self, chain: str, to: str, value: str) -> str:
        # Check circuit breaker
        if not await self.consul.check_circuit_breaker(f"{chain}-signing"):
            raise Exception("Service temporarily unavailable")
            
        # Get chain config
        config = await self.consul.get_chain_config(chain)
        if not config or not config.enabled:
            raise Exception(f"Chain {chain} not enabled")
            
        # Check transaction policy
        policy = await self.consul.get_transaction_policy()
        value_eth = Decimal(Web3.fromWei(int(value), 'ether'))
        
        if value_eth > policy.max_value_eth:
            raise Exception(f"Value exceeds limit: {value_eth} ETH")
            
        # Check if multisig required
        if value_eth > policy.require_multisig_above:
            tx_id = f"{chain}-{datetime.utcnow().timestamp()}"
            approved = await self.consul.coordinate_multisig_transaction(tx_id, 2)
            if not approved:
                raise Exception("Multisig approval required")
                
        # Build transaction
        tx = {
            "to": to,
            "value": value,
            "gas": 21000,
            "gasPrice": await self._get_gas_price(chain, config.gas_strategy),
            "nonce": await self._get_nonce(chain),
            "chainId": config.chain_id
        }
        
        # Sign with Vault
        try:
            signed_tx = await self.vault.sign_transaction(chain, tx)
        except Exception as e:
            await self.consul.trip_circuit_breaker(f"{chain}-signing")
            raise
            
        # Broadcast
        tx_hash = await self._broadcast_transaction(chain, signed_tx)
        
        # Update metrics
        await self.consul.record_transaction_metrics(chain, tx_hash, {
            "gas_price": tx["gasPrice"],
            "value": value,
            "status": "pending"
        })
        
        # Update daily volume
        await self.consul.update_daily_volume(chain, value_eth)
        
        return tx_hash
```

## Security Best Practices

### 1. Key Management

```python
# NEVER do this:
# private_key = "0x..." # NEVER store private keys in code or environment variables

# ALWAYS do this:
signed_tx = await vault_client.sign_with_transit_key("ethereum-hot-wallet", transaction)
```

### 2. Transaction Validation

```python
async def validate_transaction(tx: Dict[str, Any]) -> bool:
    """Multi-layer transaction validation"""
    
    # 1. Whitelist check
    if tx["to"] not in await get_whitelisted_addresses():
        return False
        
    # 2. Value limits
    if int(tx["value"]) > MAX_TRANSACTION_VALUE:
        return False
        
    # 3. Gas sanity check
    if int(tx["gasPrice"]) > MAX_GAS_PRICE:
        return False
        
    # 4. Time-based restrictions
    if not within_trading_hours():
        return False
        
    return True
```

### 3. Multi-Signature Flow

```yaml
# Consul coordination for high-value transactions
multisig/tx-12345/
  created_at: "2024-01-01T10:00:00Z"
  transaction: {...}
  required_signatures: 3
  signatures:
    - signer: "alice@platformq.io"
      signature: "0x..."
      timestamp: "2024-01-01T10:05:00Z"
    - signer: "bob@platformq.io"
      signature: "0x..."
      timestamp: "2024-01-01T10:10:00Z"
  status: "pending" # pending -> ready -> executed
```

## Monitoring & Alerting

### 1. Critical Metrics

```python
# Prometheus metrics
blockchain_transactions_total = Counter(
    'blockchain_transactions_total',
    'Total blockchain transactions',
    ['chain', 'status']
)

blockchain_transaction_value = Histogram(
    'blockchain_transaction_value_eth',
    'Transaction values in ETH',
    ['chain'],
    buckets=[0.01, 0.1, 1, 10, 100]
)

blockchain_gas_price = Gauge(
    'blockchain_gas_price_gwei',
    'Current gas price in gwei',
    ['chain']
)

blockchain_wallet_balance = Gauge(
    'blockchain_wallet_balance_eth',
    'Wallet balance in ETH',
    ['chain', 'wallet_type']
)
```

### 2. Security Alerts

```yaml
groups:
  - name: blockchain_security
    rules:
      - alert: HighValueTransaction
        expr: blockchain_transaction_value_eth > 10
        annotations:
          summary: "High value transaction detected: {{ $value }} ETH"
          
      - alert: RapidTransactionRate
        expr: rate(blockchain_transactions_total[5m]) > 10
        annotations:
          summary: "Unusually high transaction rate"
          
      - alert: WalletBalanceLow
        expr: blockchain_wallet_balance_eth{wallet_type="gas"} < 0.1
        annotations:
          summary: "Gas wallet balance low: {{ $value }} ETH"
          
      - alert: CircuitBreakerOpen
        expr: circuit_breaker_state{state="open"} == 1
        for: 5m
        annotations:
          summary: "Circuit breaker open for {{ $labels.operation }}"
```

## Disaster Recovery

### 1. Key Backup (Vault Managed)

```bash
# Vault automatically handles key backup
# Enable disaster recovery replication
vault write -f /sys/replication/dr/primary/enable

# Create backup of Transit keys (encrypted)
vault operator raft snapshot save blockchain-keys-backup.snap
```

### 2. Emergency Procedures

```python
class EmergencyProcedures:
    """Emergency procedures for blockchain service"""
    
    async def pause_all_transactions(self):
        """Emergency pause - trips all circuit breakers"""
        chains = ["ethereum", "polygon", "arbitrum"]
        
        for chain in chains:
            await self.consul.trip_circuit_breaker(f"{chain}-signing")
            await self.consul.trip_circuit_breaker(f"{chain}-broadcast")
            
        # Notify operations team
        await self.send_emergency_alert("All blockchain transactions paused")
        
    async def rotate_compromised_key(self, chain: str, wallet_type: str):
        """Emergency key rotation for compromised wallet"""
        
        # 1. Immediately disable old key in Vault
        old_key = f"{chain}-{wallet_type}"
        await self.vault.update_transit_key_config(
            old_key,
            {"min_encryption_version": 999999}  # Effectively disable
        )
        
        # 2. Create new key
        new_key = f"{chain}-{wallet_type}-emergency"
        await self.vault.create_transit_key(new_key)
        
        # 3. Update configuration to use new key
        await self.consul.kv_put(
            f"services/blockchain-gateway/emergency-rotation/{chain}",
            {"old_key": old_key, "new_key": new_key, "rotated_at": datetime.utcnow().isoformat()}
        )
        
        # 4. Transfer any remaining funds
        # This would trigger a separate secure process
``` 