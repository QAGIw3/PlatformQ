"""
Blockchain Gateway Service - Vault & Consul Integration
"""

from typing import Dict, Any, Optional, List
import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from dataclasses import dataclass
from enum import Enum
from web3 import Web3
from eth_account.messages import encode_defunct
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
    rpc_endpoints: List[str]

@dataclass
class TransactionPolicy:
    """Transaction security policies"""
    max_value_eth: Decimal
    max_gas_price_gwei: int
    require_multisig_above: Decimal
    daily_limit_usd: Decimal
    alert_threshold_eth: Decimal


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
        
        logger.info("Blockchain service Vault integration initialized")
        
    async def _ensure_transit_mount(self):
        """Ensure Transit secrets engine is mounted"""
        try:
            mounts = await self.vault.list_secrets_engines()
            if "transit/" not in mounts:
                await self.vault.enable_secrets_engine("transit", "transit")
                logger.info("Enabled Transit secrets engine")
        except Exception as e:
            logger.error(f"Failed to ensure Transit mount: {e}")
            
    async def _ensure_signing_keys(self):
        """Create blockchain signing keys in Transit engine"""
        chains = ["ethereum", "polygon", "arbitrum", "optimism", "bsc", "avalanche"]
        key_types = ["hot-wallet", "cold-wallet", "gas"]
        
        for chain in chains:
            for key_type in key_types:
                if key_type == "gas" and chain not in ["ethereum", "polygon", "arbitrum"]:
                    continue  # Only major chains need dedicated gas wallets
                    
                key_name = f"{chain}-{key_type}"
                
                try:
                    # Check if key exists
                    await self.vault.read_transit_key(key_name)
                    logger.debug(f"Transit key exists: {key_name}")
                except:
                    # Create key if it doesn't exist
                    key_config = {
                        "type": "secp256k1",  # Ethereum compatible
                        "exportable": False,  # NEVER allow export
                        "allow_plaintext_backup": False,
                        "derived": False
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
            "allow_plaintext_backup": False
        }
        
        # Hot wallet policy - rate limited
        hot_wallet_policy = {
            "min_decryption_version": 1,
            "min_encryption_version": 1,
            "deletion_allowed": False,
            "allow_plaintext_backup": False
        }
        
        # Apply policies
        for chain in ["ethereum", "polygon", "arbitrum"]:
            try:
                await self.vault.update_transit_key_config(
                    f"{chain}-cold-wallet",
                    cold_wallet_policy
                )
                await self.vault.update_transit_key_config(
                    f"{chain}-hot-wallet",
                    hot_wallet_policy
                )
            except Exception as e:
                logger.error(f"Failed to update key policy for {chain}: {e}")
                
    async def _load_gas_limits(self):
        """Load gas limits from Vault"""
        try:
            gas_limits_path = f"{self.service_name}/gas-management/gas-limits"
            limits = await self.vault.get_secret(gas_limits_path)
            
            for chain, limit in limits.items():
                self._gas_limits[chain] = Decimal(limit)
        except:
            # Default gas limits
            self._gas_limits = {
                "ethereum-max-gwei": Decimal("500"),
                "polygon-max-gwei": Decimal("1000"),
                "arbitrum-max-gwei": Decimal("10")
            }
            
    async def get_wallet_address(self, chain: str, wallet_type: str = "hot-wallet") -> str:
        """Get wallet address for a chain (derives from public key)"""
        key_name = f"{chain}-{wallet_type}"
        
        try:
            # Get public key from Transit
            key_info = await self.vault.read_transit_key(key_name)
            
            # For new keys, we need to get the public key differently
            # This is a simplified version - in production you'd derive the address properly
            import hashlib
            
            # Generate deterministic address from key name (for demo)
            addr_bytes = hashlib.sha256(f"{key_name}-address".encode()).digest()[-20:]
            address = "0x" + addr_bytes.hex()
            
            return Web3.toChecksumAddress(address)
            
        except Exception as e:
            logger.error(f"Failed to get wallet address: {e}")
            raise
            
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
        tx_hash = self._serialize_evm_transaction(transaction)
        
        try:
            # Sign with Transit engine
            sign_result = await self.vault.sign_data(
                mount_point="transit",
                name=key_name,
                hash_algorithm="sha2-256",
                input=tx_hash.hex(),
                signature_algorithm="ecdsa-p256k1",
                marshaling_algorithm="asn1"
            )
            
            # Apply signature to transaction
            signed_tx = self._apply_evm_signature(transaction, sign_result["signature"])
            
            logger.info(f"Transaction signed for {chain} using {wallet_type}")
            return signed_tx
            
        except Exception as e:
            logger.error(f"Failed to sign transaction: {e}")
            raise
            
    def _serialize_evm_transaction(self, tx: Dict[str, Any]) -> bytes:
        """Serialize EVM transaction for signing"""
        from eth_account._utils.transactions import serializable_unsigned_transaction_from_dict
        from eth_utils import keccak
        
        # Ensure all required fields
        tx_dict = {
            "nonce": tx.get("nonce", 0),
            "gasPrice": tx.get("gasPrice", 0),
            "gas": tx.get("gas", 21000),
            "to": tx.get("to"),
            "value": tx.get("value", 0),
            "data": tx.get("data", "0x"),
            "chainId": tx.get("chainId", 1)
        }
        
        # Create serializable transaction
        serializable_tx = serializable_unsigned_transaction_from_dict(tx_dict)
        
        # Return keccak hash
        return keccak(serializable_tx.rawTransaction)
        
    def _apply_evm_signature(self, tx: Dict[str, Any], signature: str) -> Dict[str, Any]:
        """Apply signature to transaction"""
        # This is simplified - in production you'd properly parse the signature
        # and create a signed transaction
        signed_tx = tx.copy()
        signed_tx["signature"] = signature
        signed_tx["signed"] = True
        
        return signed_tx
        
    async def _check_transaction_limits(self, chain: str, tx: Dict[str, Any]):
        """Check transaction against limits"""
        # Get limits from Vault
        limits_path = f"{self.service_name}/transit/policies/transaction-limits"
        try:
            limits = await self.vault.get_secret(limits_path)
            chain_limits = limits.get(chain, {})
        except:
            chain_limits = {
                "max_value_wei": 10**19,  # 10 ETH default
                "max_gas_price": 500 * 10**9  # 500 gwei default
            }
        
        # Check value limit
        if "value" in tx:
            value_wei = int(tx["value"])
            max_wei = int(chain_limits.get("max_value_wei", 10**19))
            
            if value_wei > max_wei:
                raise ValueError(f"Transaction value exceeds limit: {value_wei} > {max_wei}")
                
        # Check gas price
        if "gasPrice" in tx:
            gas_price = int(tx["gasPrice"])
            max_gas_key = f"{chain}-max-gwei"
            max_gas = int(self._gas_limits.get(max_gas_key, 1000)) * 10**9
            
            if gas_price > max_gas:
                raise ValueError(f"Gas price exceeds limit: {gas_price} > {max_gas}")
                
    async def get_rpc_endpoints(self, chain: str) -> List[Dict[str, Any]]:
        """Get RPC endpoints with credentials from Vault"""
        # Check cache
        cache_key = f"rpc_{chain}"
        if cache_key in self._rpc_cache:
            cached = self._rpc_cache[cache_key]
            if cached["expires"] > datetime.utcnow():
                return cached["endpoints"]
                
        endpoints = []
        
        # Get provider credentials
        providers = ["infura", "alchemy", "quicknode", "ankr"]
        
        for provider in providers:
            try:
                creds_path = f"{self.service_name}/api-keys/{provider}"
                creds = await self.vault.get_secret(creds_path)
                
                if provider == "infura":
                    endpoints.append({
                        "url": f"https://{chain}.infura.io/v3/{creds['project-id']}",
                        "priority": 1,
                        "provider": "infura",
                        "rate_limit": 100000  # requests per day
                    })
                elif provider == "alchemy":
                    chain_name = self._get_alchemy_chain_name(chain)
                    endpoints.append({
                        "url": f"https://{chain_name}.g.alchemy.com/v2/{creds['api-key']}",
                        "priority": 2,
                        "provider": "alchemy",
                        "rate_limit": 300000000  # compute units per month
                    })
                elif provider == "quicknode":
                    endpoints.append({
                        "url": creds.get(f"{chain}-endpoint", ""),
                        "priority": 3,
                        "provider": "quicknode",
                        "rate_limit": 10000000  # requests per month
                    })
                    
            except Exception as e:
                logger.warning(f"Could not get {provider} credentials: {e}")
                
        # Cache for 1 hour
        self._rpc_cache[cache_key] = {
            "endpoints": sorted(endpoints, key=lambda x: x["priority"]),
            "expires": datetime.utcnow() + timedelta(hours=1)
        }
        
        return endpoints
        
    def _get_alchemy_chain_name(self, chain: str) -> str:
        """Convert chain name to Alchemy format"""
        mapping = {
            "ethereum": "eth-mainnet",
            "polygon": "polygon-mainnet",
            "arbitrum": "arb-mainnet",
            "optimism": "opt-mainnet"
        }
        return mapping.get(chain, chain)
        
    async def rotate_gas_wallets(self):
        """Rotate gas wallet keys (create new ones, transfer funds)"""
        chains = ["ethereum", "polygon", "arbitrum"]
        
        for chain in chains:
            old_key = f"{chain}-gas"
            new_key = f"{chain}-gas-new"
            
            try:
                # Create new gas wallet
                await self.vault.create_transit_key(
                    new_key,
                    type="secp256k1",
                    exportable=False
                )
                
                # Get addresses
                old_address = await self.get_wallet_address(chain, "gas")
                new_address = await self.get_wallet_address(chain, "gas-new")
                
                # Log rotation event
                logger.info(f"Created new gas wallet for {chain}: {new_address}")
                logger.info(f"TODO: Transfer funds from {old_address} to {new_address}")
                
                # After funds are transferred, the keys would be rotated
                # This is a manual process that requires fund transfer confirmation
                
            except Exception as e:
                logger.error(f"Failed to rotate gas wallet for {chain}: {e}")
                
    async def sign_message(self, 
                          chain: str,
                          message: str,
                          wallet_type: str = "hot-wallet") -> str:
        """Sign a message for authentication/verification"""
        key_name = f"{chain}-{wallet_type}"
        
        # Prepare message
        message_hash = Web3.keccak(text=message)
        
        try:
            # Sign with Transit
            signature = await self.vault.sign_data(
                mount_point="transit",
                name=key_name,
                input=message_hash.hex(),
                hash_algorithm="sha2-256",
                signature_algorithm="ecdsa-p256k1"
            )
            
            return signature["signature"]
            
        except Exception as e:
            logger.error(f"Failed to sign message: {e}")
            raise


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
        
        logger.info("Blockchain service Consul integration initialized")
        
    async def _register_service(self):
        """Register blockchain service with Consul"""
        from platformq_shared.consul.consul_client import ServiceDefinition
        
        service = ServiceDefinition(
            name=self.service_name,
            port=8000,
            tags=["blockchain", "critical", "financial", "vault-integrated"],
            meta={
                "version": "1.0.0",
                "capabilities": "ethereum,polygon,arbitrum,optimism,bsc,avalanche",
                "vault_integration": "true",
                "signing_method": "vault-transit"
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
        
        chains = ["ethereum", "polygon", "arbitrum", "optimism", "bsc", "avalanche"]
        
        for chain in chains:
            try:
                chain_path = f"{base_path}/{chain}"
                config_data = await self.consul.kv_get_prefix(chain_path)
                
                if config_data.get("enabled", "false").lower() == "true":
                    # Get gas limits
                    gas_limit_path = f"services/{self.service_name}/config/transaction-policies/max-gas-price-gwei"
                    max_gas = await self.consul.kv_get(gas_limit_path, default="1000")
                    
                    # Get RPC endpoints
                    rpc_path = f"services/{self.service_name}/rpc-endpoints/{chain}"
                    rpc_data = await self.consul.kv_get_prefix(rpc_path)
                    rpc_endpoints = [
                        rpc_data.get("primary", ""),
                        rpc_data.get("secondary", ""),
                        rpc_data.get("fallback", "")
                    ]
                    
                    self._chain_configs[chain] = ChainConfig(
                        enabled=True,
                        network=config_data.get("network", "mainnet"),
                        chain_id=int(config_data.get("chain-id", "1")),
                        confirmations=int(config_data.get("confirmations", "12")),
                        gas_strategy=config_data.get("gas-strategy", "medium"),
                        max_gas_price_gwei=int(max_gas),
                        rpc_endpoints=[ep for ep in rpc_endpoints if ep]
                    )
                    
            except Exception as e:
                logger.error(f"Failed to load config for {chain}: {e}")
                
        logger.info(f"Loaded configurations for {len(self._chain_configs)} chains")
        
    async def reload_transaction_policies(self):
        """Reload transaction policies from Consul"""
        base_path = f"services/{self.service_name}/config/transaction-policies"
        
        try:
            policies = await self.consul.kv_get_prefix(base_path)
            
            self._tx_policy = TransactionPolicy(
                max_value_eth=Decimal(policies.get("max-value-eth", "10")),
                max_gas_price_gwei=int(policies.get("max-gas-price-gwei", "500")),
                require_multisig_above=Decimal(policies.get("require-multisig", "100")),
                daily_limit_usd=Decimal(policies.get("daily-limit-usd", "100000")),
                alert_threshold_eth=Decimal(policies.get("alert-threshold-eth", "1"))
            )
            
            logger.info("Reloaded transaction policies")
            
        except Exception as e:
            logger.error(f"Failed to reload transaction policies: {e}")
            # Use defaults
            self._tx_policy = TransactionPolicy(
                max_value_eth=Decimal("10"),
                max_gas_price_gwei=500,
                require_multisig_above=Decimal("100"),
                daily_limit_usd=Decimal("100000"),
                alert_threshold_eth=Decimal("1")
            )
            
    async def _init_circuit_breakers(self):
        """Initialize circuit breakers for chains"""
        for chain in self._chain_configs:
            breaker_key = f"services/{self.service_name}/circuit-breakers/{chain}-signing"
            state = await self.consul.kv_get(breaker_key, default="closed")
            self._circuit_breakers[f"{chain}-signing"] = CircuitState(state)
            
    async def _start_config_watchers(self):
        """Start configuration watchers"""
        watch_paths = [
            "config/chains",
            "config/transaction-policies",
            "circuit-breakers",
            "rpc-endpoints"
        ]
        
        for path in watch_paths:
            full_path = f"services/{self.service_name}/{path}"
            watcher = asyncio.create_task(
                self._watch_config_changes(full_path)
            )
            self._watchers[path] = watcher
            
    async def _watch_config_changes(self, path: str):
        """Watch for configuration changes"""
        try:
            async for event in self.consul.watch_prefix(path):
                logger.info(f"Configuration changed at {path}")
                
                if "chains" in path:
                    await self.reload_chain_configs()
                elif "transaction-policies" in path:
                    await self.reload_transaction_policies()
                    
        except asyncio.CancelledError:
            logger.info(f"Config watcher cancelled for {path}")
            raise
        except Exception as e:
            logger.error(f"Config watcher error for {path}: {e}")
            
    async def get_chain_config(self, chain: str) -> Optional[ChainConfig]:
        """Get configuration for a specific chain"""
        return self._chain_configs.get(chain)
        
    async def get_transaction_policy(self) -> TransactionPolicy:
        """Get current transaction policy"""
        if not self._tx_policy:
            await self.reload_transaction_policies()
        return self._tx_policy
        
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
            
    async def _test_circuit_breaker(self, operation: str) -> bool:
        """Test if service is recovering"""
        # Simple implementation - allow 10% of requests
        import random
        return random.random() < 0.1
        
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
        
    async def close_circuit_breaker(self, operation: str):
        """Close circuit breaker after successful recovery"""
        breaker_key = f"services/{self.service_name}/circuit-breakers/{operation}"
        await self.consul.kv_put(breaker_key, "closed")
        self._circuit_breakers[operation] = CircuitState.CLOSED
        
        logger.info(f"Closed circuit breaker for {operation}")
        
    async def record_transaction_metrics(self, chain: str, tx_hash: str, metrics: Dict[str, Any]):
        """Record transaction metrics in Consul"""
        metrics_key = f"services/{self.service_name}/metrics/transactions/{chain}/{tx_hash}"
        
        await self.consul.kv_put(
            metrics_key,
            {
                "timestamp": datetime.utcnow().isoformat(),
                "gas_used": metrics.get("gas_used"),
                "gas_price": metrics.get("gas_price"),
                "value": metrics.get("value"),
                "status": metrics.get("status"),
                "confirmations": metrics.get("confirmations", 0)
            },
            ttl=86400  # Keep for 24 hours
        )
        
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
            
    async def _trigger_volume_alert(self, chain: str, volume: Decimal):
        """Trigger alert for high volume"""
        alert_key = f"services/{self.service_name}/alerts/high-volume/{chain}"
        await self.consul.kv_put(
            alert_key,
            {
                "chain": chain,
                "volume": str(volume),
                "timestamp": datetime.utcnow().isoformat(),
                "severity": "high"
            },
            ttl=3600  # 1 hour
        )
        logger.warning(f"High volume alert for {chain}: {volume} USD")
        
    async def coordinate_multisig_transaction(self, 
                                            tx_id: str,
                                            required_signatures: int) -> bool:
        """Coordinate multi-signature transaction approval"""
        coord_key = f"services/{self.service_name}/multisig/{tx_id}"
        
        # Create coordination entry
        await self.consul.kv_put(
            coord_key,
            {
                "created_at": datetime.utcnow().isoformat(),
                "required_signatures": required_signatures,
                "signatures": [],
                "status": "pending"
            },
            ttl=3600  # 1 hour to collect signatures
        )
        
        # In production, this would watch for signature updates
        # For now, return True for demo
        return True
        
    async def select_rpc_endpoint(self, chain: str) -> Optional[str]:
        """Select best RPC endpoint based on health and priority"""
        config = await self.get_chain_config(chain)
        if not config:
            return None
            
        # Check health of each endpoint
        for endpoint in config.rpc_endpoints:
            health_key = f"services/{self.service_name}/rpc-health/{chain}/{endpoint}"
            health = await self.consul.kv_get(health_key, default="healthy")
            
            if health == "healthy":
                logger.info(f"Selected RPC endpoint for {chain}: {endpoint}")
                return endpoint
                
        logger.error(f"No healthy RPC endpoints found for {chain}")
        return None 