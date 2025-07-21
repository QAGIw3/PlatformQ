"""
Vault and Consul Integration for DeFi Protocol Service

Manages secure configuration, smart contract keys, and protocol governance.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
import json
import base64
import hashlib
from decimal import Decimal

import hvac
import consul.aio
from web3 import Web3
from eth_account import Account
from eth_account.messages import encode_defunct

from platformq_shared.vault_consul_base import VaultConsulBase

logger = logging.getLogger(__name__)


class VaultConsulIntegration(VaultConsulBase):
    """
    DeFi Protocol service specific Vault and Consul integration.
    
    Features:
    - Smart contract deployment keys
    - Protocol treasury wallet management
    - Oracle signing keys
    - Liquidity provider credentials
    - Governance voting keys
    - Bridge validator keys
    - AMM pricing oracle keys
    - Flash loan protection keys
    - Protocol upgrade keys
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(
            vault_addr=config["vault_addr"],
            vault_token=config.get("vault_token"),
            consul_addr=config["consul_addr"],
            service_name="defi-protocol-service"
        )
        
        self.protocol_config = {}
        self._key_cache = {}
        self._oracle_cache = {}
        self._governance_watchers = {}
        
    async def initialize(self):
        """Initialize DeFi-specific Vault and Consul features"""
        await super().initialize()
        
        logger.info("Initializing DeFi Protocol Vault/Consul integration")
        
        # Enable DeFi-specific secret engines
        await self._setup_defi_secrets()
        
        # Load protocol configuration
        await self._load_protocol_config()
        
        # Setup key rotation for security
        await self._setup_key_rotation()
        
        # Watch for governance changes
        await self._setup_governance_watchers()
        
        logger.info("DeFi Protocol Vault/Consul integration initialized")
        
    async def _setup_defi_secrets(self):
        """Setup DeFi-specific secret engines"""
        try:
            # Enable Transit engine for signing
            try:
                self.vault.sys.enable_secrets_engine(
                    backend_type="transit",
                    path="defi-transit"
                )
            except Exception:
                pass  # Already enabled
                
            # Create DeFi KV paths
            paths = [
                "secret/defi/contracts",
                "secret/defi/treasury",
                "secret/defi/oracles",
                "secret/defi/governance",
                "secret/defi/bridges",
                "secret/defi/liquidity",
                "secret/defi/validators"
            ]
            
            for path in paths:
                try:
                    self.vault.write(f"{path}/config", initialized=True)
                except Exception:
                    pass  # Path might already exist
                    
            # Create protocol-specific keys
            await self._create_protocol_keys()
                
        except Exception as e:
            logger.error(f"Failed to setup DeFi secrets: {e}")
            
    async def _create_protocol_keys(self):
        """Create protocol-specific keys"""
        try:
            # Contract deployment key
            self.vault.write(
                "defi-transit/keys/contract-deployer",
                type="ecdsa-p256",
                exportable=False,
                allow_plaintext_backup=False
            )
            
            # Oracle signing key
            self.vault.write(
                "defi-transit/keys/oracle-signer",
                type="ecdsa-p256",
                exportable=False,
                allow_plaintext_backup=False
            )
            
            # Governance signing key
            self.vault.write(
                "defi-transit/keys/governance-signer",
                type="rsa-4096",
                exportable=False,
                allow_plaintext_backup=False
            )
            
            # Bridge validator key
            self.vault.write(
                "defi-transit/keys/bridge-validator",
                type="ecdsa-p256",
                exportable=False,
                allow_plaintext_backup=False
            )
            
        except Exception:
            pass  # Keys might already exist
            
    async def get_contract_deployment_key(self, 
                                        contract_type: str) -> Dict[str, str]:
        """Get key for smart contract deployment"""
        cache_key = f"deploy_{contract_type}"
        
        # Check cache
        if cache_key in self._key_cache:
            cached = self._key_cache[cache_key]
            if datetime.utcnow() < cached["expires"]:
                return cached["key"]
                
        try:
            # Get or create deployment wallet
            response = self.vault.read(f"secret/defi/contracts/{contract_type}/deployer")
            
            if not response or "data" not in response:
                # Generate new deployment wallet
                account = Account.create()
                
                wallet_data = {
                    "address": account.address,
                    "private_key": account.privateKey.hex(),
                    "created_at": datetime.utcnow().isoformat(),
                    "contract_type": contract_type
                }
                
                # Store securely
                self.vault.write(
                    f"secret/defi/contracts/{contract_type}/deployer",
                    **wallet_data
                )
            else:
                wallet_data = response["data"]["data"]
                
            # Cache
            self._key_cache[cache_key] = {
                "key": wallet_data,
                "expires": datetime.utcnow() + timedelta(hours=1)
            }
            
            return wallet_data
            
        except Exception as e:
            logger.error(f"Failed to get deployment key: {e}")
            raise
            
    async def get_treasury_wallet(self, 
                                chain: str = "ethereum") -> Dict[str, str]:
        """Get protocol treasury wallet"""
        try:
            response = self.vault.read(f"secret/defi/treasury/{chain}/main")
            
            if response and "data" in response:
                return response["data"]["data"]
                
            # Generate treasury wallet with multi-sig setup
            treasury_wallet = {
                "address": "0x...",  # Would be actual multi-sig address
                "signers": [],
                "threshold": 3,
                "chain": chain
            }
            
            self.vault.write(
                f"secret/defi/treasury/{chain}/main",
                **treasury_wallet
            )
            
            return treasury_wallet
            
        except Exception as e:
            logger.error(f"Failed to get treasury wallet: {e}")
            raise
            
    async def sign_oracle_data(self,
                             data: Dict[str, Any],
                             oracle_type: str = "price") -> Dict[str, str]:
        """Sign oracle data for on-chain verification"""
        try:
            # Prepare data for signing
            oracle_data = {
                "type": oracle_type,
                "data": data,
                "timestamp": int(datetime.utcnow().timestamp()),
                "nonce": await self._get_oracle_nonce(oracle_type)
            }
            
            # Create message hash
            message = json.dumps(oracle_data, sort_keys=True)
            message_hash = Web3.keccak(text=message).hex()
            
            # Sign with Transit
            response = self.vault.write(
                "defi-transit/sign/oracle-signer",
                input=base64.b64encode(message_hash.encode()).decode(),
                hash_algorithm="sha2-256",
                signature_algorithm="ecdsa"
            )
            
            if response and "data" in response:
                return {
                    "data": oracle_data,
                    "signature": response["data"]["signature"],
                    "message_hash": message_hash,
                    "signer": "oracle-signer"
                }
                
            raise Exception("Oracle signing failed")
            
        except Exception as e:
            logger.error(f"Failed to sign oracle data: {e}")
            raise
            
    async def verify_oracle_signature(self,
                                    signed_data: Dict[str, Any]) -> bool:
        """Verify oracle signature"""
        try:
            # Recreate message hash
            oracle_data = signed_data["data"]
            message = json.dumps(oracle_data, sort_keys=True)
            message_hash = Web3.keccak(text=message).hex()
            
            # Verify with Transit
            response = self.vault.write(
                "defi-transit/verify/oracle-signer",
                input=base64.b64encode(message_hash.encode()).decode(),
                signature=signed_data["signature"],
                hash_algorithm="sha2-256",
                signature_algorithm="ecdsa"
            )
            
            if response and "data" in response:
                return response["data"]["valid"]
                
            return False
            
        except Exception as e:
            logger.error(f"Failed to verify oracle signature: {e}")
            return False
            
    async def get_liquidity_provider_keys(self,
                                        pool_id: str) -> Dict[str, Any]:
        """Get keys for liquidity provider operations"""
        try:
            response = self.vault.read(f"secret/defi/liquidity/pools/{pool_id}")
            
            if not response or "data" not in response:
                # Generate LP management keys
                lp_keys = {
                    "pool_id": pool_id,
                    "manager_address": Account.create().address,
                    "fee_collector": Account.create().address,
                    "emergency_withdraw": Account.create().address,
                    "created_at": datetime.utcnow().isoformat()
                }
                
                self.vault.write(
                    f"secret/defi/liquidity/pools/{pool_id}",
                    **lp_keys
                )
                
                return lp_keys
            else:
                return response["data"]["data"]
                
        except Exception as e:
            logger.error(f"Failed to get LP keys: {e}")
            raise
            
    async def sign_governance_proposal(self,
                                     proposal: Dict[str, Any]) -> Dict[str, str]:
        """Sign governance proposal"""
        try:
            # Add proposal metadata
            proposal_data = {
                **proposal,
                "proposed_at": datetime.utcnow().isoformat(),
                "proposer": self.service_name
            }
            
            # Create proposal hash
            proposal_json = json.dumps(proposal_data, sort_keys=True)
            proposal_hash = hashlib.sha256(proposal_json.encode()).hexdigest()
            
            # Sign with governance key
            response = self.vault.write(
                "defi-transit/sign/governance-signer",
                input=base64.b64encode(proposal_hash.encode()).decode(),
                hash_algorithm="sha2-256",
                signature_algorithm="pss"
            )
            
            if response and "data" in response:
                # Store proposal in Consul
                await self.consul.kv.put(
                    f"defi/governance/proposals/{proposal_hash}",
                    proposal_json
                )
                
                return {
                    "proposal_hash": proposal_hash,
                    "signature": response["data"]["signature"],
                    "proposal_data": proposal_data
                }
                
            raise Exception("Proposal signing failed")
            
        except Exception as e:
            logger.error(f"Failed to sign proposal: {e}")
            raise
            
    async def get_bridge_validator_key(self,
                                     bridge_id: str,
                                     chain: str) -> Dict[str, str]:
        """Get bridge validator key"""
        try:
            key_path = f"secret/defi/bridges/{bridge_id}/{chain}/validator"
            response = self.vault.read(key_path)
            
            if not response or "data" not in response:
                # Generate validator key
                validator_account = Account.create()
                
                validator_data = {
                    "address": validator_account.address,
                    "private_key": validator_account.privateKey.hex(),
                    "bridge_id": bridge_id,
                    "chain": chain,
                    "created_at": datetime.utcnow().isoformat()
                }
                
                self.vault.write(key_path, **validator_data)
                
                # Register validator in Consul
                await self._register_bridge_validator(
                    bridge_id, chain, validator_account.address
                )
                
                return validator_data
            else:
                return response["data"]["data"]
                
        except Exception as e:
            logger.error(f"Failed to get bridge validator key: {e}")
            raise
            
    async def sign_cross_chain_message(self,
                                     message: Dict[str, Any],
                                     source_chain: str,
                                     target_chain: str) -> Dict[str, str]:
        """Sign cross-chain bridge message"""
        try:
            # Prepare message
            bridge_message = {
                "message": message,
                "source_chain": source_chain,
                "target_chain": target_chain,
                "timestamp": int(datetime.utcnow().timestamp()),
                "nonce": await self._get_bridge_nonce(source_chain, target_chain)
            }
            
            # Create message hash
            message_bytes = json.dumps(bridge_message, sort_keys=True).encode()
            message_hash = Web3.keccak(message_bytes).hex()
            
            # Sign with bridge validator key
            response = self.vault.write(
                "defi-transit/sign/bridge-validator",
                input=base64.b64encode(message_hash.encode()).decode(),
                hash_algorithm="sha2-256",
                signature_algorithm="ecdsa"
            )
            
            if response and "data" in response:
                return {
                    "message": bridge_message,
                    "signature": response["data"]["signature"],
                    "message_hash": message_hash
                }
                
            raise Exception("Bridge message signing failed")
            
        except Exception as e:
            logger.error(f"Failed to sign cross-chain message: {e}")
            raise
            
    async def get_amm_pricing_key(self, pool_address: str) -> Dict[str, str]:
        """Get AMM pricing oracle key"""
        try:
            response = self.vault.read(f"secret/defi/oracles/amm/{pool_address}")
            
            if not response or "data" not in response:
                # Generate pricing oracle key
                oracle_key = {
                    "pool_address": pool_address,
                    "oracle_address": Account.create().address,
                    "update_interval": 60,  # seconds
                    "deviation_threshold": "0.005",  # 0.5%
                    "created_at": datetime.utcnow().isoformat()
                }
                
                self.vault.write(
                    f"secret/defi/oracles/amm/{pool_address}",
                    **oracle_key
                )
                
                return oracle_key
            else:
                return response["data"]["data"]
                
        except Exception as e:
            logger.error(f"Failed to get AMM pricing key: {e}")
            raise
            
    async def get_flash_loan_protection_config(self) -> Dict[str, Any]:
        """Get flash loan protection configuration"""
        try:
            _, data = await self.consul.kv.get("defi/config/flash-loan-protection")
            
            if data and data["Value"]:
                return json.loads(data["Value"])
                
            # Default configuration
            default_config = {
                "enabled": True,
                "max_loan_amount": "1000000",  # USDC
                "fee_percentage": "0.09",  # 0.09%
                "cooldown_blocks": 1,
                "max_loans_per_block": 10,
                "require_collateral": False
            }
            
            await self.consul.kv.put(
                "defi/config/flash-loan-protection",
                json.dumps(default_config)
            )
            
            return default_config
            
        except Exception as e:
            logger.error(f"Failed to get flash loan config: {e}")
            return {}
            
    async def get_protocol_upgrade_key(self) -> Dict[str, str]:
        """Get protocol upgrade authorization key"""
        try:
            response = self.vault.read("secret/defi/governance/upgrade-key")
            
            if response and "data" in response:
                return response["data"]["data"]
                
            # This should be a multi-sig setup in production
            upgrade_key = {
                "type": "multi-sig",
                "signers": [
                    Account.create().address for _ in range(5)
                ],
                "threshold": 3,
                "timelock": 172800  # 48 hours
            }
            
            self.vault.write(
                "secret/defi/governance/upgrade-key",
                **upgrade_key
            )
            
            return upgrade_key
            
        except Exception as e:
            logger.error(f"Failed to get upgrade key: {e}")
            raise
            
    async def store_yield_strategy(self,
                                 strategy_id: str,
                                 strategy_config: Dict[str, Any]):
        """Store yield farming strategy configuration"""
        try:
            # Encrypt sensitive parameters
            if "private_keys" in strategy_config:
                encrypted_keys = []
                for key in strategy_config["private_keys"]:
                    encrypted = await self.encrypt_data(
                        key.encode(),
                        "defi-strategies"
                    )
                    encrypted_keys.append(encrypted)
                strategy_config["private_keys"] = encrypted_keys
                
            # Store in Vault
            self.vault.write(
                f"secret/defi/strategies/{strategy_id}",
                **strategy_config
            )
            
            # Store metadata in Consul
            await self.consul.kv.put(
                f"defi/strategies/{strategy_id}/metadata",
                json.dumps({
                    "created_at": datetime.utcnow().isoformat(),
                    "status": "active",
                    "apy": strategy_config.get("expected_apy", "0")
                })
            )
            
        except Exception as e:
            logger.error(f"Failed to store yield strategy: {e}")
            raise
            
    async def get_protocol_parameters(self) -> Dict[str, Any]:
        """Get current protocol parameters"""
        try:
            _, data = await self.consul.kv.get("defi/protocol/parameters")
            
            if data and data["Value"]:
                return json.loads(data["Value"])
                
            # Default parameters
            default_params = {
                "lending": {
                    "base_rate": "0.02",
                    "utilization_rate": "0.75",
                    "reserve_factor": "0.1"
                },
                "amm": {
                    "swap_fee": "0.003",
                    "protocol_fee": "0.0005",
                    "slippage_tolerance": "0.01"
                },
                "staking": {
                    "min_stake": "100",
                    "unbonding_period": 604800,  # 7 days
                    "reward_rate": "0.05"
                },
                "governance": {
                    "proposal_threshold": "10000",
                    "voting_period": 259200,  # 3 days
                    "timelock_delay": 172800  # 2 days
                }
            }
            
            await self.consul.kv.put(
                "defi/protocol/parameters",
                json.dumps(default_params)
            )
            
            return default_params
            
        except Exception as e:
            logger.error(f"Failed to get protocol parameters: {e}")
            return {}
            
    async def validate_transaction_limits(self,
                                        transaction_type: str,
                                        amount: Decimal,
                                        user_address: str) -> Dict[str, Any]:
        """Validate transaction against protocol limits"""
        try:
            # Get user limits from Consul
            _, user_data = await self.consul.kv.get(
                f"defi/users/{user_address}/limits"
            )
            
            if user_data and user_data["Value"]:
                user_limits = json.loads(user_data["Value"])
            else:
                # Default limits
                user_limits = {
                    "daily_limit": "100000",
                    "single_tx_limit": "10000",
                    "transactions_today": 0,
                    "volume_today": "0"
                }
                
            # Check limits
            daily_limit = Decimal(user_limits["daily_limit"])
            single_limit = Decimal(user_limits["single_tx_limit"])
            volume_today = Decimal(user_limits["volume_today"])
            
            if amount > single_limit:
                return {
                    "valid": False,
                    "reason": "Exceeds single transaction limit"
                }
                
            if volume_today + amount > daily_limit:
                return {
                    "valid": False,
                    "reason": "Exceeds daily limit"
                }
                
            # Update volume
            user_limits["volume_today"] = str(volume_today + amount)
            user_limits["transactions_today"] += 1
            user_limits["last_updated"] = datetime.utcnow().isoformat()
            
            await self.consul.kv.put(
                f"defi/users/{user_address}/limits",
                json.dumps(user_limits)
            )
            
            return {
                "valid": True,
                "remaining_daily": str(daily_limit - volume_today - amount)
            }
            
        except Exception as e:
            logger.error(f"Failed to validate transaction limits: {e}")
            return {"valid": False, "reason": str(e)}
            
    async def _load_protocol_config(self):
        """Load protocol configuration from Consul"""
        try:
            configs = [
                "supported-chains",
                "token-list",
                "pool-parameters",
                "risk-parameters"
            ]
            
            for config_name in configs:
                _, data = await self.consul.kv.get(f"defi/config/{config_name}")
                if data and data["Value"]:
                    self.protocol_config[config_name] = json.loads(data["Value"])
                    
            logger.info(f"Loaded {len(self.protocol_config)} protocol configurations")
            
        except Exception as e:
            logger.error(f"Failed to load protocol config: {e}")
            
    async def _setup_key_rotation(self):
        """Setup automatic key rotation for security"""
        async def rotate_keys():
            while True:
                try:
                    # Rotate oracle keys monthly
                    await self._rotate_oracle_keys()
                    
                    # Rotate bridge validator keys quarterly
                    await self._rotate_bridge_keys()
                    
                    await asyncio.sleep(86400)  # Daily check
                    
                except Exception as e:
                    logger.error(f"Key rotation error: {e}")
                    await asyncio.sleep(3600)
                    
        asyncio.create_task(rotate_keys())
        
    async def _setup_governance_watchers(self):
        """Setup watchers for governance changes"""
        async def watch_governance(param_type: str):
            index = None
            while True:
                try:
                    index, data = await self.consul.kv.get(
                        f"defi/governance/{param_type}",
                        index=index,
                        wait="30s"
                    )
                    
                    if data and data["Value"]:
                        new_params = json.loads(data["Value"])
                        await self._on_governance_change(param_type, new_params)
                        
                except Exception as e:
                    logger.error(f"Governance watcher error for {param_type}: {e}")
                    await asyncio.sleep(10)
                    
        # Watch important governance parameters
        for param_type in ["protocol-parameters", "risk-limits", "fee-structure"]:
            self._governance_watchers[param_type] = asyncio.create_task(
                watch_governance(param_type)
            )
            
    async def _on_governance_change(self, 
                                  param_type: str,
                                  new_params: Dict[str, Any]):
        """Handle governance parameter changes"""
        logger.info(f"Governance change detected: {param_type}")
        
        # Implement timelock and validation
        if param_type == "protocol-parameters":
            # Validate and apply new parameters after timelock
            pass
        elif param_type == "risk-limits":
            # Update risk management parameters
            pass
        elif param_type == "fee-structure":
            # Update fee parameters
            pass
            
    async def _get_oracle_nonce(self, oracle_type: str) -> int:
        """Get next nonce for oracle"""
        nonce_key = f"defi/oracles/{oracle_type}/nonce"
        
        # Get current nonce
        _, data = await self.consul.kv.get(nonce_key)
        
        if data and data["Value"]:
            current_nonce = int(data["Value"])
        else:
            current_nonce = 0
            
        # Increment and store
        new_nonce = current_nonce + 1
        await self.consul.kv.put(nonce_key, str(new_nonce))
        
        return new_nonce
        
    async def _get_bridge_nonce(self, 
                              source_chain: str,
                              target_chain: str) -> int:
        """Get next nonce for bridge"""
        nonce_key = f"defi/bridges/{source_chain}-{target_chain}/nonce"
        
        # Get current nonce
        _, data = await self.consul.kv.get(nonce_key)
        
        if data and data["Value"]:
            current_nonce = int(data["Value"])
        else:
            current_nonce = 0
            
        # Increment and store
        new_nonce = current_nonce + 1
        await self.consul.kv.put(nonce_key, str(new_nonce))
        
        return new_nonce
        
    async def _register_bridge_validator(self,
                                       bridge_id: str,
                                       chain: str,
                                       validator_address: str):
        """Register bridge validator in Consul"""
        await self.consul.kv.put(
            f"defi/bridges/{bridge_id}/{chain}/validators/{validator_address}",
            json.dumps({
                "address": validator_address,
                "registered_at": datetime.utcnow().isoformat(),
                "status": "active"
            })
        )
        
    async def _rotate_oracle_keys(self):
        """Rotate oracle signing keys"""
        try:
            # Check last rotation
            response = self.vault.read("secret/defi/oracles/rotation")
            
            if response and "data" in response:
                last_rotation = datetime.fromisoformat(
                    response["data"]["data"]["last_rotation"]
                )
                
                # Rotate monthly
                if datetime.utcnow() - last_rotation < timedelta(days=30):
                    return
                    
            # Rotate keys
            logger.info("Rotating oracle keys")
            
            # Update rotation timestamp
            self.vault.write(
                "secret/defi/oracles/rotation",
                last_rotation=datetime.utcnow().isoformat()
            )
            
            # Clear oracle cache
            self._oracle_cache.clear()
            
        except Exception as e:
            logger.error(f"Failed to rotate oracle keys: {e}")
            
    async def _rotate_bridge_keys(self):
        """Rotate bridge validator keys"""
        try:
            # Check last rotation
            response = self.vault.read("secret/defi/bridges/rotation")
            
            if response and "data" in response:
                last_rotation = datetime.fromisoformat(
                    response["data"]["data"]["last_rotation"]
                )
                
                # Rotate quarterly
                if datetime.utcnow() - last_rotation < timedelta(days=90):
                    return
                    
            # Rotate keys
            logger.info("Rotating bridge validator keys")
            
            # Update rotation timestamp
            self.vault.write(
                "secret/defi/bridges/rotation",
                last_rotation=datetime.utcnow().isoformat()
            )
            
        except Exception as e:
            logger.error(f"Failed to rotate bridge keys: {e}")
            
    async def shutdown(self):
        """Cleanup resources"""
        # Cancel governance watchers
        for task in self._governance_watchers.values():
            task.cancel()
            
        await super().shutdown() 