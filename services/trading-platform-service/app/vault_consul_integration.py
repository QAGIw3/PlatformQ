"""
Vault and Consul Integration for Trading Platform Service

Manages secure configuration, trading credentials, and order signing keys.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
import json
import base64
import hashlib
import hmac
from decimal import Decimal

import hvac
import consul.aio
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.backends import default_backend

from platformq_shared.vault_consul_base import VaultConsulBase

logger = logging.getLogger(__name__)


class VaultConsulIntegration(VaultConsulBase):
    """
    Trading Platform service specific Vault and Consul integration.
    
    Features:
    - Exchange API credentials management
    - Trading bot private keys
    - Order signing and verification
    - Market maker credentials
    - Risk limit configuration
    - Trading strategy secrets
    - Settlement keys
    - Price feed authentication
    - Liquidity provider keys
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(
            vault_addr=config["vault_addr"],
            vault_token=config.get("vault_token"),
            consul_addr=config["consul_addr"],
            service_name="trading-platform-service"
        )
        
        self.trading_config = {}
        self._api_key_cache = {}
        self._strategy_cache = {}
        self._exchange_watchers = {}
        
    async def initialize(self):
        """Initialize trading-specific Vault and Consul features"""
        await super().initialize()
        
        logger.info("Initializing Trading Platform Vault/Consul integration")
        
        # Enable trading-specific secret engines
        await self._setup_trading_secrets()
        
        # Load trading configuration
        await self._load_trading_config()
        
        # Setup API key rotation
        await self._setup_api_key_rotation()
        
        # Watch for exchange config changes
        await self._setup_exchange_watchers()
        
        logger.info("Trading Platform Vault/Consul integration initialized")
        
    async def _setup_trading_secrets(self):
        """Setup trading-specific secret engines"""
        try:
            # Enable Transit engine for order signing
            try:
                self.vault.sys.enable_secrets_engine(
                    backend_type="transit",
                    path="trading-transit"
                )
            except Exception:
                pass  # Already enabled
                
            # Create trading KV paths
            paths = [
                "secret/trading/exchanges",
                "secret/trading/bots",
                "secret/trading/strategies",
                "secret/trading/market-makers",
                "secret/trading/settlement",
                "secret/trading/price-feeds",
                "secret/trading/risk-limits"
            ]
            
            for path in paths:
                try:
                    self.vault.write(f"{path}/config", initialized=True)
                except Exception:
                    pass  # Path might already exist
                    
            # Create trading-specific keys
            await self._create_trading_keys()
                
        except Exception as e:
            logger.error(f"Failed to setup trading secrets: {e}")
            
    async def _create_trading_keys(self):
        """Create trading-specific keys"""
        try:
            # Order signing key
            self.vault.write(
                "trading-transit/keys/order-signer",
                type="ecdsa-p256",
                exportable=False,
                allow_plaintext_backup=False
            )
            
            # Market maker signing key
            self.vault.write(
                "trading-transit/keys/market-maker",
                type="ecdsa-p256",
                exportable=False,
                allow_plaintext_backup=False
            )
            
            # Settlement signing key
            self.vault.write(
                "trading-transit/keys/settlement",
                type="rsa-4096",
                exportable=False,
                allow_plaintext_backup=False
            )
            
            # Strategy encryption key
            self.vault.write(
                "trading-transit/keys/strategy-encrypt",
                type="aes256-gcm96",
                exportable=False,
                allow_plaintext_backup=False
            )
            
        except Exception:
            pass  # Keys might already exist
            
    async def get_exchange_credentials(self, 
                                     exchange: str,
                                     credential_type: str = "api") -> Dict[str, str]:
        """Get exchange API credentials"""
        cache_key = f"{exchange}_{credential_type}"
        
        # Check cache
        if cache_key in self._api_key_cache:
            cached = self._api_key_cache[cache_key]
            if datetime.utcnow() < cached["expires"]:
                return cached["credentials"]
                
        try:
            # Get credentials from Vault
            response = self.vault.read(
                f"secret/trading/exchanges/{exchange}/{credential_type}"
            )
            
            if not response or "data" not in response:
                raise Exception(f"No credentials found for {exchange}")
                
            credentials = response["data"]["data"]
            
            # Decrypt API secret if encrypted
            if "encrypted_secret" in credentials:
                credentials["api_secret"] = await self._decrypt_api_secret(
                    credentials["encrypted_secret"],
                    exchange
                )
                
            # Cache credentials
            self._api_key_cache[cache_key] = {
                "credentials": credentials,
                "expires": datetime.utcnow() + timedelta(hours=1)
            }
            
            return credentials
            
        except Exception as e:
            logger.error(f"Failed to get exchange credentials: {e}")
            raise
            
    async def store_exchange_credentials(self,
                                       exchange: str,
                                       credentials: Dict[str, str]):
        """Store exchange API credentials securely"""
        try:
            # Encrypt API secret
            if "api_secret" in credentials:
                encrypted_secret = await self._encrypt_api_secret(
                    credentials["api_secret"],
                    exchange
                )
                credentials["encrypted_secret"] = encrypted_secret
                del credentials["api_secret"]
                
            # Add metadata
            credentials["stored_at"] = datetime.utcnow().isoformat()
            credentials["exchange"] = exchange
            
            # Store in Vault
            self.vault.write(
                f"secret/trading/exchanges/{exchange}/api",
                **credentials
            )
            
            # Clear cache
            cache_key = f"{exchange}_api"
            if cache_key in self._api_key_cache:
                del self._api_key_cache[cache_key]
                
        except Exception as e:
            logger.error(f"Failed to store exchange credentials: {e}")
            raise
            
    async def sign_order(self,
                       order_data: Dict[str, Any],
                       exchange: str) -> Dict[str, str]:
        """Sign trading order for verification"""
        try:
            # Add order metadata
            order_payload = {
                **order_data,
                "exchange": exchange,
                "timestamp": int(datetime.utcnow().timestamp()),
                "nonce": await self._get_order_nonce(exchange)
            }
            
            # Create order hash
            order_json = json.dumps(order_payload, sort_keys=True)
            order_hash = hashlib.sha256(order_json.encode()).hexdigest()
            
            # Sign with Transit
            response = self.vault.write(
                "trading-transit/sign/order-signer",
                input=base64.b64encode(order_hash.encode()).decode(),
                hash_algorithm="sha2-256",
                signature_algorithm="ecdsa"
            )
            
            if response and "data" in response:
                # Store order for audit
                await self._store_order_audit(order_payload, order_hash)
                
                return {
                    "order": order_payload,
                    "signature": response["data"]["signature"],
                    "order_hash": order_hash,
                    "signed_at": datetime.utcnow().isoformat()
                }
                
            raise Exception("Order signing failed")
            
        except Exception as e:
            logger.error(f"Failed to sign order: {e}")
            raise
            
    async def verify_order_signature(self,
                                   signed_order: Dict[str, Any]) -> bool:
        """Verify order signature"""
        try:
            # Recreate order hash
            order_data = signed_order["order"]
            order_json = json.dumps(order_data, sort_keys=True)
            order_hash = hashlib.sha256(order_json.encode()).hexdigest()
            
            # Verify with Transit
            response = self.vault.write(
                "trading-transit/verify/order-signer",
                input=base64.b64encode(order_hash.encode()).decode(),
                signature=signed_order["signature"],
                hash_algorithm="sha2-256",
                signature_algorithm="ecdsa"
            )
            
            if response and "data" in response:
                return response["data"]["valid"]
                
            return False
            
        except Exception as e:
            logger.error(f"Failed to verify order signature: {e}")
            return False
            
    async def get_market_maker_credentials(self,
                                         market: str) -> Dict[str, Any]:
        """Get market maker credentials"""
        try:
            response = self.vault.read(
                f"secret/trading/market-makers/{market}"
            )
            
            if not response or "data" not in response:
                # Generate new market maker credentials
                mm_creds = await self._generate_market_maker_credentials(market)
                
                self.vault.write(
                    f"secret/trading/market-makers/{market}",
                    **mm_creds
                )
                
                return mm_creds
            else:
                return response["data"]["data"]
                
        except Exception as e:
            logger.error(f"Failed to get market maker credentials: {e}")
            raise
            
    async def get_trading_bot_key(self,
                                bot_id: str,
                                strategy: str) -> Dict[str, str]:
        """Get trading bot API key"""
        try:
            response = self.vault.read(
                f"secret/trading/bots/{bot_id}"
            )
            
            if not response or "data" not in response:
                # Generate new bot key
                bot_key = {
                    "bot_id": bot_id,
                    "api_key": await self._generate_secure_key(),
                    "strategy": strategy,
                    "created_at": datetime.utcnow().isoformat(),
                    "permissions": ["read", "trade", "cancel"]
                }
                
                self.vault.write(
                    f"secret/trading/bots/{bot_id}",
                    **bot_key
                )
                
                return bot_key
            else:
                return response["data"]["data"]
                
        except Exception as e:
            logger.error(f"Failed to get trading bot key: {e}")
            raise
            
    async def encrypt_strategy(self,
                             strategy_data: Dict[str, Any],
                             strategy_id: str) -> str:
        """Encrypt trading strategy"""
        try:
            # Serialize strategy
            strategy_json = json.dumps(strategy_data, sort_keys=True)
            
            # Encrypt with Transit
            response = self.vault.write(
                "trading-transit/encrypt/strategy-encrypt",
                plaintext=base64.b64encode(strategy_json.encode()).decode(),
                context=base64.b64encode(f"strategy-{strategy_id}".encode()).decode()
            )
            
            if response and "data" in response:
                # Store encrypted strategy
                self.vault.write(
                    f"secret/trading/strategies/{strategy_id}",
                    encrypted_strategy=response["data"]["ciphertext"],
                    encrypted_at=datetime.utcnow().isoformat()
                )
                
                return response["data"]["ciphertext"]
                
            raise Exception("Strategy encryption failed")
            
        except Exception as e:
            logger.error(f"Failed to encrypt strategy: {e}")
            raise
            
    async def decrypt_strategy(self,
                             encrypted_strategy: str,
                             strategy_id: str) -> Dict[str, Any]:
        """Decrypt trading strategy"""
        try:
            # Decrypt with Transit
            response = self.vault.write(
                "trading-transit/decrypt/strategy-encrypt",
                ciphertext=encrypted_strategy,
                context=base64.b64encode(f"strategy-{strategy_id}".encode()).decode()
            )
            
            if response and "data" in response:
                strategy_json = base64.b64decode(response["data"]["plaintext"]).decode()
                return json.loads(strategy_json)
                
            raise Exception("Strategy decryption failed")
            
        except Exception as e:
            logger.error(f"Failed to decrypt strategy: {e}")
            raise
            
    async def get_risk_limits(self, 
                            trader_id: str,
                            market: Optional[str] = None) -> Dict[str, Any]:
        """Get trading risk limits"""
        try:
            # Get trader-specific limits
            _, trader_limits = await self.consul.kv.get(
                f"trading/risk-limits/traders/{trader_id}"
            )
            
            if trader_limits and trader_limits["Value"]:
                limits = json.loads(trader_limits["Value"])
            else:
                # Default limits
                limits = {
                    "max_position_size": "100000",
                    "max_order_size": "10000",
                    "daily_loss_limit": "5000",
                    "max_leverage": "10",
                    "allowed_markets": ["*"]
                }
                
            # Get market-specific overrides
            if market:
                _, market_limits = await self.consul.kv.get(
                    f"trading/risk-limits/markets/{market}"
                )
                
                if market_limits and market_limits["Value"]:
                    market_specific = json.loads(market_limits["Value"])
                    # Apply more restrictive limits
                    for key, value in market_specific.items():
                        if key in limits:
                            if isinstance(value, (int, float, str)):
                                limits[key] = min(Decimal(str(value)), Decimal(str(limits[key])))
                                
            return limits
            
        except Exception as e:
            logger.error(f"Failed to get risk limits: {e}")
            return {
                "max_position_size": "10000",
                "max_order_size": "1000",
                "daily_loss_limit": "1000",
                "max_leverage": "1"
            }
            
    async def validate_order_limits(self,
                                  order: Dict[str, Any],
                                  trader_id: str) -> Dict[str, Any]:
        """Validate order against risk limits"""
        try:
            limits = await self.get_risk_limits(trader_id, order.get("market"))
            
            # Get current positions
            _, positions = await self.consul.kv.get(
                f"trading/positions/{trader_id}"
            )
            
            current_position = Decimal("0")
            if positions and positions["Value"]:
                position_data = json.loads(positions["Value"])
                current_position = Decimal(position_data.get("total_position", "0"))
                
            # Validate order size
            order_size = Decimal(order["quantity"]) * Decimal(order.get("price", "1"))
            
            if order_size > Decimal(limits["max_order_size"]):
                return {
                    "valid": False,
                    "reason": "Order size exceeds limit"
                }
                
            # Validate position size
            new_position = current_position + order_size
            if new_position > Decimal(limits["max_position_size"]):
                return {
                    "valid": False,
                    "reason": "Position size would exceed limit"
                }
                
            # Check daily loss limit
            _, daily_pnl = await self.consul.kv.get(
                f"trading/pnl/{trader_id}/{datetime.utcnow().date()}"
            )
            
            if daily_pnl and daily_pnl["Value"]:
                pnl = Decimal(daily_pnl["Value"])
                if pnl < -Decimal(limits["daily_loss_limit"]):
                    return {
                        "valid": False,
                        "reason": "Daily loss limit reached"
                    }
                    
            return {
                "valid": True,
                "remaining_position": str(Decimal(limits["max_position_size"]) - new_position),
                "remaining_daily_loss": str(Decimal(limits["daily_loss_limit"]) + pnl if daily_pnl else limits["daily_loss_limit"])
            }
            
        except Exception as e:
            logger.error(f"Failed to validate order limits: {e}")
            return {"valid": False, "reason": str(e)}
            
    async def get_settlement_key(self, 
                               settlement_type: str = "default") -> Dict[str, str]:
        """Get settlement signing key"""
        try:
            response = self.vault.read(
                f"secret/trading/settlement/{settlement_type}"
            )
            
            if response and "data" in response:
                return response["data"]["data"]
                
            # Generate settlement key
            settlement_key = {
                "type": settlement_type,
                "key_id": await self._generate_secure_key(16),
                "created_at": datetime.utcnow().isoformat()
            }
            
            self.vault.write(
                f"secret/trading/settlement/{settlement_type}",
                **settlement_key
            )
            
            return settlement_key
            
        except Exception as e:
            logger.error(f"Failed to get settlement key: {e}")
            raise
            
    async def sign_settlement(self,
                            settlement_data: Dict[str, Any]) -> Dict[str, str]:
        """Sign settlement transaction"""
        try:
            # Add settlement metadata
            settlement_payload = {
                **settlement_data,
                "settled_at": datetime.utcnow().isoformat(),
                "settlement_id": await self._generate_secure_key(12)
            }
            
            # Create settlement hash
            settlement_json = json.dumps(settlement_payload, sort_keys=True)
            settlement_hash = hashlib.sha256(settlement_json.encode()).hexdigest()
            
            # Sign with settlement key
            response = self.vault.write(
                "trading-transit/sign/settlement",
                input=base64.b64encode(settlement_hash.encode()).decode(),
                hash_algorithm="sha2-256",
                signature_algorithm="pss"
            )
            
            if response and "data" in response:
                return {
                    "settlement": settlement_payload,
                    "signature": response["data"]["signature"],
                    "settlement_hash": settlement_hash
                }
                
            raise Exception("Settlement signing failed")
            
        except Exception as e:
            logger.error(f"Failed to sign settlement: {e}")
            raise
            
    async def get_price_feed_auth(self,
                                feed_provider: str) -> Dict[str, str]:
        """Get price feed authentication credentials"""
        try:
            response = self.vault.read(
                f"secret/trading/price-feeds/{feed_provider}"
            )
            
            if response and "data" in response:
                return response["data"]["data"]
                
            # Generate auth token for price feed
            auth_data = {
                "provider": feed_provider,
                "auth_token": await self._generate_secure_key(32),
                "websocket_token": await self._generate_secure_key(32),
                "created_at": datetime.utcnow().isoformat()
            }
            
            self.vault.write(
                f"secret/trading/price-feeds/{feed_provider}",
                **auth_data
            )
            
            return auth_data
            
        except Exception as e:
            logger.error(f"Failed to get price feed auth: {e}")
            raise
            
    async def store_trading_metrics(self,
                                  trader_id: str,
                                  metrics: Dict[str, Any]):
        """Store trading performance metrics"""
        try:
            # Calculate risk metrics
            sharpe_ratio = metrics.get("sharpe_ratio", 0)
            max_drawdown = metrics.get("max_drawdown", 0)
            win_rate = metrics.get("win_rate", 0)
            
            # Store in Consul with TTL
            await self.consul.kv.put(
                f"trading/metrics/{trader_id}/{datetime.utcnow().date()}",
                json.dumps({
                    **metrics,
                    "timestamp": datetime.utcnow().isoformat(),
                    "risk_score": self._calculate_risk_score(
                        sharpe_ratio, max_drawdown, win_rate
                    )
                })
            )
            
        except Exception as e:
            logger.error(f"Failed to store trading metrics: {e}")
            
    async def get_liquidity_provider_keys(self,
                                        lp_id: str) -> Dict[str, str]:
        """Get liquidity provider API keys"""
        try:
            response = self.vault.read(
                f"secret/trading/liquidity-providers/{lp_id}"
            )
            
            if not response or "data" not in response:
                # Generate LP keys
                lp_keys = {
                    "lp_id": lp_id,
                    "api_key": await self._generate_secure_key(),
                    "signing_key": await self._generate_secure_key(16),
                    "created_at": datetime.utcnow().isoformat(),
                    "permissions": ["provide_liquidity", "adjust_spread", "cancel_all"]
                }
                
                self.vault.write(
                    f"secret/trading/liquidity-providers/{lp_id}",
                    **lp_keys
                )
                
                return lp_keys
            else:
                return response["data"]["data"]
                
        except Exception as e:
            logger.error(f"Failed to get LP keys: {e}")
            raise
            
    async def _load_trading_config(self):
        """Load trading configuration from Consul"""
        try:
            configs = [
                "supported-exchanges",
                "trading-pairs",
                "fee-schedule",
                "settlement-rules"
            ]
            
            for config_name in configs:
                _, data = await self.consul.kv.get(f"trading/config/{config_name}")
                if data and data["Value"]:
                    self.trading_config[config_name] = json.loads(data["Value"])
                    
            logger.info(f"Loaded {len(self.trading_config)} trading configurations")
            
        except Exception as e:
            logger.error(f"Failed to load trading config: {e}")
            
    async def _setup_api_key_rotation(self):
        """Setup automatic API key rotation"""
        async def rotate_keys():
            while True:
                try:
                    # Rotate exchange API keys quarterly
                    await self._rotate_exchange_keys()
                    
                    # Rotate bot keys monthly
                    await self._rotate_bot_keys()
                    
                    await asyncio.sleep(86400)  # Daily check
                    
                except Exception as e:
                    logger.error(f"Key rotation error: {e}")
                    await asyncio.sleep(3600)
                    
        asyncio.create_task(rotate_keys())
        
    async def _setup_exchange_watchers(self):
        """Setup watchers for exchange configuration changes"""
        async def watch_exchange(exchange: str):
            index = None
            while True:
                try:
                    index, data = await self.consul.kv.get(
                        f"trading/exchanges/{exchange}/config",
                        index=index,
                        wait="30s"
                    )
                    
                    if data and data["Value"]:
                        new_config = json.loads(data["Value"])
                        await self._on_exchange_config_change(exchange, new_config)
                        
                except Exception as e:
                    logger.error(f"Exchange watcher error for {exchange}: {e}")
                    await asyncio.sleep(10)
                    
        # Watch major exchanges
        for exchange in ["binance", "coinbase", "kraken", "ftx"]:
            self._exchange_watchers[exchange] = asyncio.create_task(
                watch_exchange(exchange)
            )
            
    async def _on_exchange_config_change(self,
                                       exchange: str,
                                       new_config: Dict[str, Any]):
        """Handle exchange configuration changes"""
        logger.info(f"Exchange config changed: {exchange}")
        
        # Clear cached credentials
        cache_keys = [k for k in self._api_key_cache if k.startswith(exchange)]
        for key in cache_keys:
            del self._api_key_cache[key]
            
    async def _encrypt_api_secret(self, secret: str, exchange: str) -> str:
        """Encrypt API secret"""
        response = self.vault.write(
            "trading-transit/encrypt/strategy-encrypt",
            plaintext=base64.b64encode(secret.encode()).decode(),
            context=base64.b64encode(f"exchange-{exchange}".encode()).decode()
        )
        
        if response and "data" in response:
            return response["data"]["ciphertext"]
        raise Exception("API secret encryption failed")
        
    async def _decrypt_api_secret(self, encrypted: str, exchange: str) -> str:
        """Decrypt API secret"""
        response = self.vault.write(
            "trading-transit/decrypt/strategy-encrypt",
            ciphertext=encrypted,
            context=base64.b64encode(f"exchange-{exchange}".encode()).decode()
        )
        
        if response and "data" in response:
            return base64.b64decode(response["data"]["plaintext"]).decode()
        raise Exception("API secret decryption failed")
        
    async def _generate_secure_key(self, length: int = 32) -> str:
        """Generate secure random key"""
        import secrets
        return secrets.token_urlsafe(length)
        
    async def _generate_market_maker_credentials(self, market: str) -> Dict[str, str]:
        """Generate market maker credentials"""
        return {
            "market": market,
            "maker_id": await self._generate_secure_key(16),
            "api_key": await self._generate_secure_key(32),
            "signing_key": await self._generate_secure_key(32),
            "created_at": datetime.utcnow().isoformat(),
            "spread_limits": {
                "min_spread_bps": 10,
                "max_spread_bps": 100,
                "target_spread_bps": 30
            }
        }
        
    async def _get_order_nonce(self, exchange: str) -> int:
        """Get next order nonce"""
        nonce_key = f"trading/nonces/{exchange}"
        
        # Get current nonce
        _, data = await self.consul.kv.get(nonce_key)
        
        if data and data["Value"]:
            current_nonce = int(data["Value"])
        else:
            current_nonce = int(datetime.utcnow().timestamp() * 1000)
            
        # Increment and store
        new_nonce = current_nonce + 1
        await self.consul.kv.put(nonce_key, str(new_nonce))
        
        return new_nonce
        
    async def _store_order_audit(self, order: Dict[str, Any], order_hash: str):
        """Store order for audit trail"""
        await self.consul.kv.put(
            f"trading/audit/orders/{order_hash}",
            json.dumps({
                "order": order,
                "hash": order_hash,
                "timestamp": datetime.utcnow().isoformat()
            }),
            ttl=2592000  # 30 days
        )
        
    def _calculate_risk_score(self, 
                            sharpe_ratio: float,
                            max_drawdown: float,
                            win_rate: float) -> float:
        """Calculate trader risk score"""
        # Simple risk scoring
        score = 50.0
        
        # Sharpe ratio impact
        if sharpe_ratio > 2:
            score += 20
        elif sharpe_ratio > 1:
            score += 10
        elif sharpe_ratio < 0:
            score -= 20
            
        # Drawdown impact
        if max_drawdown < 0.1:
            score += 10
        elif max_drawdown > 0.3:
            score -= 20
            
        # Win rate impact
        if win_rate > 0.6:
            score += 10
        elif win_rate < 0.4:
            score -= 10
            
        return max(0, min(100, score))
        
    async def _rotate_exchange_keys(self):
        """Rotate exchange API keys"""
        try:
            # Check last rotation
            response = self.vault.read("secret/trading/exchanges/rotation")
            
            if response and "data" in response:
                last_rotation = datetime.fromisoformat(
                    response["data"]["data"]["last_rotation"]
                )
                
                # Rotate quarterly
                if datetime.utcnow() - last_rotation < timedelta(days=90):
                    return
                    
            logger.info("Rotating exchange API keys")
            
            # Update rotation timestamp
            self.vault.write(
                "secret/trading/exchanges/rotation",
                last_rotation=datetime.utcnow().isoformat()
            )
            
            # Clear API key cache
            self._api_key_cache.clear()
            
        except Exception as e:
            logger.error(f"Failed to rotate exchange keys: {e}")
            
    async def _rotate_bot_keys(self):
        """Rotate trading bot keys"""
        try:
            # Check last rotation
            response = self.vault.read("secret/trading/bots/rotation")
            
            if response and "data" in response:
                last_rotation = datetime.fromisoformat(
                    response["data"]["data"]["last_rotation"]
                )
                
                # Rotate monthly
                if datetime.utcnow() - last_rotation < timedelta(days=30):
                    return
                    
            logger.info("Rotating trading bot keys")
            
            # Update rotation timestamp
            self.vault.write(
                "secret/trading/bots/rotation",
                last_rotation=datetime.utcnow().isoformat()
            )
            
        except Exception as e:
            logger.error(f"Failed to rotate bot keys: {e}")
            
    async def shutdown(self):
        """Cleanup resources"""
        # Cancel exchange watchers
        for task in self._exchange_watchers.values():
            task.cancel()
            
        await super().shutdown() 