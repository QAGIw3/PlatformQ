"""
Blockchain Gateway Service with Vault & Consul Integration
"""

from fastapi import FastAPI, Depends, HTTPException, Request
from contextlib import asynccontextmanager
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime
from decimal import Decimal
import logging
import os

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from platformq_shared.middleware.security_middleware import SecurityMiddleware

from .vault_consul_integration import (
    BlockchainVaultIntegration,
    BlockchainConsulIntegration,
    ChainConfig,
    TransactionPolicy
)
from .models import Transaction, TransactionRequest, ChainInfo, WalletBalance
from .web3_client import Web3Client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BlockchainGatewayService:
    """Blockchain Gateway Service with Vault & Consul Integration"""
    
    def __init__(self):
        self.app = FastAPI(title="Blockchain Gateway Service", version="2.0.0")
        self.vault_integration: Optional[BlockchainVaultIntegration] = None
        self.consul_integration: Optional[BlockchainConsulIntegration] = None
        self.web3_clients: Dict[str, Web3Client] = {}
        
    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        """Application lifespan management"""
        # Startup
        await self.startup()
        yield
        # Shutdown
        await self.shutdown()
        
    async def startup(self):
        """Service startup procedure"""
        logger.info("Starting Blockchain Gateway Service with Vault & Consul integration")
        
        try:
            # Initialize Vault client
            vault_client = VaultClient(
                vault_addr=os.getenv("VAULT_ADDR", "http://vault:8200"),
                role_id=os.getenv("VAULT_ROLE_ID"),
                secret_id=os.getenv("VAULT_SECRET_ID")
            )
            await vault_client.initialize()
            
            # Initialize Consul client
            consul_client = ConsulClient(
                host=os.getenv("CONSUL_HOST", "consul"),
                port=int(os.getenv("CONSUL_PORT", "8500"))
            )
            
            # Initialize integrations
            self.vault_integration = BlockchainVaultIntegration(vault_client)
            await self.vault_integration.initialize()
            
            self.consul_integration = BlockchainConsulIntegration(consul_client)
            await self.consul_integration.initialize()
            
            # Set up Web3 clients for enabled chains
            await self._setup_web3_clients()
            
            # Set up routes
            self._setup_routes()
            
            # Add security middleware
            security_middleware = SecurityMiddleware(
                vault_client=vault_client,
                consul_client=consul_client,
                service_name="blockchain-gateway"
            )
            self.app.add_middleware(security_middleware)
            
            # Start background tasks
            asyncio.create_task(self._health_check_loop())
            asyncio.create_task(self._monitor_transactions())
            asyncio.create_task(self._update_gas_prices())
            
            logger.info("Blockchain Gateway Service started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start Blockchain Gateway Service: {e}")
            raise
            
    async def shutdown(self):
        """Service shutdown procedure"""
        logger.info("Shutting down Blockchain Gateway Service")
        
        # Cancel background tasks
        for task in asyncio.all_tasks():
            if task.get_name() in ["health_check", "tx_monitor", "gas_updater"]:
                task.cancel()
                
        # Close Web3 connections
        for client in self.web3_clients.values():
            await client.close()
            
        # Deregister from Consul
        if self.consul_integration:
            await self.consul_integration.consul.deregister_service()
            
        logger.info("Blockchain Gateway Service shutdown complete")
        
    async def _setup_web3_clients(self):
        """Set up Web3 clients for enabled chains"""
        for chain_name, config in self.consul_integration._chain_configs.items():
            if config.enabled:
                try:
                    # Get RPC endpoints from Vault
                    endpoints = await self.vault_integration.get_rpc_endpoints(chain_name)
                    
                    if endpoints:
                        # Create Web3 client with failover
                        client = Web3Client(
                            chain_name=chain_name,
                            endpoints=endpoints,
                            chain_id=config.chain_id
                        )
                        self.web3_clients[chain_name] = client
                        logger.info(f"Initialized Web3 client for {chain_name}")
                        
                except Exception as e:
                    logger.error(f"Failed to set up Web3 client for {chain_name}: {e}")
                    
    def _setup_routes(self):
        """Set up API routes"""
        
        @self.app.get("/health")
        async def health_check():
            """Health check endpoint"""
            try:
                # Check Vault connectivity
                vault_healthy = await self._check_vault_health()
                
                # Check Consul connectivity
                consul_healthy = await self._check_consul_health()
                
                # Check chain connections
                chain_health = await self._check_chain_health()
                
                overall_status = "healthy"
                if not all([vault_healthy, consul_healthy]):
                    overall_status = "unhealthy"
                elif not all(chain_health.values()):
                    overall_status = "degraded"
                    
                health_data = {
                    "status": overall_status,
                    "service": "blockchain-gateway",
                    "checks": {
                        "vault": "healthy" if vault_healthy else "unhealthy",
                        "consul": "healthy" if consul_healthy else "unhealthy",
                        "chains": chain_health
                    },
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                if overall_status == "unhealthy":
                    raise HTTPException(status_code=503, detail=health_data)
                    
                return health_data
                
            except Exception as e:
                logger.error(f"Health check failed: {e}")
                raise HTTPException(status_code=503, detail="Service unhealthy")
                
        @self.app.get("/api/v1/chains", response_model=List[ChainInfo])
        async def get_supported_chains():
            """Get list of supported blockchain networks"""
            chains = []
            
            for chain_name, config in self.consul_integration._chain_configs.items():
                if config.enabled:
                    chains.append(ChainInfo(
                        name=chain_name,
                        chain_id=config.chain_id,
                        network=config.network,
                        enabled=config.enabled,
                        confirmations_required=config.confirmations
                    ))
                    
            return chains
            
        @self.app.get("/api/v1/wallets/{chain}")
        async def get_wallet_addresses(chain: str):
            """Get wallet addresses for a chain"""
            # Check if chain is supported
            config = await self.consul_integration.get_chain_config(chain)
            if not config or not config.enabled:
                raise HTTPException(404, f"Chain {chain} not supported")
                
            wallets = {}
            for wallet_type in ["hot-wallet", "cold-wallet", "gas"]:
                try:
                    address = await self.vault_integration.get_wallet_address(chain, wallet_type)
                    wallets[wallet_type] = address
                except:
                    pass
                    
            return {"chain": chain, "wallets": wallets}
            
        @self.app.get("/api/v1/balance/{chain}/{address}")
        async def get_balance(chain: str, address: str):
            """Get balance for an address"""
            # Check if chain is supported
            if chain not in self.web3_clients:
                raise HTTPException(404, f"Chain {chain} not supported")
                
            try:
                client = self.web3_clients[chain]
                balance_wei = await client.get_balance(address)
                balance_eth = balance_wei / 10**18
                
                return WalletBalance(
                    chain=chain,
                    address=address,
                    balance_wei=str(balance_wei),
                    balance_eth=str(balance_eth),
                    timestamp=datetime.utcnow()
                )
                
            except Exception as e:
                logger.error(f"Failed to get balance: {e}")
                raise HTTPException(500, f"Failed to get balance: {str(e)}")
                
        @self.app.post("/api/v1/transaction")
        async def send_transaction(
            tx_request: TransactionRequest,
            request: Request
        ):
            """Send a blockchain transaction"""
            # Check circuit breaker
            if not await self.consul_integration.check_circuit_breaker(f"{tx_request.chain}-signing"):
                raise HTTPException(503, "Service temporarily unavailable")
                
            # Get chain config
            config = await self.consul_integration.get_chain_config(tx_request.chain)
            if not config or not config.enabled:
                raise HTTPException(404, f"Chain {tx_request.chain} not supported")
                
            # Check transaction policy
            policy = await self.consul_integration.get_transaction_policy()
            value_eth = Decimal(tx_request.value_wei) / Decimal(10**18)
            
            if value_eth > policy.max_value_eth:
                raise HTTPException(400, f"Value exceeds limit: {value_eth} ETH")
                
            # Check if multisig required
            if value_eth > policy.require_multisig_above:
                tx_id = f"{tx_request.chain}-{datetime.utcnow().timestamp()}"
                approved = await self.consul_integration.coordinate_multisig_transaction(tx_id, 2)
                if not approved:
                    raise HTTPException(400, "Multisig approval required")
                    
            try:
                # Get Web3 client
                client = self.web3_clients[tx_request.chain]
                
                # Build transaction
                nonce = await client.get_nonce(tx_request.from_address)
                gas_price = await client.get_gas_price()
                
                transaction = {
                    "from": tx_request.from_address,
                    "to": tx_request.to_address,
                    "value": int(tx_request.value_wei),
                    "gas": tx_request.gas_limit or 21000,
                    "gasPrice": int(gas_price),
                    "nonce": nonce,
                    "chainId": config.chain_id,
                    "data": tx_request.data or "0x"
                }
                
                # Sign with Vault
                signed_tx = await self.vault_integration.sign_transaction(
                    tx_request.chain,
                    transaction,
                    tx_request.wallet_type or "hot-wallet"
                )
                
                # Broadcast transaction
                tx_hash = await client.send_raw_transaction(signed_tx)
                
                # Record metrics
                await self.consul_integration.record_transaction_metrics(
                    tx_request.chain,
                    tx_hash,
                    {
                        "gas_price": gas_price,
                        "value": tx_request.value_wei,
                        "status": "pending"
                    }
                )
                
                # Update daily volume
                await self.consul_integration.update_daily_volume(
                    tx_request.chain,
                    value_eth
                )
                
                return Transaction(
                    hash=tx_hash,
                    chain=tx_request.chain,
                    from_address=tx_request.from_address,
                    to_address=tx_request.to_address,
                    value=tx_request.value_wei,
                    gas_price=str(gas_price),
                    status="pending",
                    timestamp=datetime.utcnow()
                )
                
            except Exception as e:
                logger.error(f"Transaction failed: {e}")
                await self.consul_integration.trip_circuit_breaker(f"{tx_request.chain}-signing")
                raise HTTPException(500, f"Transaction failed: {str(e)}")
                
        @self.app.post("/api/v1/sign-message")
        async def sign_message(
            chain: str,
            message: str,
            wallet_type: str = "hot-wallet"
        ):
            """Sign a message for authentication"""
            try:
                signature = await self.vault_integration.sign_message(
                    chain,
                    message,
                    wallet_type
                )
                
                return {
                    "chain": chain,
                    "message": message,
                    "signature": signature,
                    "wallet_type": wallet_type
                }
                
            except Exception as e:
                logger.error(f"Message signing failed: {e}")
                raise HTTPException(500, f"Failed to sign message: {str(e)}")
                
        @self.app.get("/api/v1/transaction/{chain}/{tx_hash}")
        async def get_transaction_status(chain: str, tx_hash: str):
            """Get transaction status"""
            if chain not in self.web3_clients:
                raise HTTPException(404, f"Chain {chain} not supported")
                
            try:
                client = self.web3_clients[chain]
                tx_receipt = await client.get_transaction_receipt(tx_hash)
                
                if not tx_receipt:
                    return {"status": "pending", "hash": tx_hash}
                    
                return {
                    "status": "confirmed" if tx_receipt["status"] == 1 else "failed",
                    "hash": tx_hash,
                    "block_number": tx_receipt["blockNumber"],
                    "gas_used": tx_receipt["gasUsed"],
                    "confirmations": await client.get_confirmations(tx_receipt["blockNumber"])
                }
                
            except Exception as e:
                logger.error(f"Failed to get transaction status: {e}")
                raise HTTPException(500, f"Failed to get status: {str(e)}")
                
        @self.app.post("/api/v1/gas-wallets/rotate")
        async def rotate_gas_wallets():
            """Rotate gas wallet keys"""
            try:
                await self.vault_integration.rotate_gas_wallets()
                return {"status": "success", "message": "Gas wallet rotation initiated"}
            except Exception as e:
                logger.error(f"Gas wallet rotation failed: {e}")
                raise HTTPException(500, f"Rotation failed: {str(e)}")
                
    async def _check_vault_health(self) -> bool:
        """Check Vault connectivity"""
        try:
            # Try to read a test secret
            await self.vault_integration.vault.get_secret("blockchain-gateway/health-check")
            return True
        except:
            return False
            
    async def _check_consul_health(self) -> bool:
        """Check Consul connectivity"""
        try:
            await self.consul_integration.consul.kv_get("services/blockchain-gateway/health/status")
            return True
        except:
            return False
            
    async def _check_chain_health(self) -> Dict[str, bool]:
        """Check health of blockchain connections"""
        chain_health = {}
        
        for chain_name, client in self.web3_clients.items():
            try:
                # Try to get latest block
                await client.get_latest_block()
                chain_health[chain_name] = True
            except:
                chain_health[chain_name] = False
                
        return chain_health
        
    async def _health_check_loop(self):
        """Periodic health check"""
        while True:
            try:
                await asyncio.sleep(30)  # Every 30 seconds
                
                # Check chain connections
                chain_health = await self._check_chain_health()
                
                for chain, healthy in chain_health.items():
                    status = "healthy" if healthy else "unhealthy"
                    await self.consul_integration.consul.kv_put(
                        f"services/blockchain-gateway/chain-health/{chain}",
                        status
                    )
                    
            except Exception as e:
                logger.error(f"Health check loop error: {e}")
                
    async def _monitor_transactions(self):
        """Monitor pending transactions"""
        while True:
            try:
                await asyncio.sleep(60)  # Every minute
                
                # This would monitor pending transactions
                # and update their status in Consul
                
            except Exception as e:
                logger.error(f"Transaction monitor error: {e}")
                
    async def _update_gas_prices(self):
        """Update gas price recommendations"""
        while True:
            try:
                await asyncio.sleep(300)  # Every 5 minutes
                
                for chain_name, client in self.web3_clients.items():
                    try:
                        gas_price = await client.get_gas_price()
                        
                        # Store in Consul
                        await self.consul_integration.consul.kv_put(
                            f"services/blockchain-gateway/gas-prices/{chain_name}",
                            {
                                "price_wei": str(gas_price),
                                "price_gwei": str(gas_price / 10**9),
                                "updated_at": datetime.utcnow().isoformat()
                            }
                        )
                    except:
                        pass
                        
            except Exception as e:
                logger.error(f"Gas price updater error: {e}")


# Create app instance
blockchain_service = BlockchainGatewayService()
app = blockchain_service.app

# Set up lifespan
app.router.lifespan_context = blockchain_service.lifespan

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 