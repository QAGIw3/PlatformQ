"""
Blockchain Connector API endpoints
"""

from typing import Dict, Any, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from ..core.chain_manager import ChainManager
from ..models.chain_types import ChainType

router = APIRouter(prefix="/api/v1", tags=["blockchain"])


# Request/Response Models
class BalanceRequest(BaseModel):
    chain: str = Field(..., description="Blockchain identifier")
    address: str = Field(..., description="Wallet address")
    token_address: Optional[str] = Field(None, description="Token contract address")


class TransactionRequest(BaseModel):
    chain: str = Field(..., description="Blockchain identifier")
    tx_hash: str = Field(..., description="Transaction hash")


class BroadcastRequest(BaseModel):
    chain: str = Field(..., description="Blockchain identifier")
    signed_tx: str = Field(..., description="Signed transaction data")


class GasEstimateRequest(BaseModel):
    chain: str = Field(..., description="Blockchain identifier")
    from_address: str = Field(..., description="Sender address")
    to_address: str = Field(..., description="Recipient address")
    value: str = Field(..., description="Amount to send")
    data: Optional[str] = Field(None, description="Transaction data")


class ContractCallRequest(BaseModel):
    chain: str = Field(..., description="Blockchain identifier")
    contract_address: str = Field(..., description="Contract address")
    method: str = Field(..., description="Method name")
    params: List[Any] = Field(default_factory=list, description="Method parameters")
    abi: List[Dict[str, Any]] = Field(..., description="Contract ABI")


# Dependency to get chain manager
def get_chain_manager(request) -> ChainManager:
    """Get chain manager instance"""
    # In production, this would return a singleton instance
    return request.app.state.chain_manager


@router.get("/chains")
async def get_supported_chains(
    chain_manager: ChainManager = Depends(get_chain_manager)
):
    """Get list of supported blockchains"""
    chains = chain_manager.get_supported_chains()
    
    return {
        "chains": [
            {
                "type": chain.value,
                "name": chain.value.title(),
                "supported": True
            }
            for chain in chains
        ]
    }


@router.get("/chains/{chain}/info")
async def get_chain_info(
    chain: str,
    chain_manager: ChainManager = Depends(get_chain_manager)
):
    """Get information about a specific chain"""
    try:
        chain_type = ChainType(chain)
        config = chain_manager.get_chain_info(chain_type)
        
        if not config:
            raise HTTPException(status_code=404, detail=f"Chain {chain} not found")
            
        return {
            "chain": chain,
            "chain_id": config.chain_id,
            "name": config.name,
            "symbol": config.symbol,
            "explorer_url": config.explorer_url,
            "features": config.features,
            "endpoints": len(config.endpoints)
        }
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid chain: {chain}")


@router.get("/chains/{chain}/status")
async def get_chain_status(
    chain: str,
    chain_manager: ChainManager = Depends(get_chain_manager)
):
    """Get health status of a blockchain"""
    try:
        chain_type = ChainType(chain)
        
        # Check if chain is configured
        if chain_type not in chain_manager.chain_configs:
            raise HTTPException(status_code=404, detail=f"Chain {chain} not configured")
            
        # Get health scores
        health_scores = chain_manager._endpoint_health.get(chain_type, {})
        avg_health = sum(health_scores.values()) / len(health_scores) if health_scores else 0
        
        return {
            "chain": chain,
            "status": "healthy" if avg_health > 0.7 else "degraded" if avg_health > 0.3 else "unhealthy",
            "health_score": avg_health,
            "endpoints": [
                {
                    "url": endpoint,
                    "health": score
                }
                for endpoint, score in health_scores.items()
            ]
        }
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid chain: {chain}")


@router.post("/balance")
async def get_balance(
    request: BalanceRequest,
    chain_manager: ChainManager = Depends(get_chain_manager)
):
    """Get balance for an address"""
    try:
        chain_type = ChainType(request.chain)
        result = await chain_manager.get_balance(
            chain_type,
            request.address,
            request.token_address
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/transaction")
async def get_transaction(
    request: TransactionRequest,
    chain_manager: ChainManager = Depends(get_chain_manager)
):
    """Get transaction details"""
    try:
        chain_type = ChainType(request.chain)
        result = await chain_manager.get_transaction(
            chain_type,
            request.tx_hash
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/broadcast")
async def broadcast_transaction(
    request: BroadcastRequest,
    chain_manager: ChainManager = Depends(get_chain_manager)
):
    """Broadcast a signed transaction"""
    try:
        chain_type = ChainType(request.chain)
        tx_hash = await chain_manager.broadcast_transaction(
            chain_type,
            request.signed_tx
        )
        return {"tx_hash": tx_hash, "chain": request.chain}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/gas/estimate")
async def estimate_gas(
    request: GasEstimateRequest,
    chain_manager: ChainManager = Depends(get_chain_manager)
):
    """Estimate gas for a transaction"""
    try:
        chain_type = ChainType(request.chain)
        result = await chain_manager.estimate_gas(
            chain_type,
            request.from_address,
            request.to_address,
            request.value,
            request.data
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/gas/price/{chain}")
async def get_gas_price(
    chain: str,
    chain_manager: ChainManager = Depends(get_chain_manager)
):
    """Get current gas price for a chain"""
    try:
        chain_type = ChainType(chain)
        
        # Get an adapter
        adapter = await chain_manager._get_adapter_from_pool(chain_type)
        if not adapter:
            raise HTTPException(status_code=503, detail=f"No adapter available for {chain}")
            
        try:
            result = await adapter.get_gas_price()
            return result
        finally:
            await chain_manager._return_adapter_to_pool(chain_type, adapter)
            
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid chain: {chain}")
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/nonce/{chain}/{address}")
async def get_nonce(
    chain: str,
    address: str,
    chain_manager: ChainManager = Depends(get_chain_manager)
):
    """Get next nonce for an address"""
    try:
        chain_type = ChainType(chain)
        
        # Get an adapter
        adapter = await chain_manager._get_adapter_from_pool(chain_type)
        if not adapter:
            raise HTTPException(status_code=503, detail=f"No adapter available for {chain}")
            
        try:
            nonce = await adapter.get_nonce(address)
            return {"address": address, "nonce": nonce}
        finally:
            await chain_manager._return_adapter_to_pool(chain_type, adapter)
            
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/contract/call")
async def call_contract(
    request: ContractCallRequest,
    chain_manager: ChainManager = Depends(get_chain_manager)
):
    """Call a smart contract method (read-only)"""
    try:
        chain_type = ChainType(request.chain)
        
        # Get an adapter
        adapter = await chain_manager._get_adapter_from_pool(chain_type)
        if not adapter:
            raise HTTPException(status_code=503, detail=f"No adapter available for {request.chain}")
            
        try:
            result = await adapter.call_contract(
                request.contract_address,
                request.method,
                request.params,
                request.abi
            )
            return {"result": result}
        finally:
            await chain_manager._return_adapter_to_pool(chain_type, adapter)
            
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/block/{chain}/latest")
async def get_latest_block(
    chain: str,
    chain_manager: ChainManager = Depends(get_chain_manager)
):
    """Get latest block number"""
    try:
        chain_type = ChainType(chain)
        
        # Get an adapter
        adapter = await chain_manager._get_adapter_from_pool(chain_type)
        if not adapter:
            raise HTTPException(status_code=503, detail=f"No adapter available for {chain}")
            
        try:
            block_number = await adapter.get_latest_block()
            return {"chain": chain, "block_number": block_number}
        finally:
            await chain_manager._return_adapter_to_pool(chain_type, adapter)
            
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid chain: {chain}")
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/block/{chain}/{block_number}")
async def get_block(
    chain: str,
    block_number: int,
    chain_manager: ChainManager = Depends(get_chain_manager)
):
    """Get block details"""
    try:
        chain_type = ChainType(chain)
        
        # Get an adapter
        adapter = await chain_manager._get_adapter_from_pool(chain_type)
        if not adapter:
            raise HTTPException(status_code=503, detail=f"No adapter available for {chain}")
            
        try:
            block = await adapter.get_block(block_number)
            return block
        finally:
            await chain_manager._return_adapter_to_pool(chain_type, adapter)
            
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except NotImplementedError as e:
        raise HTTPException(status_code=501, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 