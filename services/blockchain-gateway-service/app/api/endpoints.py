"""
API endpoints for blockchain gateway service.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
from decimal import Decimal
import logging

from app.core import BlockchainGateway, get_gateway
from platformq_blockchain_common import (
    ChainType, GasStrategy, SmartContract,
    ChainNotSupportedError,
    TransactionRequest,
    TransactionResult,
    TransactionStatus
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["blockchain"])


# Request/Response models
class TransactionRequest(BaseModel):
    chain: str = Field(..., description="Blockchain to use (ethereum, polygon, etc.)")
    from_address: str = Field(..., description="Sender address")
    to_address: str = Field(..., description="Receiver address")
    value: str = Field(..., description="Amount to send in native currency")
    data: Optional[str] = Field(None, description="Transaction data (hex string)")
    gas_strategy: str = Field("standard", description="Gas pricing strategy")
    private_key: Optional[str] = Field(None, description="Private key for signing (handle securely!)")
    

class ContractDeployRequest(BaseModel):
    chain: str = Field(..., description="Blockchain to deploy on")
    name: str = Field(..., description="Contract name")
    abi: List[Dict[str, Any]] = Field(..., description="Contract ABI")
    bytecode: str = Field(..., description="Contract bytecode")
    constructor_args: List[Any] = Field(default_factory=list, description="Constructor arguments")
    deployer_address: str = Field(..., description="Deployer address")
    private_key: Optional[str] = Field(None, description="Private key for deployment")
    

class ContractCallRequest(BaseModel):
    chain: str = Field(..., description="Blockchain to call on")
    contract_address: str = Field(..., description="Contract address")
    method: str = Field(..., description="Method name to call")
    params: List[Any] = Field(default_factory=list, description="Method parameters")
    abi: List[Dict[str, Any]] = Field(..., description="Contract ABI")


# Dependency to get gateway instance
def get_gateway():
    """Get blockchain gateway instance from app state"""
    from fastapi import Request
    from starlette.requests import Request as StarletteRequest
    
    def _get_gateway(request: Request):
        return request.app.state.gateway
    
    return _get_gateway


@router.get("/chains", response_model=List[Dict[str, Any]])
async def get_supported_chains(gateway=Depends(get_gateway())):
    """Get list of supported blockchains"""
    return gateway.get_supported_chains()


@router.get("/balance/{chain}/{address}")
async def get_balance(
    chain: str,
    address: str,
    token_address: Optional[str] = Query(None, description="Token contract address"),
    gateway=Depends(get_gateway())
):
    """Get balance for an address on specified chain"""
    try:
        chain_type = ChainType(chain.lower())
        return await gateway.get_balance(chain_type, address, token_address)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid chain: {chain}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/transaction/send")
async def send_transaction(
    request: TransactionRequest,
    gateway=Depends(get_gateway())
):
    """Send a transaction on specified blockchain"""
    try:
        chain_type = ChainType(request.chain.lower())
        gas_strategy = GasStrategy(request.gas_strategy.lower())
        
        result = await gateway.send_transaction(
            chain_type=chain_type,
            from_address=request.from_address,
            to_address=request.to_address,
            value=Decimal(request.value),
            data=request.data,
            gas_strategy=gas_strategy,
            private_key=request.private_key
        )
        
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ChainNotSupportedError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/transaction/{tx_hash}/status")
async def get_transaction_status(
    tx_hash: str,
    gateway=Depends(get_gateway())
):
    """Get status of a transaction"""
    try:
        return await gateway.get_transaction_status(tx_hash)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/gas/estimate")
async def estimate_gas(
    chain: str,
    from_address: str,
    to_address: str,
    value: str,
    data: Optional[str] = None,
    gateway=Depends(get_gateway())
):
    """Estimate gas for a transaction"""
    try:
        chain_type = ChainType(chain.lower())
        
        result = await gateway.estimate_gas(
            chain_type=chain_type,
            from_address=from_address,
            to_address=to_address,
            value=Decimal(value),
            data=data
        )
        
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/gas/price/{chain}")
async def get_gas_price(
    chain: str,
    strategy: str = Query("standard", description="Gas pricing strategy"),
    gateway=Depends(get_gateway())
):
    """Get optimal gas price for a chain"""
    try:
        chain_type = ChainType(chain.lower())
        gas_strategy = GasStrategy(strategy.lower())
        
        return await gateway.get_optimal_gas_price(chain_type, gas_strategy)
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/contract/deploy")
async def deploy_contract(
    request: ContractDeployRequest,
    gateway=Depends(get_gateway())
):
    """Deploy a smart contract"""
    try:
        chain_type = ChainType(request.chain.lower())
        
        contract = SmartContract(
            name=request.name,
            abi=request.abi,
            bytecode=request.bytecode,
            constructor_args=request.constructor_args
        )
        
        result = await gateway.deploy_contract(
            chain_type=chain_type,
            contract=contract,
            deployer_address=request.deployer_address,
            private_key=request.private_key
        )
        
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/contract/call")
async def call_contract(
    request: ContractCallRequest,
    gateway=Depends(get_gateway())
):
    """Call a smart contract method (read-only)"""
    try:
        chain_type = ChainType(request.chain.lower())
        
        result = await gateway.call_contract(
            chain_type=chain_type,
            contract_address=request.contract_address,
            method=request.method,
            params=request.params,
            abi=request.abi
        )
        
        return {"result": result}
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/chain/{chain}/metadata")
async def get_chain_metadata(
    chain: str,
    gateway=Depends(get_gateway())
):
    """Get current metadata for a blockchain"""
    try:
        chain_type = ChainType(chain.lower())
        return await gateway.get_chain_metadata(chain_type)
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "blockchain-gateway"} 


# Oracle endpoints
@router.get("/oracle/price/{asset}")
async def get_oracle_price(
    asset: str,
    chains: Optional[List[str]] = Query(None),
    gateway: BlockchainGateway = Depends(get_gateway)
):
    """Get aggregated price for an asset across multiple oracles"""
    try:
        price_aggregator = gateway.app.state.price_aggregator
        if not price_aggregator:
            raise HTTPException(status_code=503, detail="Oracle aggregator not available")
            
        price_data = await price_aggregator.get_price(
            asset=asset,
            chains=chains or ["ethereum", "polygon", "arbitrum"]
        )
        
        return {
            "asset": asset,
            "price": str(price_data["median_price"]),
            "sources": price_data["sources"],
            "timestamp": price_data["timestamp"]
        }
    except Exception as e:
        logger.error(f"Error fetching oracle price: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/oracle/feed/create")
async def create_price_feed(
    asset: str,
    chains: List[str],
    update_interval: int = 300,
    gateway: BlockchainGateway = Depends(get_gateway)
):
    """Create a new price feed for an asset"""
    try:
        price_aggregator = gateway.app.state.price_aggregator
        feed_id = await price_aggregator.create_feed(
            asset=asset,
            chains=chains,
            update_interval=update_interval
        )
        
        return {
            "feed_id": feed_id,
            "asset": asset,
            "chains": chains,
            "update_interval": update_interval
        }
    except Exception as e:
        logger.error(f"Error creating price feed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Cross-chain bridge endpoints
@router.post("/bridge/transfer")
async def bridge_transfer(
    from_chain: str,
    to_chain: str,
    asset: str,
    amount: str,
    recipient: str,
    gateway: BlockchainGateway = Depends(get_gateway)
):
    """Initiate a cross-chain bridge transfer"""
    try:
        bridge = gateway.app.state.cross_chain_bridge
        if not bridge:
            raise HTTPException(status_code=503, detail="Cross-chain bridge not available")
            
        transfer_id = await bridge.initiate_transfer(
            from_chain=from_chain,
            to_chain=to_chain,
            asset=asset,
            amount=Decimal(amount),
            recipient=recipient
        )
        
        return {
            "transfer_id": transfer_id,
            "status": "initiated",
            "from_chain": from_chain,
            "to_chain": to_chain,
            "asset": asset,
            "amount": amount
        }
    except Exception as e:
        logger.error(f"Error initiating bridge transfer: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/bridge/transfer/{transfer_id}")
async def get_bridge_transfer_status(
    transfer_id: str,
    gateway: BlockchainGateway = Depends(get_gateway)
):
    """Get status of a bridge transfer"""
    try:
        bridge = gateway.app.state.cross_chain_bridge
        status = await bridge.get_transfer_status(transfer_id)
        
        return {
            "transfer_id": transfer_id,
            "status": status["status"],
            "from_tx": status.get("from_tx"),
            "to_tx": status.get("to_tx"),
            "confirmations": status.get("confirmations", 0)
        }
    except Exception as e:
        logger.error(f"Error getting transfer status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Proposal execution endpoints
@router.post("/proposals/execute")
async def execute_proposal(
    proposal_id: str,
    chains: List[str],
    execution_strategy: str = "sequential",
    gateway: BlockchainGateway = Depends(get_gateway)
):
    """Execute a cross-chain proposal"""
    try:
        executor = gateway.app.state.proposal_executor
        if not executor:
            raise HTTPException(status_code=503, detail="Proposal executor not available")
            
        execution_id = await executor.execute_proposal(
            proposal_id=proposal_id,
            chains=chains,
            strategy=execution_strategy
        )
        
        return {
            "execution_id": execution_id,
            "proposal_id": proposal_id,
            "chains": chains,
            "strategy": execution_strategy,
            "status": "executing"
        }
    except Exception as e:
        logger.error(f"Error executing proposal: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/proposals/execution/{execution_id}")
async def get_proposal_execution_status(
    execution_id: str,
    gateway: BlockchainGateway = Depends(get_gateway)
):
    """Get status of proposal execution"""
    try:
        executor = gateway.app.state.proposal_executor
        status = await executor.get_execution_status(execution_id)
        
        return {
            "execution_id": execution_id,
            "status": status["overall_status"],
            "chain_statuses": status["chain_statuses"],
            "errors": status.get("errors", [])
        }
    except Exception as e:
        logger.error(f"Error getting execution status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# KYC/AML endpoints
@router.post("/compliance/check")
async def check_compliance(
    address: str,
    chain: str,
    check_type: str = "basic",
    gateway: BlockchainGateway = Depends(get_gateway)
):
    """Check KYC/AML compliance for an address"""
    try:
        kyc_aml = gateway.app.state.kyc_aml_service
        if not kyc_aml:
            raise HTTPException(status_code=503, detail="KYC/AML service not available")
            
        result = await kyc_aml.check_address(
            address=address,
            chain=chain,
            check_type=check_type
        )
        
        return {
            "address": address,
            "chain": chain,
            "risk_score": result["risk_score"],
            "flags": result["flags"],
            "compliant": result["compliant"]
        }
    except Exception as e:
        logger.error(f"Error checking compliance: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 