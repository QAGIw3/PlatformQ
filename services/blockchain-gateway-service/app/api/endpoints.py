"""
API endpoints for blockchain gateway service.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
from decimal import Decimal

from platformq_blockchain_common import (
    ChainType, GasStrategy, SmartContract,
    ChainNotSupportedError
)

router = APIRouter()


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