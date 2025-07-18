"""
Lending API endpoints for DeFi protocol service.
"""

import logging
from typing import Dict, Any, Optional, List
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from platformq_shared import get_current_user
from ..protocols.lending import LendingProtocol
from ..core.defi_manager import DEFI_TRANSACTIONS, TRANSACTION_LATENCY
import time

logger = logging.getLogger(__name__)

router = APIRouter()


class LoanOfferRequest(BaseModel):
    """Request model for creating a loan offer"""
    chain: str = Field(..., description="Blockchain identifier")
    nft_contract: str = Field(..., description="Accepted NFT contract address")
    max_loan_amount: float = Field(..., gt=0, description="Maximum loan amount")
    interest_rate: float = Field(..., ge=0, le=1, description="Annual interest rate (0.15 = 15%)")
    min_duration: int = Field(..., gt=0, description="Minimum loan duration in seconds")
    max_duration: int = Field(..., gt=0, description="Maximum loan duration in seconds")
    payment_token: str = Field(..., description="Token address for loan payments")
    accepted_collateral_types: Optional[List[str]] = Field(None, description="List of accepted NFT collections")


class BorrowRequest(BaseModel):
    """Request model for borrowing against NFT"""
    offer_id: str = Field(..., description="Loan offer ID")
    token_id: int = Field(..., description="NFT token ID for collateral")
    loan_amount: float = Field(..., gt=0, description="Amount to borrow")
    duration: int = Field(..., gt=0, description="Loan duration in seconds")


def get_lending_protocol(request) -> LendingProtocol:
    """Dependency to get lending protocol instance"""
    return request.app.state.lending_protocol


@router.post("/offers/create")
async def create_loan_offer(
    request: LoanOfferRequest,
    current_user: Dict = Depends(get_current_user),
    lending_protocol: LendingProtocol = Depends(get_lending_protocol)
):
    """
    Create a loan offer for NFT collateral.
    
    Lenders can specify accepted NFT collections, loan terms, and interest rates.
    """
    try:
        start_time = time.time()
        
        # Validate duration range
        if request.min_duration > request.max_duration:
            raise HTTPException(
                status_code=400,
                detail="Minimum duration cannot exceed maximum duration"
            )
        
        # Create loan offer
        result = await lending_protocol.create_loan_offer(
            chain=request.chain,
            nft_contract=request.nft_contract,
            max_loan_amount=Decimal(str(request.max_loan_amount)),
            interest_rate=Decimal(str(request.interest_rate)),
            min_duration=request.min_duration,
            max_duration=request.max_duration,
            payment_token=request.payment_token,
            lender=current_user["wallet_address"],
            accepted_collateral_types=request.accepted_collateral_types
        )
        
        # Track metrics
        duration = time.time() - start_time
        DEFI_TRANSACTIONS.labels(
            chain=request.chain,
            protocol="lending",
            operation="create_offer"
        ).inc()
        TRANSACTION_LATENCY.labels(
            chain=request.chain,
            protocol="lending"
        ).observe(duration)
        
        return {
            "offer_id": result["offer_id"],
            "transaction_hash": result["tx_hash"],
            "gas_used": result["gas_used"],
            "status": "created"
        }
        
    except Exception as e:
        logger.error(f"Error creating loan offer: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/borrow")
async def borrow_against_nft(
    chain: str,
    request: BorrowRequest,
    current_user: Dict = Depends(get_current_user),
    lending_protocol: LendingProtocol = Depends(get_lending_protocol)
):
    """
    Borrow against NFT collateral using an existing loan offer.
    
    The NFT will be locked as collateral until the loan is repaid.
    """
    try:
        start_time = time.time()
        
        # Borrow against NFT
        result = await lending_protocol.borrow_against_nft(
            chain=chain,
            offer_id=request.offer_id,
            token_id=request.token_id,
            loan_amount=Decimal(str(request.loan_amount)),
            duration=request.duration,
            borrower=current_user["wallet_address"]
        )
        
        # Track metrics
        duration = time.time() - start_time
        DEFI_TRANSACTIONS.labels(
            chain=chain,
            protocol="lending",
            operation="borrow"
        ).inc()
        TRANSACTION_LATENCY.labels(
            chain=chain,
            protocol="lending"
        ).observe(duration)
        
        return {
            "loan_id": result["loan_id"],
            "transaction_hash": result["tx_hash"],
            "gas_used": result["gas_used"],
            "repayment_due": result["repayment_due"],
            "total_repayment": result["total_repayment"]
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error borrowing against NFT: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/loans/{loan_id}/repay")
async def repay_loan(
    loan_id: str,
    chain: str,
    current_user: Dict = Depends(get_current_user),
    lending_protocol: LendingProtocol = Depends(get_lending_protocol)
):
    """
    Repay an active loan to reclaim collateral.
    
    Must repay the full amount including interest.
    """
    try:
        start_time = time.time()
        
        # Repay loan
        result = await lending_protocol.repay_loan(
            chain=chain,
            loan_id=loan_id,
            payer=current_user["wallet_address"]
        )
        
        # Track metrics
        duration = time.time() - start_time
        DEFI_TRANSACTIONS.labels(
            chain=chain,
            protocol="lending",
            operation="repay"
        ).inc()
        TRANSACTION_LATENCY.labels(
            chain=chain,
            protocol="lending"
        ).observe(duration)
        
        return {
            "transaction_hash": result["tx_hash"],
            "gas_used": result["gas_used"],
            "status": result["status"],
            "collateral_returned": result["collateral_returned"]
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error repaying loan: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/loans/{loan_id}/liquidate")
async def liquidate_loan(
    loan_id: str,
    chain: str,
    current_user: Dict = Depends(get_current_user),
    lending_protocol: LendingProtocol = Depends(get_lending_protocol)
):
    """
    Liquidate an overdue loan.
    
    Any user can liquidate overdue loans and may receive a liquidation bonus.
    """
    try:
        start_time = time.time()
        
        # Liquidate loan
        result = await lending_protocol.liquidate_loan(
            chain=chain,
            loan_id=loan_id,
            liquidator=current_user["wallet_address"]
        )
        
        # Track metrics
        duration = time.time() - start_time
        DEFI_TRANSACTIONS.labels(
            chain=chain,
            protocol="lending",
            operation="liquidate"
        ).inc()
        TRANSACTION_LATENCY.labels(
            chain=chain,
            protocol="lending"
        ).observe(duration)
        
        return {
            "transaction_hash": result["tx_hash"],
            "gas_used": result["gas_used"],
            "status": result["status"],
            "collateral_transferred": result["collateral_transferred"]
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error liquidating loan: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/loans/{loan_id}")
async def get_loan_details(
    loan_id: str,
    chain: str,
    lending_protocol: LendingProtocol = Depends(get_lending_protocol)
):
    """
    Get detailed information about a loan.
    
    Returns loan status, amounts, collateral info, and payment details.
    """
    try:
        details = await lending_protocol.get_loan_details(chain, loan_id)
        
        if "error" in details:
            raise HTTPException(status_code=404, detail=details["error"])
            
        return details
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting loan details: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/offers")
async def list_loan_offers(
    chain: Optional[str] = None,
    lender: Optional[str] = None,
    min_amount: Optional[float] = None,
    max_rate: Optional[float] = None,
    limit: int = 20,
    offset: int = 0,
    lending_protocol: LendingProtocol = Depends(get_lending_protocol)
):
    """
    List available loan offers with optional filters.
    
    Can filter by chain, lender, loan amount, or interest rate.
    """
    try:
        # This would query from a database or indexer
        # For now, return mock data
        return {
            "offers": [],
            "total": 0,
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        logger.error(f"Error listing loan offers: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/loans")
async def list_loans(
    chain: Optional[str] = None,
    borrower: Optional[str] = None,
    lender: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
    lending_protocol: LendingProtocol = Depends(get_lending_protocol)
):
    """
    List loans with optional filters.
    
    Can filter by chain, borrower, lender, or status.
    """
    try:
        # This would query from a database or indexer
        # For now, return mock data
        return {
            "loans": [],
            "total": 0,
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        logger.error(f"Error listing loans: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 