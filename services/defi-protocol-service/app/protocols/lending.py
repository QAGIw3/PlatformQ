"""
Lending Protocol Implementation

Handles NFT-backed loans, collateralized lending, and peer-to-peer lending.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum

from platformq_blockchain_common import (
    IBlockchainAdapter,
    Transaction,
    TransactionType,
    GasStrategy
)

from ..core.defi_manager import DeFiManager
from ..models.lending import LoanStatus, Loan, LoanOffer

logger = logging.getLogger(__name__)


class LendingProtocol:
    """Manages lending and borrowing operations"""
    
    def __init__(self, defi_manager: DeFiManager):
        self.defi_manager = defi_manager
        self._lending_contracts: Dict[str, str] = {}  # chain -> contract address
        self._active_loans: Dict[str, Loan] = {}  # loan_id -> loan
        self._loan_offers: Dict[str, LoanOffer] = {}  # offer_id -> offer
        
    async def initialize(self):
        """Initialize lending protocol contracts"""
        logger.info("Initializing Lending Protocol")
        
        # Load lending contract addresses for each chain
        for chain_type in self.defi_manager.get_supported_chains():
            self._lending_contracts[chain_type.value] = await self._get_contract_address(
                chain_type, "NFTLending"
            )
            
    async def shutdown(self):
        """Shutdown lending protocol"""
        logger.info("Shutting down Lending Protocol")
        self._active_loans.clear()
        self._loan_offers.clear()
        
    async def create_loan_offer(
        self,
        chain: str,
        nft_contract: str,
        max_loan_amount: Decimal,
        interest_rate: Decimal,
        min_duration: int,
        max_duration: int,
        payment_token: str,
        lender: str,
        accepted_collateral_types: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Create a loan offer for NFT collateral.
        
        Args:
            chain: Blockchain identifier
            nft_contract: Accepted NFT contract address (or "any")
            max_loan_amount: Maximum loan amount
            interest_rate: Annual interest rate (as decimal, e.g., 0.15 for 15%)
            min_duration: Minimum loan duration in seconds
            max_duration: Maximum loan duration in seconds
            payment_token: Token address for loan payments
            lender: Lender's wallet address
            accepted_collateral_types: List of accepted NFT collections
            
        Returns:
            Transaction result with offer ID
        """
        try:
            async with self.defi_manager.get_adapter(chain) as adapter:
                # Validate inputs
                if max_loan_amount <= 0:
                    raise ValueError("Loan amount must be positive")
                    
                if interest_rate < 0:
                    raise ValueError("Interest rate cannot be negative")
                    
                if min_duration <= 0 or max_duration < min_duration:
                    raise ValueError("Invalid duration range")
                    
                # Prepare contract call
                contract_address = self._lending_contracts[chain]
                
                # Encode offer parameters
                params = [
                    nft_contract,
                    int(max_loan_amount * 10**18),
                    int(interest_rate * 10000),  # Store as basis points
                    min_duration,
                    max_duration,
                    payment_token
                ]
                
                # Create transaction
                tx = await self._prepare_transaction(
                    adapter,
                    lender,
                    contract_address,
                    "createLoanOffer",
                    params
                )
                
                # Send transaction
                result = await adapter.send_transaction(tx)
                
                # Extract offer ID
                offer_id = await self._extract_offer_id(adapter, result.transaction_hash)
                
                # Store offer
                offer = LoanOffer(
                    id=offer_id,
                    chain=chain,
                    lender=lender,
                    nft_contract=nft_contract,
                    max_loan_amount=max_loan_amount,
                    interest_rate=interest_rate,
                    min_duration=min_duration,
                    max_duration=max_duration,
                    payment_token=payment_token,
                    is_active=True,
                    created_at=datetime.utcnow(),
                    accepted_collateral_types=accepted_collateral_types or [nft_contract]
                )
                self._loan_offers[offer_id] = offer
                
                logger.info(f"Created loan offer {offer_id} on {chain}")
                
                return {
                    "offer_id": offer_id,
                    "tx_hash": result.transaction_hash,
                    "gas_used": str(result.gas_used),
                    "status": "created"
                }
                
        except Exception as e:
            logger.error(f"Error creating loan offer: {e}")
            raise
            
    async def borrow_against_nft(
        self,
        chain: str,
        offer_id: str,
        token_id: int,
        loan_amount: Decimal,
        duration: int,
        borrower: str
    ) -> Dict[str, Any]:
        """
        Borrow against NFT collateral using an existing offer.
        
        Args:
            chain: Blockchain identifier
            offer_id: Loan offer ID
            token_id: NFT token ID to use as collateral
            loan_amount: Amount to borrow
            duration: Loan duration in seconds
            borrower: Borrower's wallet address
            
        Returns:
            Transaction result with loan details
        """
        try:
            offer = self._loan_offers.get(offer_id)
            if not offer:
                raise ValueError(f"Loan offer {offer_id} not found")
                
            if not offer.is_active:
                raise ValueError("Loan offer is not active")
                
            # Validate loan parameters
            if loan_amount > offer.max_loan_amount:
                raise ValueError(f"Loan amount exceeds maximum: {offer.max_loan_amount}")
                
            if duration < offer.min_duration or duration > offer.max_duration:
                raise ValueError("Duration outside allowed range")
                
            async with self.defi_manager.get_adapter(chain) as adapter:
                # Check NFT ownership
                # This would verify the borrower owns the NFT
                
                # Calculate repayment amount
                interest_amount = self._calculate_interest(
                    loan_amount,
                    offer.interest_rate,
                    duration
                )
                total_repayment = loan_amount + interest_amount
                
                # Prepare contract call
                contract_address = self._lending_contracts[chain]
                
                params = [
                    offer_id,
                    token_id,
                    int(loan_amount * 10**18),
                    duration
                ]
                
                # Create transaction
                tx = await self._prepare_transaction(
                    adapter,
                    borrower,
                    contract_address,
                    "borrowAgainstNFT",
                    params
                )
                
                # Send transaction
                result = await adapter.send_transaction(tx)
                
                # Extract loan ID
                loan_id = await self._extract_loan_id(adapter, result.transaction_hash)
                
                # Store loan
                loan = Loan(
                    id=loan_id,
                    chain=chain,
                    offer_id=offer_id,
                    borrower=borrower,
                    lender=offer.lender,
                    collateral_contract=offer.nft_contract,
                    collateral_token_id=token_id,
                    principal=loan_amount,
                    interest_rate=offer.interest_rate,
                    repayment_amount=total_repayment,
                    start_time=datetime.utcnow(),
                    due_time=datetime.utcnow() + timedelta(seconds=duration),
                    status=LoanStatus.ACTIVE,
                    payment_token=offer.payment_token
                )
                self._active_loans[loan_id] = loan
                
                logger.info(f"Created loan {loan_id} for {loan_amount} on {chain}")
                
                return {
                    "loan_id": loan_id,
                    "tx_hash": result.transaction_hash,
                    "gas_used": str(result.gas_used),
                    "repayment_due": loan.due_time.isoformat(),
                    "total_repayment": str(total_repayment)
                }
                
        except Exception as e:
            logger.error(f"Error borrowing against NFT: {e}")
            raise
            
    async def repay_loan(
        self,
        chain: str,
        loan_id: str,
        payer: str
    ) -> Dict[str, Any]:
        """
        Repay an active loan to reclaim collateral.
        
        Args:
            chain: Blockchain identifier
            loan_id: Loan identifier
            payer: Address making the payment
            
        Returns:
            Transaction result
        """
        try:
            loan = self._active_loans.get(loan_id)
            if not loan:
                raise ValueError(f"Loan {loan_id} not found")
                
            if loan.status != LoanStatus.ACTIVE:
                raise ValueError(f"Loan is not active: {loan.status}")
                
            async with self.defi_manager.get_adapter(chain) as adapter:
                contract_address = self._lending_contracts[chain]
                
                # Prepare repayment transaction
                params = [loan_id]
                
                tx = await self._prepare_transaction(
                    adapter,
                    payer,
                    contract_address,
                    "repayLoan",
                    params
                )
                
                # Send transaction
                result = await adapter.send_transaction(tx)
                
                # Update loan status
                loan.status = LoanStatus.REPAID
                loan.repaid_at = datetime.utcnow()
                
                logger.info(f"Loan {loan_id} repaid successfully")
                
                return {
                    "tx_hash": result.transaction_hash,
                    "gas_used": str(result.gas_used),
                    "status": "repaid",
                    "collateral_returned": True
                }
                
        except Exception as e:
            logger.error(f"Error repaying loan: {e}")
            raise
            
    async def liquidate_loan(
        self,
        chain: str,
        loan_id: str,
        liquidator: str
    ) -> Dict[str, Any]:
        """
        Liquidate an overdue loan.
        
        Args:
            chain: Blockchain identifier
            loan_id: Loan identifier
            liquidator: Address initiating liquidation
            
        Returns:
            Transaction result
        """
        try:
            loan = self._active_loans.get(loan_id)
            if not loan:
                raise ValueError(f"Loan {loan_id} not found")
                
            if loan.status != LoanStatus.ACTIVE:
                raise ValueError(f"Loan is not active: {loan.status}")
                
            if datetime.utcnow() <= loan.due_time:
                raise ValueError("Loan is not overdue")
                
            async with self.defi_manager.get_adapter(chain) as adapter:
                contract_address = self._lending_contracts[chain]
                
                # Prepare liquidation transaction
                params = [loan_id]
                
                tx = await self._prepare_transaction(
                    adapter,
                    liquidator,
                    contract_address,
                    "liquidateLoan",
                    params
                )
                
                # Send transaction
                result = await adapter.send_transaction(tx)
                
                # Update loan status
                loan.status = LoanStatus.LIQUIDATED
                loan.liquidated_at = datetime.utcnow()
                
                logger.info(f"Loan {loan_id} liquidated")
                
                return {
                    "tx_hash": result.transaction_hash,
                    "gas_used": str(result.gas_used),
                    "status": "liquidated",
                    "collateral_transferred": True
                }
                
        except Exception as e:
            logger.error(f"Error liquidating loan: {e}")
            raise
            
    async def get_loan_details(
        self,
        chain: str,
        loan_id: str
    ) -> Dict[str, Any]:
        """Get details of a loan"""
        loan = self._active_loans.get(loan_id)
        if not loan:
            # Try fetching from chain
            return await self._fetch_loan_from_chain(chain, loan_id)
            
        return {
            "id": loan.id,
            "status": loan.status.value,
            "borrower": loan.borrower,
            "lender": loan.lender,
            "collateral": {
                "contract": loan.collateral_contract,
                "token_id": loan.collateral_token_id
            },
            "principal": str(loan.principal),
            "interest_rate": str(loan.interest_rate),
            "repayment_amount": str(loan.repayment_amount),
            "start_time": loan.start_time.isoformat(),
            "due_time": loan.due_time.isoformat(),
            "is_overdue": datetime.utcnow() > loan.due_time,
            "payment_token": loan.payment_token
        }
        
    def _calculate_interest(
        self,
        principal: Decimal,
        annual_rate: Decimal,
        duration_seconds: int
    ) -> Decimal:
        """Calculate interest amount for a loan"""
        # Convert duration to years
        duration_years = Decimal(duration_seconds) / Decimal(365 * 24 * 3600)
        
        # Simple interest calculation
        # Could be enhanced with compound interest
        interest = principal * annual_rate * duration_years
        
        return interest.quantize(Decimal("0.01"))  # Round to 2 decimals
        
    async def _prepare_transaction(
        self,
        adapter: IBlockchainAdapter,
        from_address: str,
        to_address: str,
        method: str,
        params: List[Any],
        value: Decimal = Decimal("0")
    ) -> Transaction:
        """Prepare a transaction for lending contract interaction"""
        return Transaction(
            from_address=from_address,
            to_address=to_address,
            value=value,
            data=f"{method}({params})".encode(),  # Simplified
            type=TransactionType.CONTRACT_CALL,
            gas_strategy=GasStrategy.STANDARD
        )
        
    async def _extract_offer_id(
        self,
        adapter: IBlockchainAdapter,
        tx_hash: str
    ) -> str:
        """Extract offer ID from transaction events"""
        return f"offer_{tx_hash[:8]}"
        
    async def _extract_loan_id(
        self,
        adapter: IBlockchainAdapter,
        tx_hash: str
    ) -> str:
        """Extract loan ID from transaction events"""
        return f"loan_{tx_hash[:8]}"
        
    async def _get_contract_address(
        self,
        chain_type,
        contract_name: str
    ) -> str:
        """Get deployed contract address for chain"""
        return f"0x{hash(f'{chain_type.value}_{contract_name}') % 16**40:040x}"
        
    async def _fetch_loan_from_chain(
        self,
        chain: str,
        loan_id: str
    ) -> Dict[str, Any]:
        """Fetch loan details from blockchain"""
        return {
            "error": "Loan not found in cache",
            "loan_id": loan_id
        } 