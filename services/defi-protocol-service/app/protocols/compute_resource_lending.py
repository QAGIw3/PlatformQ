"""
Compute Resource Lending Protocol

Enables lending and borrowing of compute resources (quantum, AI, network)
with specialized collateral requirements and liquidation mechanisms.
"""

from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal
from datetime import datetime, timedelta
import logging
import asyncio
from enum import Enum

from web3 import Web3
from platformq_shared.blockchain import BlockchainClient
from platformq_shared.models import ResourceType
from ..models import (
    LendingPool, LoanPosition, CollateralAsset,
    InterestRate, LiquidationEvent
)
from .lending import LendingProtocol

logger = logging.getLogger(__name__)


class ComputeCollateralType(str, Enum):
    COMPUTE_TOKEN = "compute_token"  # Tokenized compute resources
    STAKED_COMPUTE = "staked_compute"  # Staked compute tokens
    FUTURE_COMPUTE = "future_compute"  # Future compute contracts
    LP_TOKEN = "lp_token"  # Liquidity pool tokens
    QUALITY_BOND = "quality_bond"  # Quality guarantee bonds


class ComputeLoanType(str, Enum):
    SPOT_COMPUTE = "spot_compute"  # Immediate compute access
    RESERVED_COMPUTE = "reserved_compute"  # Reserved compute slots
    BURST_COMPUTE = "burst_compute"  # Burst compute capacity
    HYBRID_COMPUTE = "hybrid_compute"  # Multi-resource compute


class ComputeResourceLending(LendingProtocol):
    """Extended lending protocol for compute resources"""
    
    def __init__(
        self,
        blockchain_client: BlockchainClient,
        lending_pool_address: str,
        oracle_address: str,
        quantum_market_address: str,
        ai_market_address: str,
        network_market_address: str,
        aggregator_address: str
    ):
        super().__init__(
            blockchain_client,
            lending_pool_address,
            oracle_address
        )
        
        # Compute market addresses
        self.quantum_market_address = quantum_market_address
        self.ai_market_address = ai_market_address
        self.network_market_address = network_market_address
        self.aggregator_address = aggregator_address
        
        # Compute-specific parameters
        self.compute_ltv_ratios = {
            ComputeCollateralType.COMPUTE_TOKEN: Decimal("0.75"),
            ComputeCollateralType.STAKED_COMPUTE: Decimal("0.85"),
            ComputeCollateralType.FUTURE_COMPUTE: Decimal("0.60"),
            ComputeCollateralType.LP_TOKEN: Decimal("0.70"),
            ComputeCollateralType.QUALITY_BOND: Decimal("0.90")
        }
        
        self.liquidation_thresholds = {
            ComputeCollateralType.COMPUTE_TOKEN: Decimal("0.80"),
            ComputeCollateralType.STAKED_COMPUTE: Decimal("0.90"),
            ComputeCollateralType.FUTURE_COMPUTE: Decimal("0.65"),
            ComputeCollateralType.LP_TOKEN: Decimal("0.75"),
            ComputeCollateralType.QUALITY_BOND: Decimal("0.95")
        }
        
        # Interest rate models per resource type
        self.base_rates = {
            'quantum': Decimal("0.05"),  # 5% base for quantum
            'ai': Decimal("0.03"),  # 3% base for AI
            'network': Decimal("0.02")  # 2% base for network
        }
        
        # Quality score impact on rates
        self.quality_rate_adjustment = Decimal("0.02")  # 2% adjustment per 10 quality points
        
    async def create_compute_lending_pool(
        self,
        resource_type: str,
        initial_liquidity: Decimal,
        reserve_factor: int = 1000,  # 10%
        enable_quality_scoring: bool = True
    ) -> Dict[str, Any]:
        """
        Create a lending pool for compute resources
        
        Args:
            resource_type: Type of compute resource (quantum/ai/network)
            initial_liquidity: Initial liquidity in tokens
            reserve_factor: Reserve factor in basis points
            enable_quality_scoring: Whether to use quality scores for rates
            
        Returns:
            Pool creation result
        """
        try:
            # Deploy compute lending pool
            pool_factory = await self.blockchain.get_contract(
                self.lending_pool_address,
                "ComputeLendingFactory"
            )
            
            # Get market address for resource type
            market_address = self._get_market_address(resource_type)
            
            tx = await pool_factory.functions.createComputePool(
                resource_type,
                market_address,
                self.oracle_address,
                reserve_factor,
                enable_quality_scoring
            ).transact({'value': Web3.toWei(initial_liquidity, 'ether')})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Get pool address from event
            pool_created_event = receipt.events.get('ComputePoolCreated')
            pool_address = pool_created_event['poolAddress']
            
            # Initialize pool tracking
            pool_data = {
                'address': pool_address,
                'resource_type': resource_type,
                'total_liquidity': initial_liquidity,
                'available_liquidity': initial_liquidity,
                'total_borrowed': Decimal("0"),
                'reserve_factor': reserve_factor,
                'quality_scoring_enabled': enable_quality_scoring,
                'created_at': datetime.utcnow()
            }
            
            self._lending_pools[pool_address] = pool_data
            
            logger.info(f"Created compute lending pool {pool_address} for {resource_type}")
            
            return {
                'pool_address': pool_address,
                'resource_type': resource_type,
                'initial_liquidity': initial_liquidity,
                'tx_hash': tx
            }
            
        except Exception as e:
            logger.error(f"Failed to create compute lending pool: {e}")
            raise
    
    async def borrow_compute_resources(
        self,
        pool_address: str,
        borrower: str,
        resource_ids: List[int],
        amounts: List[int],
        duration_hours: int,
        collateral_type: ComputeCollateralType,
        collateral_amount: Decimal,
        loan_type: ComputeLoanType = ComputeLoanType.SPOT_COMPUTE
    ) -> Dict[str, Any]:
        """
        Borrow compute resources against collateral
        
        Args:
            pool_address: Lending pool address
            borrower: Borrower's address
            resource_ids: Resource IDs to borrow
            amounts: Amounts to borrow
            duration_hours: Loan duration
            collateral_type: Type of collateral
            collateral_amount: Amount of collateral
            loan_type: Type of compute loan
            
        Returns:
            Loan details
        """
        try:
            pool_data = self._lending_pools.get(pool_address)
            if not pool_data:
                raise ValueError("Pool not found")
            
            # Calculate total borrow value
            total_borrow_value = Decimal("0")
            resource_details = []
            
            for resource_id, amount in zip(resource_ids, amounts):
                # Get resource price and quality
                price = await self._get_resource_price(
                    resource_id,
                    pool_data['resource_type']
                )
                
                quality = await self._get_resource_quality(resource_id)
                
                # Calculate value
                resource_value = price * amount * duration_hours
                total_borrow_value += resource_value
                
                resource_details.append({
                    'resource_id': resource_id,
                    'amount': amount,
                    'price_per_hour': price,
                    'quality_score': quality['overall_score'],
                    'total_value': resource_value
                })
            
            # Check collateral requirements
            ltv_ratio = self.compute_ltv_ratios.get(
                collateral_type,
                Decimal("0.5")
            )
            
            required_collateral = total_borrow_value / ltv_ratio
            
            if collateral_amount < required_collateral:
                raise ValueError(
                    f"Insufficient collateral. Required: {required_collateral}, "
                    f"Provided: {collateral_amount}"
                )
            
            # Calculate interest rate
            interest_rate = await self._calculate_compute_interest_rate(
                pool_data,
                resource_details,
                loan_type
            )
            
            total_interest = total_borrow_value * interest_rate * duration_hours / 8760  # Annual to hourly
            
            # Check available liquidity
            if total_borrow_value > pool_data['available_liquidity']:
                raise ValueError("Insufficient liquidity in pool")
            
            # Create loan on-chain
            pool_contract = await self.blockchain.get_contract(
                pool_address,
                "ComputeLendingPool"
            )
            
            tx = await pool_contract.functions.borrow(
                resource_ids,
                amounts,
                duration_hours * 3600,  # Convert to seconds
                borrower,
                collateral_type,
                Web3.toWei(collateral_amount, 'ether')
            ).transact()
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Get loan ID from event
            loan_created_event = receipt.events.get('ComputeLoanCreated')
            loan_id = loan_created_event['loanId']
            
            # Update pool state
            pool_data['available_liquidity'] -= total_borrow_value
            pool_data['total_borrowed'] += total_borrow_value
            
            # Create loan position
            loan = LoanPosition(
                loan_id=loan_id,
                pool_address=pool_address,
                borrower=borrower,
                principal=total_borrow_value,
                interest_rate=interest_rate,
                collateral_type=collateral_type,
                collateral_amount=collateral_amount,
                start_time=datetime.utcnow(),
                maturity=datetime.utcnow() + timedelta(hours=duration_hours),
                resource_details=resource_details
            )
            
            self._active_loans[loan_id] = loan
            
            # Schedule resource allocation
            if loan_type == ComputeLoanType.SPOT_COMPUTE:
                await self._allocate_compute_resources(
                    resource_ids,
                    amounts,
                    borrower,
                    duration_hours
                )
            
            return {
                'loan_id': loan_id,
                'principal': total_borrow_value,
                'interest_rate': interest_rate,
                'total_interest': total_interest,
                'total_payment': total_borrow_value + total_interest,
                'collateral_ratio': collateral_amount / total_borrow_value,
                'maturity': loan.maturity,
                'resource_details': resource_details,
                'tx_hash': tx
            }
            
        except Exception as e:
            logger.error(f"Failed to borrow compute resources: {e}")
            raise
    
    async def liquidate_compute_loan(
        self,
        loan_id: str,
        liquidator: str
    ) -> Dict[str, Any]:
        """
        Liquidate an undercollateralized compute loan
        
        Args:
            loan_id: Loan to liquidate
            liquidator: Address performing liquidation
            
        Returns:
            Liquidation result
        """
        try:
            loan = self._active_loans.get(loan_id)
            if not loan:
                raise ValueError("Loan not found")
            
            # Check if loan is liquidatable
            current_collateral_value = await self._get_collateral_value(
                loan.collateral_type,
                loan.collateral_amount
            )
            
            # Calculate current loan value including accrued interest
            time_elapsed = (datetime.utcnow() - loan.start_time).total_seconds() / 3600
            accrued_interest = loan.principal * loan.interest_rate * time_elapsed / 8760
            current_loan_value = loan.principal + accrued_interest
            
            # Get liquidation threshold
            liquidation_threshold = self.liquidation_thresholds.get(
                loan.collateral_type,
                Decimal("0.75")
            )
            
            collateral_ratio = current_collateral_value / current_loan_value
            
            if collateral_ratio >= liquidation_threshold:
                return {
                    'success': False,
                    'reason': 'Loan is not liquidatable',
                    'collateral_ratio': collateral_ratio,
                    'threshold': liquidation_threshold
                }
            
            # Calculate liquidation bonus (5-15% based on severity)
            severity = (liquidation_threshold - collateral_ratio) / liquidation_threshold
            liquidation_bonus = Decimal("0.05") + (severity * Decimal("0.10"))
            
            # Execute liquidation on-chain
            pool_contract = await self.blockchain.get_contract(
                loan.pool_address,
                "ComputeLendingPool"
            )
            
            tx = await pool_contract.functions.liquidate(
                loan_id,
                liquidator
            ).transact()
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Get liquidation details from event
            liquidation_event = receipt.events.get('ComputeLoanLiquidated')
            
            if liquidation_event:
                # Release compute resources
                await self._release_compute_resources(loan)
                
                # Update loan status
                loan.status = 'liquidated'
                loan.liquidated_at = datetime.utcnow()
                
                # Record liquidation
                liquidation = LiquidationEvent(
                    loan_id=loan_id,
                    liquidator=liquidator,
                    collateral_seized=loan.collateral_amount,
                    debt_covered=current_loan_value,
                    liquidation_bonus=loan.collateral_amount * liquidation_bonus,
                    timestamp=datetime.utcnow()
                )
                
                return {
                    'success': True,
                    'tx_hash': tx,
                    'collateral_seized': loan.collateral_amount,
                    'debt_covered': current_loan_value,
                    'liquidation_bonus': liquidation.liquidation_bonus,
                    'total_received': loan.collateral_amount * (1 + liquidation_bonus)
                }
            
            return {'success': False, 'reason': 'Liquidation failed'}
            
        except Exception as e:
            logger.error(f"Failed to liquidate compute loan: {e}")
            raise
    
    async def flash_compute_loan(
        self,
        pool_address: str,
        borrower: str,
        resource_ids: List[int],
        amounts: List[int],
        callback_contract: str,
        callback_data: bytes
    ) -> Dict[str, Any]:
        """
        Flash loan for compute resources (must be returned in same transaction)
        
        Args:
            pool_address: Lending pool address
            borrower: Borrower's address
            resource_ids: Resources to flash borrow
            amounts: Amounts to borrow
            callback_contract: Contract to call with resources
            callback_data: Data to pass to callback
            
        Returns:
            Flash loan result
        """
        try:
            pool_data = self._lending_pools.get(pool_address)
            if not pool_data:
                raise ValueError("Pool not found")
            
            # Calculate flash loan fee (0.1%)
            flash_fee_rate = Decimal("0.001")
            
            total_value = Decimal("0")
            for resource_id, amount in zip(resource_ids, amounts):
                price = await self._get_resource_price(
                    resource_id,
                    pool_data['resource_type']
                )
                total_value += price * amount
            
            flash_fee = total_value * flash_fee_rate
            
            # Execute flash loan on-chain
            pool_contract = await self.blockchain.get_contract(
                pool_address,
                "ComputeLendingPool"
            )
            
            tx = await pool_contract.functions.flashLoan(
                resource_ids,
                amounts,
                callback_contract,
                callback_data
            ).transact({'from': borrower})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Check if flash loan succeeded
            flash_loan_event = receipt.events.get('FlashLoanExecuted')
            
            if flash_loan_event:
                return {
                    'success': True,
                    'tx_hash': tx,
                    'resources_borrowed': list(zip(resource_ids, amounts)),
                    'flash_fee': flash_fee,
                    'total_value': total_value
                }
            
            return {'success': False, 'reason': 'Flash loan execution failed'}
            
        except Exception as e:
            logger.error(f"Failed to execute flash compute loan: {e}")
            raise
    
    async def create_compute_cdp(
        self,
        borrower: str,
        collateral_resources: List[Dict[str, Any]],
        target_debt: Decimal,
        resource_type: str
    ) -> Dict[str, Any]:
        """
        Create a Collateralized Debt Position with compute resources
        
        Args:
            borrower: CDP owner
            collateral_resources: List of compute resources as collateral
            target_debt: Amount of stablecoin to mint
            resource_type: Type of borrowed compute resource
            
        Returns:
            CDP details
        """
        try:
            # Calculate collateral value
            total_collateral_value = Decimal("0")
            collateral_details = []
            
            for resource in collateral_resources:
                resource_id = resource['resource_id']
                amount = resource['amount']
                
                # Get price and quality
                price = await self._get_resource_price(resource_id, resource_type)
                quality = await self._get_resource_quality(resource_id)
                
                # Quality-adjusted value
                quality_multiplier = 1 + (quality['overall_score'] - 80) / 100
                adjusted_value = price * amount * quality_multiplier
                
                total_collateral_value += adjusted_value
                
                collateral_details.append({
                    'resource_id': resource_id,
                    'amount': amount,
                    'market_price': price,
                    'quality_score': quality['overall_score'],
                    'adjusted_value': adjusted_value
                })
            
            # Check collateralization ratio (minimum 150%)
            min_collateral_ratio = Decimal("1.5")
            if total_collateral_value < target_debt * min_collateral_ratio:
                raise ValueError(
                    f"Insufficient collateral. Need {target_debt * min_collateral_ratio}, "
                    f"have {total_collateral_value}"
                )
            
            # Create CDP on-chain
            cdp_manager = await self.blockchain.get_contract(
                self.lending_pool_address,
                "ComputeCDPManager"
            )
            
            tx = await cdp_manager.functions.createCDP(
                [r['resource_id'] for r in collateral_resources],
                [r['amount'] for r in collateral_resources],
                Web3.toWei(target_debt, 'ether')
            ).transact({'from': borrower})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Get CDP ID from event
            cdp_created_event = receipt.events.get('CDPCreated')
            cdp_id = cdp_created_event['cdpId']
            
            return {
                'cdp_id': cdp_id,
                'owner': borrower,
                'collateral_value': total_collateral_value,
                'debt': target_debt,
                'collateral_ratio': total_collateral_value / target_debt,
                'collateral_details': collateral_details,
                'liquidation_price': target_debt * min_collateral_ratio / total_collateral_value,
                'tx_hash': tx
            }
            
        except Exception as e:
            logger.error(f"Failed to create compute CDP: {e}")
            raise
    
    # Helper methods
    
    async def _calculate_compute_interest_rate(
        self,
        pool_data: Dict[str, Any],
        resource_details: List[Dict[str, Any]],
        loan_type: ComputeLoanType
    ) -> Decimal:
        """Calculate interest rate for compute loan"""
        # Base rate for resource type
        base_rate = self.base_rates.get(
            pool_data['resource_type'],
            Decimal("0.05")
        )
        
        # Utilization rate adjustment
        utilization = pool_data['total_borrowed'] / (
            pool_data['total_borrowed'] + pool_data['available_liquidity']
        )
        
        # Exponential curve for high utilization
        if utilization > Decimal("0.8"):
            utilization_multiplier = Decimal("2") ** ((utilization - Decimal("0.8")) * 10)
        else:
            utilization_multiplier = Decimal("1") + utilization * Decimal("0.5")
        
        # Quality score adjustment
        avg_quality = sum(
            r['quality_score'] for r in resource_details
        ) / len(resource_details)
        
        quality_adjustment = (avg_quality - 80) / 10 * self.quality_rate_adjustment
        
        # Loan type adjustment
        type_multipliers = {
            ComputeLoanType.SPOT_COMPUTE: Decimal("1.0"),
            ComputeLoanType.RESERVED_COMPUTE: Decimal("0.8"),
            ComputeLoanType.BURST_COMPUTE: Decimal("1.5"),
            ComputeLoanType.HYBRID_COMPUTE: Decimal("1.2")
        }
        
        type_multiplier = type_multipliers.get(loan_type, Decimal("1.0"))
        
        # Final rate calculation
        final_rate = base_rate * utilization_multiplier * type_multiplier
        final_rate -= quality_adjustment  # Higher quality = lower rate
        
        # Clamp between 1% and 50%
        return max(Decimal("0.01"), min(final_rate, Decimal("0.50")))
    
    async def _allocate_compute_resources(
        self,
        resource_ids: List[int],
        amounts: List[int],
        borrower: str,
        duration_hours: int
    ):
        """Allocate borrowed compute resources to borrower"""
        # In production, would interact with compute markets
        # to actually allocate the resources
        logger.info(
            f"Allocating resources {resource_ids} to {borrower} "
            f"for {duration_hours} hours"
        )
    
    async def _release_compute_resources(
        self,
        loan: LoanPosition
    ):
        """Release compute resources after loan repayment or liquidation"""
        # In production, would interact with compute markets
        # to release the allocated resources
        resource_ids = [r['resource_id'] for r in loan.resource_details]
        logger.info(f"Releasing resources {resource_ids} from loan {loan.loan_id}")
    
    async def _get_resource_price(
        self,
        resource_id: int,
        resource_type: str
    ) -> Decimal:
        """Get current market price for resource"""
        market_address = self._get_market_address(resource_type)
        
        market = await self.blockchain.get_contract(
            market_address,
            f"{resource_type.title()}Market"
        )
        
        price = await market.functions.getCurrentPrice(resource_id).call()
        return Decimal(str(price)) / 10**18
    
    async def _get_resource_quality(
        self,
        resource_id: int
    ) -> Dict[str, Any]:
        """Get resource quality from oracle"""
        oracle = await self.blockchain.get_contract(
            self.oracle_address,
            "ComputeResourceOracle"
        )
        
        quality_data = await oracle.functions.getQualityScore(
            resource_id
        ).call()
        
        return {
            'overall_score': quality_data['overallScore'],
            'components': quality_data['components'],
            'timestamp': quality_data['timestamp']
        }
    
    async def _get_collateral_value(
        self,
        collateral_type: ComputeCollateralType,
        amount: Decimal
    ) -> Decimal:
        """Get current value of collateral"""
        # In production, would query oracles for collateral values
        # For now, return a simple calculation
        return amount * Decimal("0.95")  # 5% haircut
    
    def _get_market_address(self, resource_type: str) -> str:
        """Get market address for resource type"""
        return {
            'quantum': self.quantum_market_address,
            'ai': self.ai_market_address,
            'network': self.network_market_address
        }.get(resource_type.lower()) 