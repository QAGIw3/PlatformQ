"""
Infrastructure Lending Protocol

Manages infrastructure-backed loans using resource tokens as collateral.
"""

from typing import Dict, Any, List, Optional
from decimal import Decimal
import logging
import asyncio
from datetime import datetime
from web3 import Web3
from eth_account import Account

from ..contracts import InfrastructureLendingContract
from ..services.blockchain_pool import BlockchainPool
from ..config import Config
from ..models import ChainId

logger = logging.getLogger(__name__)


class InfrastructureLendingProtocol:
    """Protocol for infrastructure-backed lending"""
    
    def __init__(self, blockchain_pool: BlockchainPool, config: Config):
        self.blockchain_pool = blockchain_pool
        self.config = config
        self.contracts: Dict[ChainId, InfrastructureLendingContract] = {}
        self._monitoring_tasks: Dict[int, asyncio.Task] = {}
        
    async def initialize(self):
        """Initialize lending contracts on all supported chains"""
        for chain_id in self.config.supported_chains:
            try:
                contract = InfrastructureLendingContract(
                    chain_id=chain_id,
                    contract_address=self.config.infrastructure_lending_addresses[chain_id],
                    blockchain_pool=self.blockchain_pool
                )
                await contract.initialize()
                self.contracts[chain_id] = contract
                logger.info(f"Initialized infrastructure lending on chain {chain_id}")
            except Exception as e:
                logger.error(f"Failed to initialize lending on chain {chain_id}: {e}")
                
    async def create_resource_loan(
        self,
        borrower: str,
        resource_token_id: int,
        amount: int,
        loan_amount: Decimal,
        duration_seconds: int,
        payment_token: str,
        chain_id: Optional[ChainId] = None
    ) -> Dict[str, Any]:
        """Create a new loan with resource tokens as collateral"""
        if not chain_id:
            chain_id = ChainId.POLYGON  # Default to Polygon
            
        contract = self.contracts.get(chain_id)
        if not contract:
            raise ValueError(f"Lending not available on chain {chain_id}")
            
        try:
            # Call smart contract
            tx_hash = await contract.create_loan(
                resource_token_id=resource_token_id,
                amount=amount,
                loan_amount=Web3.toWei(loan_amount, 'ether'),
                duration=duration_seconds,
                payment_token=payment_token,
                from_address=borrower
            )
            
            # Wait for transaction confirmation
            receipt = await contract.wait_for_transaction(tx_hash)
            
            # Parse events to get loan ID
            loan_id = None
            collateral_value = None
            interest_rate = None
            
            for log in receipt['logs']:
                if log['topics'][0] == contract.LOAN_CREATED_EVENT:
                    loan_id = int(log['data'][:66], 16)
                elif log['topics'][0] == contract.COLLATERAL_DEPOSITED_EVENT:
                    collateral_value = Decimal(int(log['data'][66:130], 16)) / Decimal(10**18)
                    
            # Get loan details from contract
            loan_details = await contract.get_loan_details(loan_id)
            interest_rate = Decimal(loan_details['interest_rate']) / Decimal(10000)
            total_due = Decimal(loan_details['principal'] + loan_details['interest']) / Decimal(10**18)
            
            return {
                "loan_id": loan_id,
                "collateral_value": collateral_value,
                "interest_rate": interest_rate,
                "total_due": total_due,
                "tx_hash": tx_hash.hex(),
                "chain_id": chain_id
            }
            
        except Exception as e:
            logger.error(f"Error creating resource loan: {e}")
            raise
            
    async def get_loan_details(self, loan_id: int, chain_id: Optional[ChainId] = None) -> Dict[str, Any]:
        """Get details of a specific loan"""
        if not chain_id:
            # Try all chains to find the loan
            for cid, contract in self.contracts.items():
                try:
                    details = await contract.get_loan_details(loan_id)
                    if details['borrower'] != '0x0000000000000000000000000000000000000000':
                        chain_id = cid
                        break
                except:
                    continue
                    
        if not chain_id:
            raise ValueError("Loan not found")
            
        contract = self.contracts[chain_id]
        loan = await contract.get_loan_details(loan_id)
        collateral = await contract.get_collateral_details(loan_id)
        
        return {
            "loan_id": loan_id,
            "borrower": loan['borrower'],
            "lender": loan['lender'],
            "resource_token_id": loan['tokenId'],
            "collateral_amount": collateral['amount'],
            "collateral_value": Decimal(collateral['collateralValue']) / Decimal(10**18),
            "principal": Decimal(loan['principal']) / Decimal(10**18),
            "interest": Decimal(loan['interest']) / Decimal(10**18),
            "total_due": Decimal(loan['principal'] + loan['interest']) / Decimal(10**18),
            "start_time": datetime.fromtimestamp(loan['startTime']),
            "end_time": datetime.fromtimestamp(loan['endTime']),
            "status": self._get_loan_status(loan['status']),
            "payment_token": loan['paymentToken'],
            "resource_type": collateral['resourceType'],
            "service_tier": collateral['tier'],
            "region": collateral['region'],
            "valid_until": datetime.fromtimestamp(collateral['validUntil']),
            "chain_id": chain_id
        }
        
    async def repay_loan(
        self,
        loan_id: int,
        payer: str,
        amount: Optional[Decimal] = None,
        chain_id: Optional[ChainId] = None
    ) -> Dict[str, Any]:
        """Repay a loan partially or fully"""
        if not chain_id:
            # Find the chain with this loan
            loan = await self.get_loan_details(loan_id)
            chain_id = loan['chain_id']
            
        contract = self.contracts[chain_id]
        
        # If no amount specified, repay full amount
        if amount is None:
            loan_details = await contract.get_loan_details(loan_id)
            amount = Decimal(loan_details['principal'] + loan_details['interest']) / Decimal(10**18)
            
        try:
            tx_hash = await contract.repay_loan(
                loan_id=loan_id,
                amount=Web3.toWei(amount, 'ether'),
                from_address=payer
            )
            
            receipt = await contract.wait_for_transaction(tx_hash)
            
            # Get updated loan details
            updated_loan = await contract.get_loan_details(loan_id)
            
            return {
                "amount_repaid": amount,
                "remaining_debt": Decimal(updated_loan['principal'] + updated_loan['interest']) / Decimal(10**18),
                "collateral_returned": updated_loan['status'] == 2,  # Repaid status
                "status": self._get_loan_status(updated_loan['status']),
                "tx_hash": tx_hash.hex()
            }
            
        except Exception as e:
            logger.error(f"Error repaying loan: {e}")
            raise
            
    async def revalue_collateral(self, loan_id: int, chain_id: Optional[ChainId] = None) -> Dict[str, Any]:
        """Revalue collateral for a loan"""
        if not chain_id:
            loan = await self.get_loan_details(loan_id)
            chain_id = loan['chain_id']
            
        contract = self.contracts[chain_id]
        
        try:
            # Get current collateral value
            old_collateral = await contract.get_collateral_details(loan_id)
            old_value = Decimal(old_collateral['collateralValue']) / Decimal(10**18)
            
            # Trigger revaluation
            tx_hash = await contract.revalue_collateral(loan_id)
            receipt = await contract.wait_for_transaction(tx_hash)
            
            # Get new collateral value
            new_collateral = await contract.get_collateral_details(loan_id)
            new_value = Decimal(new_collateral['collateralValue']) / Decimal(10**18)
            
            # Calculate health factor
            loan_details = await contract.get_loan_details(loan_id)
            principal = Decimal(loan_details['principal']) / Decimal(10**18)
            
            # Get LTV ratio for this resource type
            ltv_ratio = await contract.get_ltv_ratio(
                new_collateral['resourceType'],
                new_collateral['tier']
            )
            max_loan = new_value * Decimal(ltv_ratio) / Decimal(10000)
            health_factor = max_loan / principal if principal > 0 else Decimal('999')
            
            return {
                "old_value": old_value,
                "new_value": new_value,
                "health_factor": health_factor,
                "tx_hash": tx_hash.hex()
            }
            
        except Exception as e:
            logger.error(f"Error revaluing collateral: {e}")
            raise
            
    async def liquidate_loan(self, loan_id: int, chain_id: Optional[ChainId] = None):
        """Liquidate an undercollateralized loan"""
        if not chain_id:
            loan = await self.get_loan_details(loan_id)
            chain_id = loan['chain_id']
            
        contract = self.contracts[chain_id]
        
        try:
            # Trigger liquidation
            tx_hash = await contract.liquidate_loan(loan_id)
            receipt = await contract.wait_for_transaction(tx_hash)
            
            logger.info(f"Liquidated loan {loan_id} on chain {chain_id}")
            
        except Exception as e:
            logger.error(f"Error liquidating loan: {e}")
            
    async def monitor_loan_health(self, loan_id: int):
        """Monitor loan health and trigger liquidation if needed"""
        try:
            while True:
                loan = await self.get_loan_details(loan_id)
                
                # Stop monitoring if loan is no longer active
                if loan['status'] != 'active':
                    break
                    
                # Revalue collateral
                result = await self.revalue_collateral(loan_id, loan['chain_id'])
                
                # Check if liquidation is needed
                if result['health_factor'] < Decimal('1.0'):
                    logger.warning(f"Loan {loan_id} is undercollateralized, triggering liquidation")
                    await self.liquidate_loan(loan_id, loan['chain_id'])
                    break
                    
                # Check every 5 minutes
                await asyncio.sleep(300)
                
        except Exception as e:
            logger.error(f"Error monitoring loan {loan_id}: {e}")
        finally:
            # Remove from monitoring tasks
            self._monitoring_tasks.pop(loan_id, None)
            
    async def get_user_loans(
        self,
        user: str,
        status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get all loans for a user across all chains"""
        all_loans = []
        
        for chain_id, contract in self.contracts.items():
            try:
                # Get loan IDs for user (would need contract method)
                loan_ids = await contract.get_user_loan_ids(user)
                
                for loan_id in loan_ids:
                    loan = await self.get_loan_details(loan_id, chain_id)
                    
                    # Filter by status if specified
                    if status and loan['status'] != status:
                        continue
                        
                    all_loans.append(loan)
                    
            except Exception as e:
                logger.error(f"Error getting loans on chain {chain_id}: {e}")
                
        return all_loans
        
    async def get_protocol_stats(self) -> Dict[str, Any]:
        """Get protocol-wide statistics"""
        total_loans = 0
        active_loans = 0
        total_value_locked = Decimal('0')
        total_borrowed = Decimal('0')
        liquidations = 0
        resource_breakdown = {}
        
        for chain_id, contract in self.contracts.items():
            try:
                stats = await contract.get_protocol_stats()
                
                total_loans += stats['totalLoans']
                active_loans += stats['activeLoans']
                total_value_locked += Decimal(stats['totalValueLocked']) / Decimal(10**18)
                total_borrowed += Decimal(stats['totalBorrowed']) / Decimal(10**18)
                liquidations += stats['liquidations']
                
                # Aggregate resource breakdown
                for resource, amount in stats['resourceBreakdown'].items():
                    if resource not in resource_breakdown:
                        resource_breakdown[resource] = Decimal('0')
                    resource_breakdown[resource] += Decimal(amount) / Decimal(10**18)
                    
            except Exception as e:
                logger.error(f"Error getting stats from chain {chain_id}: {e}")
                
        # Calculate average LTV
        average_ltv = (total_borrowed / total_value_locked * 100) if total_value_locked > 0 else Decimal('0')
        
        # Calculate liquidation rate
        liquidation_rate = (liquidations / total_loans * 100) if total_loans > 0 else Decimal('0')
        
        return {
            "total_loans": total_loans,
            "active_loans": active_loans,
            "tvl": total_value_locked,
            "total_borrowed": total_borrowed,
            "average_ltv": average_ltv,
            "liquidation_rate": liquidation_rate,
            "resource_breakdown": resource_breakdown
        }
        
    async def get_ltv_ratios(self) -> Dict[str, Dict[str, float]]:
        """Get all LTV ratios"""
        # Use first available contract
        contract = next(iter(self.contracts.values()))
        
        resource_types = ['CPU', 'GPU', 'STORAGE', 'BANDWIDTH', 'MEMORY']
        service_tiers = ['STANDARD', 'PREMIUM', 'GUARANTEED']
        
        ltv_ratios = {}
        
        for resource in resource_types:
            ltv_ratios[resource] = {}
            for tier in service_tiers:
                ratio = await contract.get_ltv_ratio(
                    self._get_resource_enum(resource),
                    self._get_tier_enum(tier)
                )
                ltv_ratios[resource][tier] = ratio / 100  # Convert basis points to percentage
                
        return ltv_ratios
        
    def _get_loan_status(self, status: int) -> str:
        """Convert numeric status to string"""
        statuses = {
            0: "none",
            1: "active",
            2: "repaid",
            3: "defaulted",
            4: "liquidated"
        }
        return statuses.get(status, "unknown")
        
    def _get_resource_enum(self, resource: str) -> int:
        """Convert resource string to enum"""
        resources = {
            "CPU": 0,
            "GPU": 1,
            "STORAGE": 2,
            "BANDWIDTH": 3,
            "MEMORY": 4
        }
        return resources.get(resource.upper(), 0)
        
    def _get_tier_enum(self, tier: str) -> int:
        """Convert tier string to enum"""
        tiers = {
            "STANDARD": 0,
            "PREMIUM": 1,
            "GUARANTEED": 2
        }
        return tiers.get(tier.upper(), 0) 