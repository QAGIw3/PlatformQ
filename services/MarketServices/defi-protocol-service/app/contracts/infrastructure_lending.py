"""
Infrastructure Lending Contract Wrapper

Provides Python interface to the InfrastructureLending smart contract.
"""

from typing import Dict, Any, List, Optional
from decimal import Decimal
import json
import logging
from web3 import Web3
from eth_account import Account

from ..services.blockchain_pool import BlockchainPool
from ..models import ChainId

logger = logging.getLogger(__name__)


class InfrastructureLendingContract:
    """Wrapper for InfrastructureLending smart contract"""
    
    # Event signatures
    LOAN_CREATED_EVENT = Web3.keccak(text="LoanCreated(uint256,address,address)")
    COLLATERAL_DEPOSITED_EVENT = Web3.keccak(text="ResourceCollateralDeposited(uint256,uint256,uint256,uint256)")
    LOAN_REPAID_EVENT = Web3.keccak(text="LoanRepaid(uint256,uint256,address)")
    LOAN_LIQUIDATED_EVENT = Web3.keccak(text="LoanLiquidated(uint256,address,uint256)")
    COLLATERAL_REVALUED_EVENT = Web3.keccak(text="CollateralRevalued(uint256,uint256,uint256)")
    
    def __init__(
        self,
        chain_id: ChainId,
        contract_address: str,
        blockchain_pool: BlockchainPool
    ):
        self.chain_id = chain_id
        self.contract_address = Web3.toChecksumAddress(contract_address)
        self.blockchain_pool = blockchain_pool
        self.w3 = None
        self.contract = None
        self.abi = None
        
    async def initialize(self):
        """Initialize contract connection"""
        self.w3 = await self.blockchain_pool.get_web3(self.chain_id)
        
        # Load ABI
        self.abi = self._load_abi()
        
        # Create contract instance
        self.contract = self.w3.eth.contract(
            address=self.contract_address,
            abi=self.abi
        )
        
    def _load_abi(self) -> List[Dict]:
        """Load contract ABI"""
        # In production, this would load from a file or database
        # For now, return essential methods
        return [
            {
                "name": "borrowWithResource",
                "type": "function",
                "inputs": [
                    {"name": "resourceTokenAddress", "type": "address"},
                    {"name": "tokenId", "type": "uint256"},
                    {"name": "amount", "type": "uint256"},
                    {"name": "loanAmount", "type": "uint256"},
                    {"name": "duration", "type": "uint256"},
                    {"name": "paymentToken", "type": "address"}
                ],
                "outputs": [{"name": "loanId", "type": "uint256"}]
            },
            {
                "name": "repayLoan",
                "type": "function",
                "inputs": [
                    {"name": "loanId", "type": "uint256"},
                    {"name": "amount", "type": "uint256"}
                ],
                "outputs": []
            },
            {
                "name": "revalueCollateral",
                "type": "function",
                "inputs": [{"name": "loanId", "type": "uint256"}],
                "outputs": []
            },
            {
                "name": "loans",
                "type": "function",
                "stateMutability": "view",
                "inputs": [{"name": "loanId", "type": "uint256"}],
                "outputs": [
                    {"name": "loanId", "type": "uint256"},
                    {"name": "borrower", "type": "address"},
                    {"name": "lender", "type": "address"},
                    {"name": "nftContract", "type": "address"},
                    {"name": "tokenId", "type": "uint256"},
                    {"name": "principal", "type": "uint256"},
                    {"name": "interest", "type": "uint256"},
                    {"name": "duration", "type": "uint256"},
                    {"name": "startTime", "type": "uint256"},
                    {"name": "endTime", "type": "uint256"},
                    {"name": "status", "type": "uint8"},
                    {"name": "paymentToken", "type": "address"}
                ]
            },
            {
                "name": "resourceCollaterals",
                "type": "function",
                "stateMutability": "view",
                "inputs": [{"name": "loanId", "type": "uint256"}],
                "outputs": [
                    {"name": "tokenId", "type": "uint256"},
                    {"name": "amount", "type": "uint256"},
                    {"name": "resourceType", "type": "uint8"},
                    {"name": "tier", "type": "uint8"},
                    {"name": "region", "type": "string"},
                    {"name": "validUntil", "type": "uint256"},
                    {"name": "collateralValue", "type": "uint256"},
                    {"name": "lastValuationTime", "type": "uint256"}
                ]
            },
            {
                "name": "ltvRatios",
                "type": "function",
                "stateMutability": "view",
                "inputs": [
                    {"name": "resourceType", "type": "uint8"},
                    {"name": "tier", "type": "uint8"}
                ],
                "outputs": [{"name": "ratio", "type": "uint256"}]
            }
        ]
        
    async def create_loan(
        self,
        resource_token_id: int,
        amount: int,
        loan_amount: int,
        duration: int,
        payment_token: str,
        from_address: str,
        resource_token_address: Optional[str] = None
    ) -> str:
        """Create a new loan with resource tokens as collateral"""
        if not resource_token_address:
            # Get resource token address from config
            resource_token_address = await self._get_resource_token_address()
            
        # Build transaction
        function = self.contract.functions.borrowWithResource(
            Web3.toChecksumAddress(resource_token_address),
            resource_token_id,
            amount,
            loan_amount,
            duration,
            Web3.toChecksumAddress(payment_token)
        )
        
        # Send transaction
        tx_hash = await self._send_transaction(function, from_address)
        return tx_hash
        
    async def repay_loan(
        self,
        loan_id: int,
        amount: int,
        from_address: str
    ) -> str:
        """Repay a loan partially or fully"""
        function = self.contract.functions.repayLoan(loan_id, amount)
        tx_hash = await self._send_transaction(function, from_address)
        return tx_hash
        
    async def revalue_collateral(self, loan_id: int) -> str:
        """Trigger collateral revaluation"""
        function = self.contract.functions.revalueCollateral(loan_id)
        
        # Use a keeper account for this
        keeper_address = await self._get_keeper_address()
        tx_hash = await self._send_transaction(function, keeper_address)
        return tx_hash
        
    async def liquidate_loan(self, loan_id: int) -> str:
        """Liquidate an undercollateralized loan"""
        # In the actual contract, this might be an internal function
        # For now, assume it's callable
        function = self.contract.functions.liquidateLoan(loan_id)
        
        keeper_address = await self._get_keeper_address()
        tx_hash = await self._send_transaction(function, keeper_address)
        return tx_hash
        
    async def get_loan_details(self, loan_id: int) -> Dict[str, Any]:
        """Get loan details"""
        result = await self.contract.functions.loans(loan_id).call()
        
        return {
            "loanId": result[0],
            "borrower": result[1],
            "lender": result[2],
            "nftContract": result[3],
            "tokenId": result[4],
            "principal": result[5],
            "interest": result[6],
            "duration": result[7],
            "startTime": result[8],
            "endTime": result[9],
            "status": result[10],
            "paymentToken": result[11]
        }
        
    async def get_collateral_details(self, loan_id: int) -> Dict[str, Any]:
        """Get resource collateral details"""
        result = await self.contract.functions.resourceCollaterals(loan_id).call()
        
        return {
            "tokenId": result[0],
            "amount": result[1],
            "resourceType": result[2],
            "tier": result[3],
            "region": result[4],
            "validUntil": result[5],
            "collateralValue": result[6],
            "lastValuationTime": result[7]
        }
        
    async def get_ltv_ratio(self, resource_type: int, tier: int) -> int:
        """Get LTV ratio for resource type and tier"""
        return await self.contract.functions.ltvRatios(resource_type, tier).call()
        
    async def get_user_loan_ids(self, user: str) -> List[int]:
        """Get all loan IDs for a user"""
        # This would need a contract method to track user loans
        # For now, return empty list
        return []
        
    async def get_protocol_stats(self) -> Dict[str, Any]:
        """Get protocol statistics"""
        # This would need contract methods to track stats
        # For now, return mock data
        return {
            "totalLoans": 0,
            "activeLoans": 0,
            "totalValueLocked": 0,
            "totalBorrowed": 0,
            "liquidations": 0,
            "resourceBreakdown": {}
        }
        
    async def wait_for_transaction(self, tx_hash: str) -> Dict[str, Any]:
        """Wait for transaction confirmation"""
        receipt = await self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return receipt
        
    async def _send_transaction(self, function, from_address: str) -> str:
        """Build and send a transaction"""
        # Get nonce
        nonce = await self.w3.eth.get_transaction_count(from_address)
        
        # Estimate gas
        gas_estimate = await function.estimateGas({'from': from_address})
        
        # Get gas price
        gas_price = await self.w3.eth.gas_price
        
        # Build transaction
        tx = function.buildTransaction({
            'from': from_address,
            'nonce': nonce,
            'gas': int(gas_estimate * 1.2),  # Add 20% buffer
            'gasPrice': gas_price,
            'chainId': self.chain_id.value
        })
        
        # Sign transaction (in production, this would use a secure key management service)
        signed_tx = self.w3.eth.account.sign_transaction(
            tx,
            private_key=await self._get_private_key(from_address)
        )
        
        # Send transaction
        tx_hash = await self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        
        return tx_hash.hex()
        
    async def _get_resource_token_address(self) -> str:
        """Get resource token contract address"""
        # In production, this would come from config
        return "0x0000000000000000000000000000000000000000"
        
    async def _get_keeper_address(self) -> str:
        """Get keeper account address"""
        # In production, this would be a dedicated keeper account
        return "0x0000000000000000000000000000000000000000"
        
    async def _get_private_key(self, address: str) -> str:
        """Get private key for address"""
        # In production, this would use a secure key management service
        # Never store private keys in code!
        return "0x0000000000000000000000000000000000000000000000000000000000000000" 