from typing import Dict, Any, Optional, List, Tuple
from web3 import Web3
from web3.contract import Contract
from eth_account import Account
from eth_account.messages import encode_defunct
import json
import asyncio
from datetime import datetime
import uuid

from .base_bridge import BaseBridge
from ..models.bridge_models import (
    BridgeTransfer, BridgeAttestation, BridgeEvent,
    TransferStatus, TokenType
)


# Simplified bridge contract ABI
BRIDGE_CONTRACT_ABI = [
    {
        "name": "lockTokens",
        "type": "function",
        "inputs": [
            {"name": "token", "type": "address"},
            {"name": "amount", "type": "uint256"},
            {"name": "targetChain", "type": "string"},
            {"name": "targetAddress", "type": "address"}
        ],
        "outputs": [{"name": "lockId", "type": "bytes32"}]
    },
    {
        "name": "mintTokens",
        "type": "function",
        "inputs": [
            {"name": "token", "type": "address"},
            {"name": "amount", "type": "uint256"},
            {"name": "recipient", "type": "address"},
            {"name": "sourceChain", "type": "string"},
            {"name": "lockId", "type": "bytes32"},
            {"name": "signatures", "type": "bytes[]"}
        ],
        "outputs": [{"name": "success", "type": "bool"}]
    },
    {
        "name": "isPaused",
        "type": "function",
        "inputs": [],
        "outputs": [{"name": "", "type": "bool"}],
        "view": True
    }
]

ERC20_ABI = [
    {
        "name": "balanceOf",
        "type": "function",
        "inputs": [{"name": "account", "type": "address"}],
        "outputs": [{"name": "", "type": "uint256"}],
        "view": True
    },
    {
        "name": "approve",
        "type": "function",
        "inputs": [
            {"name": "spender", "type": "address"},
            {"name": "amount", "type": "uint256"}
        ],
        "outputs": [{"name": "", "type": "bool"}]
    }
]


class EVMBridge(BaseBridge):
    """Bridge implementation for EVM-compatible chains"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_w3: Optional[Web3] = None
        self.target_w3: Optional[Web3] = None
        self.source_bridge: Optional[Contract] = None
        self.target_bridge: Optional[Contract] = None
        
    async def initialize(self) -> None:
        """Initialize Web3 connections and contracts"""
        self.logger.info(f"Initializing EVM bridge {self.source_chain}->{self.target_chain}")
        
        # Initialize Web3 connections
        self.source_w3 = Web3(Web3.HTTPProvider(self.source_rpc))
        self.target_w3 = Web3(Web3.HTTPProvider(self.target_rpc))
        
        # Wait for connections
        source_connected = await self._wait_for_connection(self.source_w3, "source")
        target_connected = await self._wait_for_connection(self.target_w3, "target")
        
        if not source_connected or not target_connected:
            raise Exception("Failed to connect to blockchain nodes")
        
        # Initialize contracts
        if self.source_bridge_contract:
            self.source_bridge = self.source_w3.eth.contract(
                address=Web3.toChecksumAddress(self.source_bridge_contract),
                abi=BRIDGE_CONTRACT_ABI
            )
        
        if self.target_bridge_contract:
            self.target_bridge = self.target_w3.eth.contract(
                address=Web3.toChecksumAddress(self.target_bridge_contract),
                abi=BRIDGE_CONTRACT_ABI
            )
    
    async def _wait_for_connection(self, w3: Web3, chain_name: str, timeout: int = 30) -> bool:
        """Wait for Web3 connection to be established"""
        start_time = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start_time < timeout:
            if w3.isConnected():
                self.logger.info(f"Connected to {chain_name} chain")
                return True
            await asyncio.sleep(1)
        self.logger.error(f"Failed to connect to {chain_name} chain")
        return False
    
    async def lock_tokens(
        self,
        transfer: BridgeTransfer,
        private_key: Optional[str] = None
    ) -> Tuple[str, Dict[str, Any]]:
        """Lock tokens on the source chain"""
        if not self.source_bridge:
            raise Exception("Source bridge contract not initialized")
        
        self.logger.info(f"Locking tokens for transfer {transfer.transfer_id}")
        
        # Check if contract is paused
        if await self.is_contract_paused(self.source_chain):
            raise Exception("Bridge contract is paused")
        
        # For ERC20 tokens, need to approve first
        if transfer.token_type == TokenType.ERC20 and transfer.token_address:
            await self._approve_tokens(
                transfer.token_address,
                transfer.amount,
                self.source_bridge_contract,
                private_key
            )
        
        # Prepare transaction
        lock_function = self.source_bridge.functions.lockTokens(
            Web3.toChecksumAddress(transfer.token_address) if transfer.token_address else "0x0000000000000000000000000000000000000000",
            int(transfer.amount),
            self.target_chain,
            Web3.toChecksumAddress(transfer.to_address)
        )
        
        # Estimate gas
        from_address = Web3.toChecksumAddress(transfer.from_address)
        gas_estimate = lock_function.estimateGas({'from': from_address})
        
        # Build transaction
        tx = lock_function.buildTransaction({
            'from': from_address,
            'gas': int(gas_estimate * 1.2),  # Add 20% buffer
            'gasPrice': self.source_w3.eth.gas_price,
            'nonce': self.source_w3.eth.get_transaction_count(from_address),
            'value': int(transfer.amount) if transfer.token_type == TokenType.NATIVE else 0
        })
        
        # Sign and send transaction
        if private_key:
            signed_tx = self.source_w3.eth.account.sign_transaction(tx, private_key)
            tx_hash = self.source_w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        else:
            # In production, would use key management service
            raise Exception("Private key required for signing")
        
        tx_hash_hex = tx_hash.hex()
        self.logger.info(f"Lock transaction sent: {tx_hash_hex}")
        
        return tx_hash_hex, {
            'gas_used': gas_estimate,
            'gas_price': str(tx['gasPrice']),
            'nonce': tx['nonce']
        }
    
    async def _approve_tokens(
        self,
        token_address: str,
        amount: str,
        spender: str,
        private_key: str
    ) -> str:
        """Approve ERC20 token spending"""
        token_contract = self.source_w3.eth.contract(
            address=Web3.toChecksumAddress(token_address),
            abi=ERC20_ABI
        )
        
        approve_function = token_contract.functions.approve(
            Web3.toChecksumAddress(spender),
            int(amount)
        )
        
        # Build and send approval transaction
        tx = approve_function.buildTransaction({
            'from': self.source_w3.eth.account.privateKeyToAccount(private_key).address,
            'gas': 100000,
            'gasPrice': self.source_w3.eth.gas_price,
            'nonce': self.source_w3.eth.get_transaction_count(
                self.source_w3.eth.account.privateKeyToAccount(private_key).address
            )
        })
        
        signed_tx = self.source_w3.eth.account.sign_transaction(tx, private_key)
        tx_hash = self.source_w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        
        # Wait for confirmation
        receipt = self.source_w3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt['status'] != 1:
            raise Exception("Token approval failed")
        
        return tx_hash.hex()
    
    async def mint_tokens(
        self,
        transfer: BridgeTransfer,
        attestations: List[BridgeAttestation],
        private_key: Optional[str] = None
    ) -> Tuple[str, Dict[str, Any]]:
        """Mint tokens on the target chain"""
        if not self.target_bridge:
            raise Exception("Target bridge contract not initialized")
        
        self.logger.info(f"Minting tokens for transfer {transfer.transfer_id}")
        
        # Verify we have enough attestations
        if len(attestations) < transfer.attestations_required:
            raise Exception(f"Insufficient attestations: {len(attestations)}/{transfer.attestations_required}")
        
        # Prepare signatures
        signatures = [bytes.fromhex(att.signature[2:] if att.signature.startswith('0x') else att.signature) 
                     for att in attestations]
        
        # Get wrapped token address for minting
        wrapped_token = await self.get_wrapped_token_address(
            transfer.token_address or "0x0000000000000000000000000000000000000000",
            self.source_chain,
            self.target_chain
        )
        
        if not wrapped_token:
            raise Exception("No wrapped token mapping found")
        
        # Prepare mint transaction
        mint_function = self.target_bridge.functions.mintTokens(
            Web3.toChecksumAddress(wrapped_token),
            int(transfer.amount),
            Web3.toChecksumAddress(transfer.to_address),
            self.source_chain,
            bytes.fromhex(transfer.transfer_id),
            signatures
        )
        
        # Estimate gas
        relayer_address = self.config.get('relayer_address')
        if not relayer_address:
            raise Exception("No relayer address configured")
        
        gas_estimate = mint_function.estimateGas({'from': Web3.toChecksumAddress(relayer_address)})
        
        # Build transaction
        tx = mint_function.buildTransaction({
            'from': Web3.toChecksumAddress(relayer_address),
            'gas': int(gas_estimate * 1.2),
            'gasPrice': self.target_w3.eth.gas_price,
            'nonce': self.target_w3.eth.get_transaction_count(Web3.toChecksumAddress(relayer_address))
        })
        
        # Sign and send
        if private_key:
            signed_tx = self.target_w3.eth.account.sign_transaction(tx, private_key)
            tx_hash = self.target_w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        else:
            raise Exception("Private key required for signing")
        
        tx_hash_hex = tx_hash.hex()
        self.logger.info(f"Mint transaction sent: {tx_hash_hex}")
        
        return tx_hash_hex, {
            'gas_used': gas_estimate,
            'gas_price': str(tx['gasPrice']),
            'wrapped_token': wrapped_token
        }
    
    async def verify_lock_transaction(
        self,
        transaction_hash: str,
        expected_transfer: BridgeTransfer
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Verify lock transaction on source chain"""
        try:
            receipt = self.source_w3.eth.get_transaction_receipt(transaction_hash)
            if receipt['status'] != 1:
                return False, {'error': 'Transaction failed'}
            
            # Get transaction details
            tx = self.source_w3.eth.get_transaction(transaction_hash)
            
            # Verify sender
            if tx['from'].lower() != expected_transfer.from_address.lower():
                return False, {'error': 'Sender mismatch'}
            
            # Verify amount for native tokens
            if expected_transfer.token_type == TokenType.NATIVE:
                if str(tx['value']) != expected_transfer.amount:
                    return False, {'error': 'Amount mismatch'}
            
            # Get block details
            block = self.source_w3.eth.get_block(receipt['blockNumber'])
            
            return True, {
                'block_number': receipt['blockNumber'],
                'block_hash': block['hash'].hex(),
                'confirmations': self.source_w3.eth.block_number - receipt['blockNumber'],
                'gas_used': receipt['gasUsed'],
                'timestamp': block['timestamp']
            }
            
        except Exception as e:
            self.logger.error(f"Error verifying lock transaction: {e}")
            return False, {'error': str(e)}
    
    async def verify_mint_transaction(
        self,
        transaction_hash: str,
        expected_transfer: BridgeTransfer
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Verify mint transaction on target chain"""
        try:
            receipt = self.target_w3.eth.get_transaction_receipt(transaction_hash)
            if receipt['status'] != 1:
                return False, {'error': 'Transaction failed'}
            
            # Could parse logs here to verify mint details
            # For now, just return basic info
            block = self.target_w3.eth.get_block(receipt['blockNumber'])
            
            return True, {
                'block_number': receipt['blockNumber'],
                'block_hash': block['hash'].hex(),
                'confirmations': self.target_w3.eth.block_number - receipt['blockNumber'],
                'gas_used': receipt['gasUsed'],
                'timestamp': block['timestamp']
            }
            
        except Exception as e:
            self.logger.error(f"Error verifying mint transaction: {e}")
            return False, {'error': str(e)}
    
    async def get_token_balance(
        self,
        chain: str,
        address: str,
        token_address: Optional[str] = None
    ) -> str:
        """Get token balance for an address"""
        w3 = self.source_w3 if chain == self.source_chain else self.target_w3
        
        if not token_address:  # Native token
            balance = w3.eth.get_balance(Web3.toChecksumAddress(address))
            return str(balance)
        else:  # ERC20 token
            token_contract = w3.eth.contract(
                address=Web3.toChecksumAddress(token_address),
                abi=ERC20_ABI
            )
            balance = token_contract.functions.balanceOf(
                Web3.toChecksumAddress(address)
            ).call()
            return str(balance)
    
    async def estimate_fees(
        self,
        transfer: BridgeTransfer
    ) -> Dict[str, str]:
        """Estimate fees for the transfer"""
        # Estimate lock fee on source chain
        lock_gas = 150000 if transfer.token_type == TokenType.ERC20 else 50000
        lock_gas_price = self.source_w3.eth.gas_price
        lock_fee = lock_gas * lock_gas_price
        
        # Estimate mint fee on target chain
        mint_gas = 200000
        mint_gas_price = self.target_w3.eth.gas_price
        mint_fee = mint_gas * mint_gas_price
        
        # Calculate bridge fee
        bridge_fee = self.calculate_bridge_fee(transfer.amount)
        
        return {
            'lock_fee': str(lock_fee),
            'mint_fee': str(mint_fee),
            'bridge_fee': bridge_fee,
            'total_fee': str(lock_fee + mint_fee + int(bridge_fee))
        }
    
    async def create_attestation(
        self,
        transfer: BridgeTransfer,
        lock_tx_hash: str,
        validator_key: str
    ) -> BridgeAttestation:
        """Create attestation for a lock transaction"""
        # Verify the lock transaction first
        is_valid, tx_details = await self.verify_lock_transaction(lock_tx_hash, transfer)
        if not is_valid:
            raise Exception(f"Invalid lock transaction: {tx_details.get('error')}")
        
        # Create message to sign
        message = {
            'transfer_id': transfer.transfer_id,
            'source_chain': self.source_chain,
            'target_chain': self.target_chain,
            'lock_tx_hash': lock_tx_hash,
            'block_number': tx_details['block_number'],
            'block_hash': tx_details['block_hash'],
            'amount': transfer.amount,
            'from': transfer.from_address,
            'to': transfer.to_address
        }
        
        # Create hash of message
        message_hash = Web3.keccak(text=json.dumps(message, sort_keys=True))
        
        # Sign message
        account = Account.from_key(validator_key)
        signed_message = account.sign_message(encode_defunct(message_hash))
        
        return BridgeAttestation(
            attestation_id=str(uuid.uuid4()),
            transfer_id=transfer.transfer_id,
            validator_address=account.address,
            signature=signed_message.signature.hex(),
            block_number=tx_details['block_number'],
            block_hash=tx_details['block_hash'],
            metadata=message
        )
    
    async def verify_attestation(
        self,
        attestation: BridgeAttestation,
        transfer: BridgeTransfer
    ) -> bool:
        """Verify attestation signature"""
        try:
            # Recreate the message that was signed
            message = {
                'transfer_id': transfer.transfer_id,
                'source_chain': self.source_chain,
                'target_chain': self.target_chain,
                'lock_tx_hash': transfer.lock_tx_hash,
                'block_number': attestation.block_number,
                'block_hash': attestation.block_hash,
                'amount': transfer.amount,
                'from': transfer.from_address,
                'to': transfer.to_address
            }
            
            message_hash = Web3.keccak(text=json.dumps(message, sort_keys=True))
            
            # Recover signer address
            recovered_address = Account.recover_message(
                encode_defunct(message_hash),
                signature=attestation.signature
            )
            
            # Verify it matches the attestation validator
            return recovered_address.lower() == attestation.validator_address.lower()
            
        except Exception as e:
            self.logger.error(f"Error verifying attestation: {e}")
            return False
    
    async def is_contract_paused(self, chain: str) -> bool:
        """Check if bridge contract is paused"""
        try:
            contract = self.source_bridge if chain == self.source_chain else self.target_bridge
            if not contract:
                return False
            
            return contract.functions.isPaused().call()
        except Exception as e:
            self.logger.error(f"Error checking contract pause status: {e}")
            return False 