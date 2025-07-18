"""
Utility functions for blockchain operations.
"""

import hashlib
import re
from typing import Optional, Union
from decimal import Decimal
from eth_utils import to_checksum_address, is_address, to_wei, from_wei


def calculate_transaction_hash(tx_data: dict) -> str:
    """Calculate a hash for a transaction"""
    # Sort keys for consistent hashing
    sorted_data = json.dumps(tx_data, sort_keys=True)
    return hashlib.sha256(sorted_data.encode()).hexdigest()


def validate_address(address: str, chain_type: str = "ethereum") -> bool:
    """Validate blockchain address format"""
    if chain_type in ["ethereum", "polygon", "arbitrum", "optimism", "avalanche", "bsc"]:
        return is_address(address)
    elif chain_type == "solana":
        # Solana addresses are base58 encoded and 32-44 chars
        return bool(re.match(r'^[1-9A-HJ-NP-Za-km-z]{32,44}$', address))
    elif chain_type == "cosmos":
        # Cosmos addresses start with chain prefix and are bech32
        return bool(re.match(r'^[a-z]+1[a-z0-9]{38,}$', address))
    elif chain_type == "near":
        # NEAR addresses are account names
        return bool(re.match(r'^[a-z0-9][a-z0-9-_]*[a-z0-9]$', address))
    return False


def normalize_address(address: str, chain_type: str = "ethereum") -> str:
    """Normalize address to standard format"""
    if chain_type in ["ethereum", "polygon", "arbitrum", "optimism", "avalanche", "bsc"]:
        return to_checksum_address(address)
    # Other chains typically don't need normalization
    return address


def estimate_gas_price(base_fee: int, priority_fee: int, multiplier: float = 1.0) -> int:
    """Estimate gas price with EIP-1559"""
    return int((base_fee + priority_fee) * multiplier)


def format_wei(wei_value: Union[int, str], unit: str = "ether") -> Decimal:
    """Convert wei to larger unit"""
    return Decimal(str(from_wei(int(wei_value), unit)))


def to_wei_amount(amount: Union[Decimal, float, str], unit: str = "ether") -> int:
    """Convert amount to wei"""
    if isinstance(amount, Decimal):
        amount = str(amount)
    return to_wei(amount, unit)


def calculate_tx_cost(gas_used: int, gas_price: int) -> int:
    """Calculate total transaction cost in wei"""
    return gas_used * gas_price


def encode_function_data(function_name: str, params: list, abi: list) -> bytes:
    """Encode function call data (simplified version)"""
    # In production, use eth_abi or web3.py for proper encoding
    from eth_abi import encode_abi
    from eth_utils import function_signature_to_4byte_selector
    
    # Find function ABI
    func_abi = next((item for item in abi if item.get('name') == function_name), None)
    if not func_abi:
        raise ValueError(f"Function {function_name} not found in ABI")
    
    # Get function selector (first 4 bytes of keccak256 hash)
    selector = function_signature_to_4byte_selector(
        f"{function_name}({','.join(input['type'] for input in func_abi['inputs'])})"
    )
    
    # Encode parameters
    param_types = [input['type'] for input in func_abi['inputs']]
    encoded_params = encode_abi(param_types, params)
    
    return selector + encoded_params


def decode_event_data(event_data: dict, abi: list) -> dict:
    """Decode event data using ABI"""
    # Simplified version - in production use proper decoding
    return event_data


def is_contract_address(address: str, web3_instance) -> bool:
    """Check if address is a contract"""
    code = web3_instance.eth.get_code(address)
    return code != b''


def generate_deterministic_address(deployer: str, nonce: int) -> str:
    """Generate deterministic contract address"""
    from eth_utils import keccak
    from rlp import encode
    
    encoded = encode([bytes.fromhex(deployer[2:]), nonce])
    hash_result = keccak(encoded)
    return '0x' + hash_result[-20:].hex()


def validate_private_key(private_key: str) -> bool:
    """Validate private key format"""
    # Remove 0x prefix if present
    if private_key.startswith('0x'):
        private_key = private_key[2:]
    
    # Check if it's 64 hex characters
    return bool(re.match(r'^[0-9a-fA-F]{64}$', private_key))


def calculate_percentage_change(old_value: Decimal, new_value: Decimal) -> Decimal:
    """Calculate percentage change between two values"""
    if old_value == 0:
        return Decimal(0)
    return ((new_value - old_value) / old_value) * 100


# Import json for transaction hash calculation
import json 