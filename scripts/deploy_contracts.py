#!/usr/bin/env python3
"""
Deploy marketplace smart contracts to testnet (Polygon Mumbai)
"""

import json
import os
import sys
from web3 import Web3
from eth_account import Account
from solcx import compile_source, install_solc
import time

# Install specific Solidity version
install_solc('0.8.19')

# Configuration
POLYGON_MUMBAI_RPC = "https://rpc-mumbai.maticvigil.com"
PRIVATE_KEY = os.getenv("DEPLOYER_PRIVATE_KEY")
CONTRACTS_PATH = "../services/verifiable-credential-service/app/contracts/marketplace/"

if not PRIVATE_KEY:
    print("Error: DEPLOYER_PRIVATE_KEY environment variable not set")
    sys.exit(1)

# Connect to Polygon Mumbai
w3 = Web3(Web3.HTTPProvider(POLYGON_MUMBAI_RPC))
account = Account.from_key(PRIVATE_KEY)
w3.eth.default_account = account.address

print(f"Deploying from account: {account.address}")
print(f"Account balance: {w3.eth.get_balance(account.address) / 1e18} MATIC")

def compile_contract(contract_path):
    """Compile Solidity contract and return ABI and bytecode"""
    with open(contract_path, 'r') as file:
        contract_source = file.read()
    
    compiled = compile_source(contract_source)
    contract_id = list(compiled.keys())[0]
    
    return {
        'abi': compiled[contract_id]['abi'],
        'bytecode': compiled[contract_id]['bin']
    }

def deploy_contract(contract_name, *args):
    """Deploy a contract and return its address"""
    print(f"\nDeploying {contract_name}...")
    
    # Compile contract
    contract_path = os.path.join(CONTRACTS_PATH, f"{contract_name}.sol")
    compiled = compile_contract(contract_path)
    
    # Create contract instance
    Contract = w3.eth.contract(abi=compiled['abi'], bytecode=compiled['bytecode'])
    
    # Build transaction
    constructor = Contract.constructor(*args)
    tx = constructor.build_transaction({
        'from': account.address,
        'nonce': w3.eth.get_transaction_count(account.address),
        'gas': 3000000,
        'gasPrice': w3.to_wei('30', 'gwei')
    })
    
    # Sign and send transaction
    signed_tx = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
    
    # Wait for receipt
    print(f"Transaction hash: {tx_hash.hex()}")
    print("Waiting for confirmation...")
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
    
    print(f"{contract_name} deployed at: {receipt.contractAddress}")
    
    # Save ABI
    with open(f"{contract_name}_abi.json", 'w') as f:
        json.dump(compiled['abi'], f, indent=2)
    
    return receipt.contractAddress, compiled['abi']

def main():
    """Deploy all marketplace contracts"""
    print("Starting marketplace contracts deployment to Polygon Mumbai...")
    
    # Deploy PlatformAsset (NFT contract)
    platform_asset_address, platform_asset_abi = deploy_contract("PlatformAsset")
    
    # Deploy RoyaltyDistributor
    royalty_distributor_address, royalty_distributor_abi = deploy_contract(
        "RoyaltyDistributor",
        platform_asset_address
    )
    
    # Deploy UsageLicense
    # Assuming we need to deploy CredentialRegistry first
    # For now, use a dummy address
    credential_registry_address = "0x0000000000000000000000000000000000000000"
    fee_recipient = account.address  # Use deployer as fee recipient for testing
    
    usage_license_address, usage_license_abi = deploy_contract(
        "UsageLicense",
        credential_registry_address,
        fee_recipient,
        platform_asset_address
    )
    
    # Save deployment info
    deployment_info = {
        "network": "polygon-mumbai",
        "chainId": 80001,
        "deployer": account.address,
        "timestamp": int(time.time()),
        "contracts": {
            "PlatformAsset": {
                "address": platform_asset_address,
                "blockNumber": w3.eth.block_number
            },
            "RoyaltyDistributor": {
                "address": royalty_distributor_address,
                "blockNumber": w3.eth.block_number
            },
            "UsageLicense": {
                "address": usage_license_address,
                "blockNumber": w3.eth.block_number
            }
        }
    }
    
    with open("deployment_polygon_mumbai.json", 'w') as f:
        json.dump(deployment_info, f, indent=2)
    
    print("\n✅ Deployment complete!")
    print(f"\nPlatformAsset: {platform_asset_address}")
    print(f"RoyaltyDistributor: {royalty_distributor_address}")
    print(f"UsageLicense: {usage_license_address}")
    
    print("\nDeployment info saved to deployment_polygon_mumbai.json")
    print("Contract ABIs saved to *_abi.json files")
    
    # Verify on Polygonscan (optional)
    print("\nTo verify contracts on Polygonscan:")
    print(f"1. PlatformAsset: https://mumbai.polygonscan.com/address/{platform_asset_address}")
    print(f"2. RoyaltyDistributor: https://mumbai.polygonscan.com/address/{royalty_distributor_address}")
    print(f"3. UsageLicense: https://mumbai.polygonscan.com/address/{usage_license_address}")

if __name__ == "__main__":
    main() 