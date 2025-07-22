"""
Blockchain-Anchored Asset Lineage

Anchors asset lineage and transformations on blockchain for
immutable provenance tracking.
"""

import hashlib
import json
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import asyncio
from enum import Enum

from web3 import Web3
from web3.middleware import geth_poa_middleware
import ipfshttpclient
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class LineageEventType(Enum):
    """Types of lineage events"""
    CREATED = "created"
    DERIVED = "derived"
    TRANSFORMED = "transformed"
    VERIFIED = "verified"
    TRANSFERRED = "transferred"


class AssetLineageEvent(BaseModel):
    """Asset lineage event model"""
    event_id: str
    event_type: LineageEventType
    asset_id: str
    parent_asset_ids: List[str] = Field(default_factory=list)
    transformation_type: Optional[str] = None
    metadata_hash: str
    timestamp: datetime
    actor_id: str
    signature: Optional[str] = None


class BlockchainLineageAnchor:
    """Manages blockchain anchoring of asset lineage"""
    
    def __init__(self,
                 web3_provider_url: str,
                 contract_address: str,
                 contract_abi: List[Dict[str, Any]],
                 ipfs_api_url: str = "/dns/ipfs/tcp/5001"):
        
        # Initialize Web3
        self.w3 = Web3(Web3.HTTPProvider(web3_provider_url))
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        
        # Initialize contract
        self.contract = self.w3.eth.contract(
            address=Web3.toChecksumAddress(contract_address),
            abi=contract_abi
        )
        
        # Initialize IPFS client
        self.ipfs = ipfshttpclient.connect(ipfs_api_url)
        
        # Event cache
        self.event_cache = {}
        
    async def anchor_lineage_event(self,
                                 event: AssetLineageEvent,
                                 private_key: str) -> Dict[str, Any]:
        """Anchor a lineage event on blockchain"""
        try:
            # Prepare event data
            event_data = {
                "event_id": event.event_id,
                "event_type": event.event_type.value,
                "asset_id": event.asset_id,
                "parent_asset_ids": event.parent_asset_ids,
                "transformation_type": event.transformation_type,
                "metadata_hash": event.metadata_hash,
                "timestamp": event.timestamp.isoformat(),
                "actor_id": event.actor_id
            }
            
            # Store full event data in IPFS
            ipfs_hash = await self._store_in_ipfs(event_data)
            
            # Create blockchain transaction
            tx_hash = await self._anchor_on_blockchain(
                asset_id=event.asset_id,
                event_type=event.event_type.value,
                ipfs_hash=ipfs_hash,
                metadata_hash=event.metadata_hash,
                private_key=private_key
            )
            
            # Wait for confirmation
            receipt = await self._wait_for_confirmation(tx_hash)
            
            # Cache event
            self.event_cache[event.event_id] = {
                "event": event.dict(),
                "ipfs_hash": ipfs_hash,
                "tx_hash": tx_hash.hex(),
                "block_number": receipt["blockNumber"]
            }
            
            return {
                "success": True,
                "event_id": event.event_id,
                "ipfs_hash": ipfs_hash,
                "tx_hash": tx_hash.hex(),
                "block_number": receipt["blockNumber"],
                "gas_used": receipt["gasUsed"]
            }
            
        except Exception as e:
            logger.error(f"Error anchoring lineage event: {e}")
            return {
                "success": False,
                "error": str(e)
            }
            
    async def verify_lineage(self,
                           asset_id: str,
                           from_block: int = 0) -> Dict[str, Any]:
        """Verify complete lineage of an asset from blockchain"""
        try:
            # Get all events for asset
            events = await self._get_asset_events(asset_id, from_block)
            
            # Build lineage tree
            lineage_tree = await self._build_lineage_tree(events)
            
            # Verify integrity
            integrity_check = await self._verify_lineage_integrity(lineage_tree)
            
            return {
                "asset_id": asset_id,
                "lineage_tree": lineage_tree,
                "total_events": len(events),
                "integrity_valid": integrity_check["valid"],
                "integrity_details": integrity_check["details"],
                "verification_timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error verifying lineage: {e}")
            raise
            
    async def get_provenance_proof(self,
                                 asset_id: str,
                                 target_block: Optional[int] = None) -> Dict[str, Any]:
        """Generate cryptographic proof of asset provenance"""
        try:
            # Get lineage up to target block
            lineage = await self.verify_lineage(asset_id, 0)
            
            if not lineage["integrity_valid"]:
                raise ValueError("Lineage integrity check failed")
                
            # Generate Merkle proof
            merkle_proof = await self._generate_merkle_proof(
                lineage["lineage_tree"],
                target_block
            )
            
            # Create provenance certificate
            certificate = {
                "asset_id": asset_id,
                "lineage_root": merkle_proof["root"],
                "proof_path": merkle_proof["path"],
                "verified_events": len(lineage["lineage_tree"]),
                "timestamp": datetime.utcnow().isoformat(),
                "certificate_hash": ""
            }
            
            # Hash the certificate
            cert_bytes = json.dumps(certificate, sort_keys=True).encode()
            certificate["certificate_hash"] = hashlib.sha256(cert_bytes).hexdigest()
            
            return certificate
            
        except Exception as e:
            logger.error(f"Error generating provenance proof: {e}")
            raise
            
    async def _store_in_ipfs(self, data: Dict[str, Any]) -> str:
        """Store data in IPFS and return hash"""
        try:
            # Convert to JSON
            json_data = json.dumps(data, sort_keys=True, indent=2)
            
            # Add to IPFS
            result = self.ipfs.add_json(data)
            
            return result
            
        except Exception as e:
            logger.error(f"Error storing in IPFS: {e}")
            raise
            
    async def _anchor_on_blockchain(self,
                                  asset_id: str,
                                  event_type: str,
                                  ipfs_hash: str,
                                  metadata_hash: str,
                                  private_key: str) -> str:
        """Create blockchain transaction to anchor event"""
        try:
            # Get account from private key
            account = self.w3.eth.account.from_key(private_key)
            
            # Build transaction
            nonce = self.w3.eth.get_transaction_count(account.address)
            
            tx = self.contract.functions.anchorLineageEvent(
                asset_id,
                event_type,
                ipfs_hash,
                metadata_hash,
                int(datetime.utcnow().timestamp())
            ).buildTransaction({
                'chainId': self.w3.eth.chain_id,
                'gas': 200000,
                'gasPrice': self.w3.toWei('20', 'gwei'),
                'nonce': nonce
            })
            
            # Sign transaction
            signed_tx = self.w3.eth.account.sign_transaction(tx, private_key)
            
            # Send transaction
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            
            return tx_hash
            
        except Exception as e:
            logger.error(f"Error anchoring on blockchain: {e}")
            raise
            
    async def _wait_for_confirmation(self,
                                   tx_hash: str,
                                   timeout: int = 120) -> Dict[str, Any]:
        """Wait for transaction confirmation"""
        try:
            receipt = self.w3.eth.wait_for_transaction_receipt(
                tx_hash,
                timeout=timeout
            )
            
            return receipt
            
        except Exception as e:
            logger.error(f"Error waiting for confirmation: {e}")
            raise
            
    async def _get_asset_events(self,
                              asset_id: str,
                              from_block: int) -> List[Dict[str, Any]]:
        """Get all blockchain events for an asset"""
        try:
            # Create event filter
            event_filter = self.contract.events.LineageEventAnchored.createFilter(
                fromBlock=from_block,
                argument_filters={'assetId': asset_id}
            )
            
            # Get events
            events = event_filter.get_all_entries()
            
            # Process events
            processed_events = []
            for event in events:
                # Get IPFS data
                ipfs_data = self.ipfs.get_json(event['args']['ipfsHash'])
                
                processed_events.append({
                    "block_number": event['blockNumber'],
                    "tx_hash": event['transactionHash'].hex(),
                    "event_data": ipfs_data,
                    "metadata_hash": event['args']['metadataHash'],
                    "timestamp": event['args']['timestamp']
                })
                
            return processed_events
            
        except Exception as e:
            logger.error(f"Error getting asset events: {e}")
            raise
            
    async def _build_lineage_tree(self,
                                events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build lineage tree from events"""
        try:
            # Sort events by timestamp
            sorted_events = sorted(
                events,
                key=lambda x: x["event_data"]["timestamp"]
            )
            
            # Build tree structure
            tree = {
                "root": None,
                "nodes": {},
                "edges": []
            }
            
            for event in sorted_events:
                event_data = event["event_data"]
                asset_id = event_data["asset_id"]
                
                # Add node
                tree["nodes"][asset_id] = {
                    "event": event_data,
                    "block_number": event["block_number"],
                    "tx_hash": event["tx_hash"]
                }
                
                # Set root if first event
                if tree["root"] is None and event_data["event_type"] == "created":
                    tree["root"] = asset_id
                    
                # Add edges for parent relationships
                for parent_id in event_data.get("parent_asset_ids", []):
                    tree["edges"].append({
                        "from": parent_id,
                        "to": asset_id,
                        "type": event_data["event_type"]
                    })
                    
            return tree
            
        except Exception as e:
            logger.error(f"Error building lineage tree: {e}")
            raise
            
    async def _verify_lineage_integrity(self,
                                      lineage_tree: Dict[str, Any]) -> Dict[str, Any]:
        """Verify integrity of lineage tree"""
        try:
            issues = []
            
            # Check for orphaned nodes
            referenced_nodes = set()
            for edge in lineage_tree["edges"]:
                referenced_nodes.add(edge["from"])
                referenced_nodes.add(edge["to"])
                
            all_nodes = set(lineage_tree["nodes"].keys())
            orphaned = all_nodes - referenced_nodes - {lineage_tree["root"]}
            
            if orphaned:
                issues.append(f"Orphaned nodes found: {orphaned}")
                
            # Check for cycles
            if self._has_cycle(lineage_tree):
                issues.append("Cycle detected in lineage")
                
            # Verify metadata hashes
            for node_id, node_data in lineage_tree["nodes"].items():
                event = node_data["event"]
                expected_hash = self._calculate_metadata_hash(event)
                
                if event.get("metadata_hash") != expected_hash:
                    issues.append(f"Metadata hash mismatch for {node_id}")
                    
            return {
                "valid": len(issues) == 0,
                "details": issues
            }
            
        except Exception as e:
            logger.error(f"Error verifying lineage integrity: {e}")
            raise
            
    def _has_cycle(self, tree: Dict[str, Any]) -> bool:
        """Check if tree has cycles using DFS"""
        visited = set()
        rec_stack = set()
        
        def dfs(node):
            visited.add(node)
            rec_stack.add(node)
            
            # Get children
            children = [
                edge["to"] for edge in tree["edges"]
                if edge["from"] == node
            ]
            
            for child in children:
                if child not in visited:
                    if dfs(child):
                        return True
                elif child in rec_stack:
                    return True
                    
            rec_stack.remove(node)
            return False
            
        # Check from all nodes
        for node in tree["nodes"]:
            if node not in visited:
                if dfs(node):
                    return True
                    
        return False
        
    def _calculate_metadata_hash(self, event_data: Dict[str, Any]) -> str:
        """Calculate hash of event metadata"""
        # Remove variable fields
        metadata = {
            k: v for k, v in event_data.items()
            if k not in ["event_id", "signature", "metadata_hash"]
        }
        
        # Hash
        metadata_bytes = json.dumps(metadata, sort_keys=True).encode()
        return hashlib.sha256(metadata_bytes).hexdigest()
        
    async def _generate_merkle_proof(self,
                                   lineage_tree: Dict[str, Any],
                                   target_block: Optional[int]) -> Dict[str, Any]:
        """Generate Merkle proof for lineage"""
        try:
            # Get relevant events up to target block
            events = []
            for node_data in lineage_tree["nodes"].values():
                if target_block is None or node_data["block_number"] <= target_block:
                    events.append(node_data["tx_hash"])
                    
            # Build Merkle tree
            if not events:
                return {"root": "", "path": []}
                
            # Simple Merkle tree implementation
            def hash_pair(a: str, b: str) -> str:
                combined = "".join(sorted([a, b]))
                return hashlib.sha256(combined.encode()).hexdigest()
                
            # Build tree levels
            current_level = events[:]
            tree_levels = [current_level[:]]
            
            while len(current_level) > 1:
                next_level = []
                
                for i in range(0, len(current_level), 2):
                    if i + 1 < len(current_level):
                        next_level.append(
                            hash_pair(current_level[i], current_level[i + 1])
                        )
                    else:
                        next_level.append(current_level[i])
                        
                tree_levels.append(next_level[:])
                current_level = next_level
                
            # Root is the single element at the top
            root = current_level[0] if current_level else ""
            
            # Generate proof path for first event
            proof_path = []
            if events:
                index = 0
                for level in tree_levels[:-1]:
                    if index % 2 == 0 and index + 1 < len(level):
                        proof_path.append({
                            "hash": level[index + 1],
                            "position": "right"
                        })
                    elif index % 2 == 1:
                        proof_path.append({
                            "hash": level[index - 1],
                            "position": "left"
                        })
                    index //= 2
                    
            return {
                "root": root,
                "path": proof_path
            }
            
        except Exception as e:
            logger.error(f"Error generating Merkle proof: {e}")
            raise


class LineageSmartContract:
    """Smart contract interface for asset lineage"""
    
    CONTRACT_ABI = [
        {
            "inputs": [
                {"name": "assetId", "type": "string"},
                {"name": "eventType", "type": "string"},
                {"name": "ipfsHash", "type": "string"},
                {"name": "metadataHash", "type": "string"},
                {"name": "timestamp", "type": "uint256"}
            ],
            "name": "anchorLineageEvent",
            "outputs": [],
            "type": "function"
        },
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "name": "assetId", "type": "string"},
                {"indexed": False, "name": "eventType", "type": "string"},
                {"indexed": False, "name": "ipfsHash", "type": "string"},
                {"indexed": False, "name": "metadataHash", "type": "string"},
                {"indexed": False, "name": "timestamp", "type": "uint256"}
            ],
            "name": "LineageEventAnchored",
            "type": "event"
        }
    ] 