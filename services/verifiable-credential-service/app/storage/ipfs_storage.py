"""
IPFS Storage Integration for Verifiable Credentials

This module provides decentralized storage capabilities for credentials using IPFS,
with encryption and access control.
"""

import json
import hashlib
import asyncio
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
import logging
import httpx
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64

logger = logging.getLogger(__name__)


class IPFSCredentialStorage:
    """
    Manages credential storage on IPFS with encryption and pinning strategies
    """
    
    def __init__(self, storage_proxy_url: str, encryption_key: Optional[str] = None):
        self.storage_proxy_url = storage_proxy_url
        self.client = httpx.AsyncClient()
        self.encryption_key = encryption_key or Fernet.generate_key()
        self.cipher_suite = Fernet(self.encryption_key)
        self.credential_index: Dict[str, str] = {}  # credential_id -> IPFS CID
        
    async def store_credential(
        self,
        credential: Dict[str, Any],
        tenant_id: str,
        encrypt: bool = True,
        pin: bool = True
    ) -> Dict[str, str]:
        """
        Store a credential on IPFS with optional encryption
        
        Args:
            credential: The verifiable credential to store
            tenant_id: Tenant ID for multi-tenancy
            encrypt: Whether to encrypt the credential before storage
            pin: Whether to pin the content on IPFS
            
        Returns:
            Dictionary containing CID and metadata
        """
        credential_id = credential.get("id", "")
        
        # Prepare credential data
        credential_data = json.dumps(credential, sort_keys=True)
        
        # Encrypt if requested
        if encrypt:
            encrypted_data = self.cipher_suite.encrypt(credential_data.encode())
            storage_data = {
                "encrypted": True,
                "data": base64.b64encode(encrypted_data).decode(),
                "algorithm": "Fernet",
                "tenant_id": tenant_id,
                "stored_at": datetime.utcnow().isoformat()
            }
        else:
            storage_data = {
                "encrypted": False,
                "data": credential_data,
                "tenant_id": tenant_id,
                "stored_at": datetime.utcnow().isoformat()
            }
            
        # Upload to IPFS via storage proxy
        files = {'file': ('credential.json', json.dumps(storage_data).encode())}
        response = await self.client.post(
            f"{self.storage_proxy_url}/upload",
            files=files
        )
        response.raise_for_status()
        
        result = response.json()
        cid = result["cid"]
        
        # Store in local index
        self.credential_index[credential_id] = cid
        
        # Create storage receipt
        storage_receipt = {
            "credential_id": credential_id,
            "cid": cid,
            "encrypted": encrypt,
            "pinned": pin,
            "stored_at": datetime.utcnow().isoformat(),
            "content_hash": hashlib.sha256(credential_data.encode()).hexdigest()
        }
        
        logger.info(f"Stored credential {credential_id} on IPFS: {cid}")
        return storage_receipt
        
    async def retrieve_credential(
        self,
        cid: str,
        tenant_id: str,
        decrypt: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a credential from IPFS
        
        Args:
            cid: IPFS Content Identifier
            tenant_id: Tenant ID for verification
            decrypt: Whether to decrypt the credential
            
        Returns:
            The credential data or None if not found
        """
        try:
            # Download from IPFS
            response = await self.client.get(
                f"{self.storage_proxy_url}/download/{cid}"
            )
            response.raise_for_status()
            
            # Parse storage data
            storage_data = json.loads(response.content)
            
            # Verify tenant
            if storage_data.get("tenant_id") != tenant_id:
                logger.warning(f"Tenant mismatch for CID {cid}")
                return None
                
            # Decrypt if needed
            if storage_data.get("encrypted") and decrypt:
                encrypted_data = base64.b64decode(storage_data["data"])
                decrypted_data = self.cipher_suite.decrypt(encrypted_data)
                credential = json.loads(decrypted_data)
            else:
                credential = json.loads(storage_data["data"]) if isinstance(storage_data["data"], str) else storage_data["data"]
                
            return credential
            
        except Exception as e:
            logger.error(f"Failed to retrieve credential from IPFS: {e}")
            return None
            
    async def create_credential_dag(
        self,
        credentials: List[Dict[str, Any]],
        tenant_id: str
    ) -> str:
        """
        Create a DAG (Directed Acyclic Graph) of related credentials
        
        Args:
            credentials: List of related credentials
            tenant_id: Tenant ID
            
        Returns:
            Root CID of the DAG
        """
        # Store individual credentials and collect CIDs
        credential_nodes = []
        
        for credential in credentials:
            receipt = await self.store_credential(credential, tenant_id)
            credential_nodes.append({
                "credential_id": credential.get("id"),
                "cid": receipt["cid"],
                "type": credential.get("type", [])[-1] if credential.get("type") else "Unknown"
            })
            
        # Create DAG structure
        dag = {
            "version": "1.0",
            "tenant_id": tenant_id,
            "created_at": datetime.utcnow().isoformat(),
            "credentials": credential_nodes,
            "relationships": self._extract_relationships(credentials)
        }
        
        # Store DAG on IPFS
        files = {'file': ('credential_dag.json', json.dumps(dag).encode())}
        response = await self.client.post(
            f"{self.storage_proxy_url}/upload",
            files=files
        )
        response.raise_for_status()
        
        return response.json()["cid"]
        
    def _extract_relationships(self, credentials: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        """Extract relationships between credentials"""
        relationships = []
        
        for i, cred1 in enumerate(credentials):
            for j, cred2 in enumerate(credentials):
                if i != j:
                    # Check for explicit relationships
                    if "relatedCredential" in cred1:
                        related = cred1["relatedCredential"]
                        if isinstance(related, list) and cred2["id"] in related:
                            relationships.append({
                                "from": cred1["id"],
                                "to": cred2["id"],
                                "type": "relatedTo"
                            })
                            
                    # Check for subject relationships
                    subject1 = cred1.get("credentialSubject", {})
                    subject2 = cred2.get("credentialSubject", {})
                    if subject1.get("id") == subject2.get("id"):
                        relationships.append({
                            "from": cred1["id"],
                            "to": cred2["id"],
                            "type": "sameSubject"
                        })
                        
        return relationships
        
    async def create_merkle_tree(
        self,
        credential_cids: List[str]
    ) -> Dict[str, Any]:
        """
        Create a Merkle tree of credential CIDs for efficient verification
        
        Args:
            credential_cids: List of IPFS CIDs
            
        Returns:
            Merkle tree structure with root hash
        """
        # Sort CIDs for consistent tree structure
        sorted_cids = sorted(credential_cids)
        
        # Build Merkle tree bottom-up
        current_level = [
            hashlib.sha256(cid.encode()).hexdigest()
            for cid in sorted_cids
        ]
        
        tree_levels = [current_level]
        
        while len(current_level) > 1:
            next_level = []
            
            # Hash pairs of nodes
            for i in range(0, len(current_level), 2):
                if i + 1 < len(current_level):
                    combined = current_level[i] + current_level[i + 1]
                else:
                    combined = current_level[i] + current_level[i]  # Duplicate last node if odd
                    
                next_level.append(hashlib.sha256(combined.encode()).hexdigest())
                
            tree_levels.append(next_level)
            current_level = next_level
            
        return {
            "root": current_level[0] if current_level else None,
            "levels": tree_levels,
            "leaf_cids": sorted_cids
        }
        
    async def generate_merkle_proof(
        self,
        cid: str,
        merkle_tree: Dict[str, Any]
    ) -> Optional[List[Dict[str, str]]]:
        """
        Generate a Merkle proof for a specific CID
        
        Args:
            cid: The CID to prove
            merkle_tree: The Merkle tree structure
            
        Returns:
            Merkle proof path or None if CID not in tree
        """
        if cid not in merkle_tree["leaf_cids"]:
            return None
            
        proof = []
        index = merkle_tree["leaf_cids"].index(cid)
        
        for level in merkle_tree["levels"][:-1]:  # Exclude root level
            # Determine sibling index
            if index % 2 == 0:
                sibling_index = index + 1
                position = "right"
            else:
                sibling_index = index - 1
                position = "left"
                
            # Add sibling to proof if it exists
            if sibling_index < len(level):
                proof.append({
                    "hash": level[sibling_index],
                    "position": position
                })
                
            # Move to parent index
            index = index // 2
            
        return proof
        
    async def verify_merkle_proof(
        self,
        cid: str,
        root_hash: str,
        proof: List[Dict[str, str]]
    ) -> bool:
        """
        Verify a Merkle proof for a CID
        
        Args:
            cid: The CID to verify
            root_hash: The expected root hash
            proof: The Merkle proof path
            
        Returns:
            True if proof is valid
        """
        # Start with hash of the CID
        current_hash = hashlib.sha256(cid.encode()).hexdigest()
        
        # Apply proof path
        for step in proof:
            sibling_hash = step["hash"]
            
            if step["position"] == "left":
                combined = sibling_hash + current_hash
            else:
                combined = current_hash + sibling_hash
                
            current_hash = hashlib.sha256(combined.encode()).hexdigest()
            
        return current_hash == root_hash
        
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()


class DistributedCredentialNetwork:
    """
    Manages a distributed network of credential storage nodes
    """
    
    def __init__(self, storage_nodes: List[str], replication_factor: int = 3):
        self.storage_nodes = storage_nodes
        self.replication_factor = min(replication_factor, len(storage_nodes))
        self.node_clients = [IPFSCredentialStorage(node) for node in storage_nodes]
        
    async def store_with_replication(
        self,
        credential: Dict[str, Any],
        tenant_id: str
    ) -> Dict[str, Any]:
        """
        Store credential with replication across multiple nodes
        
        Args:
            credential: The credential to store
            tenant_id: Tenant ID
            
        Returns:
            Storage receipt with replication details
        """
        # Select nodes for replication
        import random
        selected_nodes = random.sample(
            list(enumerate(self.node_clients)),
            self.replication_factor
        )
        
        # Store on multiple nodes in parallel
        tasks = []
        for node_idx, client in selected_nodes:
            task = client.store_credential(credential, tenant_id)
            tasks.append((node_idx, task))
            
        results = []
        for node_idx, task in tasks:
            try:
                receipt = await task
                results.append({
                    "node": self.storage_nodes[node_idx],
                    "cid": receipt["cid"],
                    "success": True
                })
            except Exception as e:
                logger.error(f"Failed to store on node {node_idx}: {e}")
                results.append({
                    "node": self.storage_nodes[node_idx],
                    "success": False,
                    "error": str(e)
                })
                
        # Verify we have minimum successful replications
        successful = [r for r in results if r["success"]]
        if len(successful) < 2:
            raise Exception("Insufficient successful replications")
            
        return {
            "credential_id": credential.get("id"),
            "replications": results,
            "primary_cid": successful[0]["cid"],
            "replication_factor": len(successful)
        }
        
    async def retrieve_with_fallback(
        self,
        cid: str,
        tenant_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve credential with automatic fallback to replica nodes
        
        Args:
            cid: IPFS CID
            tenant_id: Tenant ID
            
        Returns:
            Credential data or None
        """
        for client in self.node_clients:
            try:
                credential = await client.retrieve_credential(cid, tenant_id)
                if credential:
                    return credential
            except Exception as e:
                logger.warning(f"Failed to retrieve from node: {e}")
                continue
                
        return None 