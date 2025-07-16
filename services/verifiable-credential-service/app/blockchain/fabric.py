from hfc.fabric import Client as FabricClient
from hfc.fabric.transaction.tx_context import TXContext
from hfc.fabric.transaction.tx_proposal_request import TXProposalRequest
from typing import Dict, Any, Optional, List
from datetime import datetime
import json
import logging

from .base import BlockchainClient, ChainType, CredentialAnchor

logger = logging.getLogger(__name__)

class FabricClient(BlockchainClient):
    """Hyperledger Fabric blockchain client for credential anchoring"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(ChainType.FABRIC, config)
        self.fabric_client = None
        self.channel_name = config.get("channel_name", "platformq-channel")
        self.chaincode_name = config.get("chaincode_name", "credential-cc")
        self.org_name = config.get("org_name", "Org1")
        self.user_name = config.get("user_name", "Admin")
        
    async def connect(self) -> bool:
        """Connect to Fabric network"""
        try:
            self.fabric_client = FabricClient()
            
            # Load network configuration
            net_profile = self.config.get("network_profile", "network.yaml")
            self.fabric_client.new_channel(self.channel_name)
            
            # Set user context
            await self.fabric_client.init_with_net_profile(net_profile)
            org = self.fabric_client.get_net_info()['organizations'][self.org_name]
            
            user = self.fabric_client.get_user(
                org_name=self.org_name,
                name=self.user_name
            )
            
            if user:
                logger.info(f"Connected to Fabric network as {self.user_name}@{self.org_name}")
                return True
            else:
                logger.error("Failed to get user context")
                return False
                
        except Exception as e:
            logger.error(f"Error connecting to Fabric: {e}")
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from Fabric network"""
        if self.fabric_client:
            # Fabric SDK doesn't have explicit disconnect
            self.fabric_client = None
        logger.info("Disconnected from Fabric network")
    
    async def anchor_credential(self, credential: Dict[str, Any], 
                              tenant_id: str) -> CredentialAnchor:
        """Anchor credential on Fabric blockchain"""
        try:
            credential_id = credential.get("id")
            credential_hash = self.hash_credential(credential)
            
            # Prepare chaincode invocation
            args = [
                credential_id,
                credential_hash,
                tenant_id,
                json.dumps(credential)  # Store full credential in private data
            ]
            
            # Invoke chaincode
            response = await self.fabric_client.chaincode_invoke(
                requestor=self.fabric_client.get_user(self.org_name, self.user_name),
                channel_name=self.channel_name,
                peers=self.fabric_client.get_peers(),
                fcn='anchorCredential',
                args=args,
                cc_name=self.chaincode_name,
                wait_for_event=True
            )
            
            # Parse response
            tx_id = response['tx_id']
            block_number = response['block_number']
            
            anchor = CredentialAnchor(
                credential_id=credential_id,
                credential_hash=credential_hash,
                transaction_hash=tx_id,
                block_number=block_number,
                timestamp=datetime.utcnow(),
                chain_type=self.chain_type
            )
            
            logger.info(f"Credential {credential_id} anchored at tx: {tx_id}")
            return anchor
            
        except Exception as e:
            logger.error(f"Error anchoring credential: {e}")
            raise
    
    async def verify_credential_anchor(self, credential_id: str) -> Optional[CredentialAnchor]:
        """Verify credential exists on Fabric"""
        try:
            # Query chaincode
            response = await self.fabric_client.chaincode_query(
                requestor=self.fabric_client.get_user(self.org_name, self.user_name),
                channel_name=self.channel_name,
                peers=self.fabric_client.get_peers(),
                fcn='getCredential',
                args=[credential_id],
                cc_name=self.chaincode_name
            )
            
            if not response:
                return None
            
            # Parse response
            data = json.loads(response)
            if data.get('credential_hash'):
                return CredentialAnchor(
                    credential_id=credential_id,
                    credential_hash=data['credential_hash'],
                    transaction_hash=data['tx_id'],
                    block_number=data['block_number'],
                    timestamp=datetime.fromisoformat(data['timestamp']),
                    chain_type=self.chain_type
                )
            
            return None
            
        except Exception as e:
            logger.error(f"Error verifying credential: {e}")
            return None
    
    async def get_credential_history(self, credential_id: str) -> List[CredentialAnchor]:
        """Get credential history from Fabric"""
        try:
            # Query history
            response = await self.fabric_client.chaincode_query(
                requestor=self.fabric_client.get_user(self.org_name, self.user_name),
                channel_name=self.channel_name,
                peers=self.fabric_client.get_peers(),
                fcn='getCredentialHistory',
                args=[credential_id],
                cc_name=self.chaincode_name
            )
            
            history = []
            if response:
                data = json.loads(response)
                for item in data:
                    anchor = CredentialAnchor(
                        credential_id=credential_id,
                        credential_hash=item['credential_hash'],
                        transaction_hash=item['tx_id'],
                        block_number=item['block_number'],
                        timestamp=datetime.fromisoformat(item['timestamp']),
                        chain_type=self.chain_type
                    )
                    history.append(anchor)
            
            return history
            
        except Exception as e:
            logger.error(f"Error getting credential history: {e}")
            return []
    
    async def revoke_credential(self, credential_id: str, reason: str) -> str:
        """Revoke credential on Fabric"""
        try:
            # Invoke revocation
            response = await self.fabric_client.chaincode_invoke(
                requestor=self.fabric_client.get_user(self.org_name, self.user_name),
                channel_name=self.channel_name,
                peers=self.fabric_client.get_peers(),
                fcn='revokeCredential',
                args=[credential_id, reason],
                cc_name=self.chaincode_name,
                wait_for_event=True
            )
            
            tx_id = response['tx_id']
            logger.info(f"Credential {credential_id} revoked at tx: {tx_id}")
            
            return tx_id
            
        except Exception as e:
            logger.error(f"Error revoking credential: {e}")
            raise
    
    async def deploy_smart_contract(self, contract_name: str, 
                                  contract_code: str) -> str:
        """Deploy chaincode to Fabric (requires admin privileges)"""
        try:
            # Package chaincode
            cc_package = self.fabric_client.package_chaincode(
                cc_path=contract_code,  # Path to chaincode
                cc_type='golang',
                cc_name=contract_name
            )
            
            # Install chaincode
            responses = await self.fabric_client.install_chaincode(
                requestor=self.fabric_client.get_user(self.org_name, self.user_name),
                peers=self.fabric_client.get_peers(),
                cc_package=cc_package
            )
            
            # Instantiate chaincode
            response = await self.fabric_client.instantiate_chaincode(
                requestor=self.fabric_client.get_user(self.org_name, self.user_name),
                channel_name=self.channel_name,
                peers=self.fabric_client.get_peers(),
                cc_name=contract_name,
                cc_version='1.0',
                fcn='init',
                args=[],
                wait_for_event=True
            )
            
            logger.info(f"Chaincode {contract_name} deployed successfully")
            return contract_name
            
        except Exception as e:
            logger.error(f"Error deploying chaincode: {e}")
            raise
    
    async def call_smart_contract(self, contract_address: str, 
                                method: str, params: List[Any]) -> Any:
        """Call chaincode method"""
        try:
            # In Fabric, contract_address is the chaincode name
            response = await self.fabric_client.chaincode_query(
                requestor=self.fabric_client.get_user(self.org_name, self.user_name),
                channel_name=self.channel_name,
                peers=self.fabric_client.get_peers(),
                fcn=method,
                args=params,
                cc_name=contract_address
            )
            
            return json.loads(response) if response else None
            
        except Exception as e:
            logger.error(f"Error calling chaincode: {e}")
            raise 