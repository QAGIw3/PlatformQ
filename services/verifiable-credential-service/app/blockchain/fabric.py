from hfc.fabric import Client as FabricClient
from hfc.fabric.transaction.tx_context import TXContext
from hfc.fabric.transaction.tx_proposal_request import TXProposalRequest
from typing import Dict, Any, Optional, List
from datetime import datetime
import json
import logging
from prometheus_client import Counter, Histogram, Gauge # Import Prometheus client
import time # Import time for metrics timing

from .base import BlockchainClient, ChainType, CredentialAnchor

logger = logging.getLogger(__name__)

# Prometheus Metrics for Fabric Client
FABRIC_TRANSACTIONS_TOTAL = Counter(
    'fabric_transactions_total', 
    'Total number of chaincode invocations on Fabric',
    ['chain_type', 'method', 'status']
)
FABRIC_TRANSACTION_DURATION_SECONDS = Histogram(
    'fabric_transaction_duration_seconds', 
    'Duration of Fabric chaincode invocations in seconds',
    ['chain_type', 'method']
)
FABRIC_BLOCK_HEIGHT = Gauge(
    'fabric_block_height', 
    'Current block height of the Fabric network',
    ['chain_type']
)
FABRIC_CHAINCODE_DEPLOYMENTS_TOTAL = Counter(
    'fabric_chaincode_deployments_total', 
    'Total chaincodes deployed on Fabric',
    ['chain_type', 'chaincode_name']
)
FABRIC_DAO_PROPOSAL_EXECUTIONS_TOTAL = Counter(
    'fabric_dao_proposal_executions_total', 
    'Total DAO proposal executions on Fabric',
    ['chain_type', 'dao_id']
)
FABRIC_DAO_VOTES_CAST_TOTAL = Counter(
    'fabric_dao_votes_cast_total', 
    'Total DAO votes cast on Fabric',
    ['chain_type', 'dao_id']
)


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
                # For Fabric, getting block height might be more complex, or rely on a separate monitor.
                # Placeholder for now:
                # FABRIC_BLOCK_HEIGHT.labels(chain_type=self.chain_type.value).set(current_block)
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
        method_name = "anchor_credential"
        start_time = time.time()
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
            
            # Metrics
            FABRIC_TRANSACTIONS_TOTAL.labels(chain_type=self.chain_type.value, method=method_name, status='success').inc()
            FABRIC_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name).observe(time.time() - start_time)

            return anchor
            
        except Exception as e:
            logger.error(f"Error anchoring credential: {e}")
            FABRIC_TRANSACTIONS_TOTAL.labels(chain_type=self.chain_type.value, method=method_name, status='failure').inc()
            FABRIC_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name).observe(time.time() - start_time)
            raise
    
    async def verify_credential_anchor(self, credential_id: str) -> Optional[CredentialAnchor]:
        """Verify credential exists on Fabric"""
        method_name = "verify_credential_anchor"
        start_time = time.time()
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
                # Metrics
                FABRIC_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name).observe(time.time() - start_time)

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
            FABRIC_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name).observe(time.time() - start_time)
            return None
    
    async def get_credential_history(self, credential_id: str) -> List[CredentialAnchor]:
        """Get credential history from Fabric"""
        method_name = "get_credential_history"
        start_time = time.time()
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
            
            # Metrics
            FABRIC_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name).observe(time.time() - start_time)

            return history
            
        except Exception as e:
            logger.error(f"Error getting credential history: {e}")
            FABRIC_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name).observe(time.time() - start_time)
            return []
    
    async def revoke_credential(self, credential_id: str, reason: str) -> str:
        """Revoke credential on Fabric"""
        method_name = "revoke_credential"
        start_time = time.time()
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
            
            # Metrics
            FABRIC_TRANSACTIONS_TOTAL.labels(chain_type=self.chain_type.value, method=method_name, status='success').inc()
            FABRIC_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name).observe(time.time() - start_time)

            return tx_id
            
        except Exception as e:
            logger.error(f"Error revoking credential: {e}")
            FABRIC_TRANSACTIONS_TOTAL.labels(chain_type=self.chain_type.value, method=method_name, status='failure').inc()
            FABRIC_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name).observe(time.time() - start_time)
            raise
    
    async def deploy_smart_contract(self, contract_name: str, 
                                  contract_code: str) -> str:
        """Deploy chaincode to Fabric (requires admin privileges)"""
        method_name = "deploy_smart_contract"
        start_time = time.time()
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
            
            # Metrics
            FABRIC_CHAINCODE_DEPLOYMENTS_TOTAL.labels(chain_type=self.chain_type.value, chaincode_name=contract_name).inc()
            FABRIC_TRANSACTIONS_TOTAL.labels(chain_type=self.chain_type.value, method=method_name, status='success').inc()
            FABRIC_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name).observe(time.time() - start_time)

            return contract_name
            
        except Exception as e:
            logger.error(f"Error deploying chaincode: {e}")
            FABRIC_TRANSACTIONS_TOTAL.labels(chain_type=self.chain_type.value, method=method_name, status='failure').inc()
            FABRIC_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name).observe(time.time() - start_time)
            raise
    
    async def call_smart_contract(self, contract_address: str, 
                                method: str, params: List[Any]) -> Any:
        """Call chaincode method"""
        method_name_full = f"call_chaincode_{method}" # Differentiate calls by method
        start_time = time.time()
        try:
            # In Fabric, contract_address is the chaincode name
            # Determine if it's a query (read) or invoke (write) operation
            # This is a simplification; a more robust solution would rely on chaincode design
            is_write_operation = False
            if any(kw in method.lower() for kw in ['invoke', 'set', 'create', 'put', 'delete', 'execute', 'vote']):
                is_write_operation = True

            if is_write_operation:
                logger.info(f"Invoking chaincode method '{method}' on chaincode {contract_address}")
                response = await self.fabric_client.chaincode_invoke(
                    requestor=self.fabric_client.get_user(self.org_name, self.user_name),
                    channel_name=self.channel_name,
                    peers=self.fabric_client.get_peers(),
                    fcn=method,
                    args=params,
                    cc_name=contract_address,
                    wait_for_event=True
                )
                result = json.loads(response['payload'].decode('utf-8')) if response.get('payload') else None # Return payload for invoke ops

                # DAO specific metrics for known methods
                if method.lower() == "executeproposal" and len(params) > 0: # Assuming first param is proposalId or DAO ID
                    FABRIC_DAO_PROPOSAL_EXECUTIONS_TOTAL.labels(chain_type=self.chain_type.value, dao_id=params[0]).inc() # Assuming params[0] is dao_id or proposalId
                    logger.info(f"Incremented Fabric DAO proposal execution metric for DAO: {params[0]}")
                elif method.lower() == "castvote" and len(params) > 0: # Assuming first param is proposalId or DAO ID
                    FABRIC_DAO_VOTES_CAST_TOTAL.labels(chain_type=self.chain_type.value, dao_id=params[0]).inc() # Assuming params[0] is dao_id or proposalId
                    logger.info(f"Incremented Fabric DAO vote cast metric for DAO: {params[0]}")

                # General transaction metrics for invoke ops
                FABRIC_TRANSACTIONS_TOTAL.labels(chain_type=self.chain_type.value, method=method_name_full, status='success').inc()
                FABRIC_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name_full).observe(time.time() - start_time)
            else:
                logger.info(f"Querying chaincode method '{method}' on chaincode {contract_address}")
                response = await self.fabric_client.chaincode_query(
                    requestor=self.fabric_client.get_user(self.org_name, self.user_name),
                    channel_name=self.channel_name,
                    peers=self.fabric_client.get_peers(),
                    fcn=method,
                    args=params,
                    cc_name=contract_address
                )
                result = json.loads(response) if response else None

                # Metrics for query ops
                FABRIC_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name_full).observe(time.time() - start_time)
            
            return result
            
        except Exception as e:
            logger.error(f"Error calling chaincode: {e}")
            if is_write_operation:
                FABRIC_TRANSACTIONS_TOTAL.labels(chain_type=self.chain_type.value, method=method_name_full, status='failure').inc()
            FABRIC_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name_full).observe(time.time() - start_time)
            raise 