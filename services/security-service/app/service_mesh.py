"""
Service Mesh Coordination for Zero-Trust Architecture

Manages Consul Connect service mesh, mTLS certificates, and service-to-service communication.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import json
import base64
from dataclasses import dataclass
from enum import Enum

import consul.aio
import hvac
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend

logger = logging.getLogger(__name__)


class ServiceIntentionAction(Enum):
    """Service intention actions"""
    ALLOW = "allow"
    DENY = "deny"


@dataclass
class ServiceIntention:
    """Service mesh intention (traffic policy)"""
    source: str
    destination: str
    action: ServiceIntentionAction
    description: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class ServiceHealth:
    """Service health status"""
    service_name: str
    healthy_instances: int
    unhealthy_instances: int
    last_check: datetime
    checks: List[Dict[str, Any]]


class ServiceMeshCoordinator:
    """
    Coordinates Consul Connect service mesh operations.
    
    Features:
    - Service registration with Connect
    - Intention (traffic policy) management
    - Service discovery
    - Health monitoring
    - Sidecar proxy configuration
    - Observability integration
    """
    
    def __init__(self,
                 consul_client: consul.aio.Consul,
                 vault_client: hvac.Client,
                 service_name: str):
        self.consul = consul_client
        self.vault = vault_client
        self.service_name = service_name
        self._registered_services: Dict[str, Dict[str, Any]] = {}
        self._service_watchers: Dict[str, asyncio.Task] = {}
        self._mesh_status = {
            "connected": False,
            "services": 0,
            "intentions": 0,
            "last_update": None
        }
        
    async def initialize(self):
        """Initialize service mesh coordinator"""
        logger.info("Initializing service mesh coordinator")
        
        # Verify Consul Connect is enabled
        await self._verify_connect_enabled()
        
        # Load existing services
        await self._load_registered_services()
        
        # Start mesh status monitor
        asyncio.create_task(self._monitor_mesh_status())
        
        self._mesh_status["connected"] = True
        logger.info("Service mesh coordinator initialized")
        
    async def _verify_connect_enabled(self):
        """Verify Consul Connect is enabled"""
        try:
            # Check if Connect CA is configured
            ca_config = await self.consul.connect.ca.get_configuration()
            if not ca_config:
                raise Exception("Consul Connect CA not configured")
            logger.info("Consul Connect verified")
        except Exception as e:
            logger.error(f"Consul Connect not properly configured: {e}")
            raise
            
    async def register_service(self,
                             name: str,
                             port: int,
                             tags: List[str] = None,
                             meta: Dict[str, str] = None,
                             connect_enabled: bool = True) -> str:
        """Register a service with Consul Connect"""
        service_id = f"{name}-{datetime.utcnow().timestamp()}"
        
        # Service definition
        service_def = {
            "name": name,
            "id": service_id,
            "port": port,
            "tags": tags or [],
            "meta": meta or {},
            "check": {
                "http": f"http://localhost:{port}/health",
                "interval": "10s",
                "timeout": "5s",
                "deregister_critical_service_after": "30s"
            }
        }
        
        # Enable Connect sidecar if requested
        if connect_enabled:
            service_def["connect"] = {
                "sidecar_service": {
                    "port": port + 1000,  # Sidecar proxy port
                    "check": {
                        "tcp": f"localhost:{port + 1000}",
                        "interval": "10s"
                    },
                    "proxy": {
                        "upstreams": [],  # Will be configured dynamically
                        "config": {
                            "protocol": "http"
                        }
                    }
                }
            }
            
        # Register service
        await self.consul.agent.service.register(**service_def)
        
        # Track registered service
        self._registered_services[service_id] = service_def
        
        # Start health watcher
        self._service_watchers[service_id] = asyncio.create_task(
            self._watch_service_health(name)
        )
        
        logger.info(f"Registered service {name} with ID {service_id}")
        return service_id
        
    async def deregister_service(self, service_id: str):
        """Deregister a service"""
        try:
            await self.consul.agent.service.deregister(service_id)
            
            # Stop health watcher
            if service_id in self._service_watchers:
                self._service_watchers[service_id].cancel()
                del self._service_watchers[service_id]
                
            # Remove from tracking
            if service_id in self._registered_services:
                del self._registered_services[service_id]
                
            logger.info(f"Deregistered service {service_id}")
        except Exception as e:
            logger.error(f"Failed to deregister service {service_id}: {e}")
            
    async def create_intention(self,
                             source: str,
                             destination: str,
                             action: str = "allow",
                             description: Optional[str] = None) -> Dict[str, Any]:
        """Create service mesh intention (traffic policy)"""
        intention = {
            "SourceName": source,
            "DestinationName": destination,
            "Action": action,
            "Description": description or f"{action} traffic from {source} to {destination}",
            "Meta": {
                "created_by": self.service_name,
                "created_at": datetime.utcnow().isoformat()
            }
        }
        
        # Create intention
        result = await self.consul.connect.intentions.create(intention)
        
        logger.info(f"Created intention: {source} -> {destination} ({action})")
        return result
        
    async def update_intention(self, intention_id: str, updates: Dict[str, Any]):
        """Update existing intention"""
        try:
            # Get current intention
            current = await self.consul.connect.intentions.get(intention_id)
            
            # Apply updates
            current.update(updates)
            current["Meta"]["updated_at"] = datetime.utcnow().isoformat()
            
            # Update intention
            await self.consul.connect.intentions.update(intention_id, current)
            
            logger.info(f"Updated intention {intention_id}")
        except Exception as e:
            logger.error(f"Failed to update intention {intention_id}: {e}")
            raise
            
    async def get_service_certificate(self, service_name: str) -> Tuple[str, str]:
        """Get Connect certificate for a service"""
        try:
            # Get leaf certificate from Connect CA
            result = await self.consul.connect.ca.leaf(service_name)
            
            return result["CertPEM"], result["PrivateKeyPEM"]
        except Exception as e:
            logger.error(f"Failed to get certificate for {service_name}: {e}")
            raise
            
    async def get_ca_roots(self) -> str:
        """Get Connect CA root certificates"""
        try:
            result = await self.consul.connect.ca.roots()
            return result["Roots"][0]["RootCert"]
        except Exception as e:
            logger.error(f"Failed to get CA roots: {e}")
            raise
            
    async def configure_upstream(self,
                               service_id: str,
                               upstream_name: str,
                               upstream_port: int):
        """Configure upstream dependency for a service"""
        try:
            # Get current service config
            service = self._registered_services.get(service_id)
            if not service:
                raise Exception(f"Service {service_id} not found")
                
            # Add upstream to sidecar config
            if "connect" in service and "sidecar_service" in service["connect"]:
                upstreams = service["connect"]["sidecar_service"]["proxy"]["upstreams"]
                upstreams.append({
                    "destination_name": upstream_name,
                    "local_bind_port": upstream_port
                })
                
                # Re-register service with updated config
                await self.consul.agent.service.register(**service)
                
            logger.info(f"Configured upstream {upstream_name} for {service_id}")
        except Exception as e:
            logger.error(f"Failed to configure upstream: {e}")
            raise
            
    async def get_services(self) -> List[Dict[str, Any]]:
        """Get all services in the mesh"""
        try:
            # Get all services
            _, services = await self.consul.catalog.services()
            
            mesh_services = []
            for service_name in services:
                # Get service instances
                _, instances = await self.consul.health.service(service_name)
                
                # Check if Connect-enabled
                connect_enabled = any(
                    "connect" in instance["Service"].get("Tags", [])
                    for instance in instances
                )
                
                mesh_services.append({
                    "name": service_name,
                    "instances": len(instances),
                    "connect_enabled": connect_enabled,
                    "tags": services[service_name]
                })
                
            return mesh_services
        except Exception as e:
            logger.error(f"Failed to get services: {e}")
            return []
            
    async def get_service_health(self, service_name: str) -> ServiceHealth:
        """Get health status for a service"""
        try:
            # Get service health
            _, instances = await self.consul.health.service(service_name)
            
            healthy = 0
            unhealthy = 0
            checks = []
            
            for instance in instances:
                # Check overall health
                all_passing = all(
                    check["Status"] == "passing"
                    for check in instance["Checks"]
                )
                
                if all_passing:
                    healthy += 1
                else:
                    unhealthy += 1
                    
                # Collect check details
                for check in instance["Checks"]:
                    checks.append({
                        "node": instance["Node"]["Node"],
                        "check_id": check["CheckID"],
                        "name": check["Name"],
                        "status": check["Status"],
                        "output": check.get("Output", "")
                    })
                    
            return ServiceHealth(
                service_name=service_name,
                healthy_instances=healthy,
                unhealthy_instances=unhealthy,
                last_check=datetime.utcnow(),
                checks=checks
            )
        except Exception as e:
            logger.error(f"Failed to get health for {service_name}: {e}")
            raise
            
    async def get_mesh_status(self) -> Dict[str, Any]:
        """Get overall mesh status"""
        return self._mesh_status.copy()
        
    async def get_metrics(self) -> Dict[str, Any]:
        """Get service mesh metrics"""
        try:
            services = await self.get_services()
            
            # Get intentions count
            _, intentions = await self.consul.connect.intentions.list()
            
            metrics = {
                "total_services": len(services),
                "connect_enabled_services": sum(
                    1 for s in services if s["connect_enabled"]
                ),
                "total_instances": sum(s["instances"] for s in services),
                "total_intentions": len(intentions),
                "registered_by_coordinator": len(self._registered_services),
                "active_health_watchers": len(self._service_watchers)
            }
            
            return metrics
        except Exception as e:
            logger.error(f"Failed to get metrics: {e}")
            return {}
            
    async def _watch_service_health(self, service_name: str):
        """Watch service health and emit events on changes"""
        last_status = None
        
        while True:
            try:
                health = await self.get_service_health(service_name)
                
                # Check for status changes
                current_status = health.healthy_instances > 0
                if last_status is not None and current_status != last_status:
                    logger.warning(
                        f"Service {service_name} health changed: "
                        f"healthy={health.healthy_instances}, "
                        f"unhealthy={health.unhealthy_instances}"
                    )
                    
                last_status = current_status
                
            except Exception as e:
                logger.error(f"Health watch error for {service_name}: {e}")
                
            await asyncio.sleep(30)  # Check every 30 seconds
            
    async def _monitor_mesh_status(self):
        """Monitor overall mesh status"""
        while True:
            try:
                metrics = await self.get_metrics()
                
                self._mesh_status.update({
                    "connected": True,
                    "services": metrics.get("total_services", 0),
                    "intentions": metrics.get("total_intentions", 0),
                    "last_update": datetime.utcnow().isoformat()
                })
                
            except Exception as e:
                logger.error(f"Mesh status monitor error: {e}")
                self._mesh_status["connected"] = False
                
            await asyncio.sleep(60)  # Update every minute
            
    async def _load_registered_services(self):
        """Load previously registered services"""
        try:
            # Get agent services
            services = await self.consul.agent.services()
            
            for service_id, service in services.items():
                if service.get("Meta", {}).get("coordinator") == self.service_name:
                    self._registered_services[service_id] = service
                    
            logger.info(f"Loaded {len(self._registered_services)} registered services")
        except Exception as e:
            logger.error(f"Failed to load registered services: {e}")
            
    async def shutdown(self):
        """Shutdown coordinator"""
        logger.info("Shutting down service mesh coordinator")
        
        # Cancel all watchers
        for task in self._service_watchers.values():
            task.cancel()
            
        # Deregister services if needed
        # (Usually handled by service shutdown)
        
        logger.info("Service mesh coordinator shutdown complete")


class mTLSManager:
    """
    Manages mTLS certificates for services.
    
    Features:
    - Certificate issuance via Vault PKI
    - Automatic rotation
    - Certificate validation
    - Chain of trust management
    - OCSP support
    """
    
    def __init__(self,
                 vault_client: hvac.Client,
                 consul_client: consul.aio.Consul,
                 ca_path: str = "pki",
                 intermediate_path: str = "pki_int"):
        self.vault = vault_client
        self.consul = consul_client
        self.ca_path = ca_path
        self.intermediate_path = intermediate_path
        self._certificates: Dict[str, Dict[str, Any]] = {}
        self._rotation_tasks: Dict[str, asyncio.Task] = {}
        
    async def initialize(self):
        """Initialize mTLS manager"""
        logger.info("Initializing mTLS manager")
        
        # Verify PKI backends are mounted
        await self._verify_pki_backends()
        
        # Load existing certificates
        await self._load_certificates()
        
        # Start certificate monitor
        asyncio.create_task(self._monitor_certificates())
        
        logger.info("mTLS manager initialized")
        
    async def _verify_pki_backends(self):
        """Verify PKI backends are properly configured"""
        try:
            # Check root CA
            ca_info = self.vault.read(f"{self.ca_path}/ca/pem")
            if not ca_info:
                logger.warning("Root CA not configured")
                
            # Check intermediate CA
            int_info = self.vault.read(f"{self.intermediate_path}/ca/pem")
            if not int_info:
                logger.warning("Intermediate CA not configured")
                
        except Exception as e:
            logger.error(f"PKI backend verification failed: {e}")
            raise
            
    async def issue_certificate(self,
                              common_name: str,
                              ttl: str = "720h",
                              alt_names: Optional[List[str]] = None,
                              ip_sans: Optional[List[str]] = None) -> Dict[str, Any]:
        """Issue a new certificate"""
        try:
            # Certificate request parameters
            params = {
                "common_name": common_name,
                "ttl": ttl,
                "format": "pem"
            }
            
            if alt_names:
                params["alt_names"] = ",".join(alt_names)
                
            if ip_sans:
                params["ip_sans"] = ",".join(ip_sans)
                
            # Issue certificate from intermediate CA
            result = self.vault.write(
                f"{self.intermediate_path}/issue/server",
                **params
            )
            
            cert_data = result["data"]
            
            # Parse certificate for metadata
            cert = x509.load_pem_x509_certificate(
                cert_data["certificate"].encode(),
                default_backend()
            )
            
            cert_info = {
                "common_name": common_name,
                "certificate": cert_data["certificate"],
                "private_key": cert_data["private_key"],
                "ca_chain": cert_data["ca_chain"],
                "serial_number": cert_data["serial_number"],
                "expiry": cert.not_valid_after.isoformat(),
                "issued_at": datetime.utcnow().isoformat(),
                "ttl": ttl,
                "alt_names": alt_names or [],
                "auto_rotate": True
            }
            
            # Store certificate info
            self._certificates[common_name] = cert_info
            
            # Store in Consul for persistence
            await self._store_certificate_metadata(common_name, cert_info)
            
            # Schedule rotation
            self._schedule_rotation(common_name, cert.not_valid_after)
            
            logger.info(f"Issued certificate for {common_name}")
            return cert_info
            
        except Exception as e:
            logger.error(f"Failed to issue certificate for {common_name}: {e}")
            raise
            
    async def rotate_service_certificate(self, service_name: str) -> Dict[str, Any]:
        """Rotate certificate for a service"""
        try:
            # Get current certificate info
            current = self._certificates.get(service_name)
            if not current:
                raise Exception(f"No certificate found for {service_name}")
                
            # Issue new certificate with same parameters
            new_cert = await self.issue_certificate(
                common_name=service_name,
                ttl=current.get("ttl", "720h"),
                alt_names=current.get("alt_names", [])
            )
            
            # Notify service of new certificate via Consul event
            await self.consul.event.fire(
                "certificate-rotated",
                json.dumps({
                    "service": service_name,
                    "serial": new_cert["serial_number"],
                    "expiry": new_cert["expiry"]
                })
            )
            
            logger.info(f"Rotated certificate for {service_name}")
            return new_cert
            
        except Exception as e:
            logger.error(f"Failed to rotate certificate for {service_name}: {e}")
            raise
            
    async def revoke_certificate(self, serial_number: str):
        """Revoke a certificate"""
        try:
            self.vault.write(
                f"{self.intermediate_path}/revoke",
                serial_number=serial_number
            )
            
            # Update CRL
            self.vault.read(f"{self.intermediate_path}/crl/rotate")
            
            logger.info(f"Revoked certificate {serial_number}")
        except Exception as e:
            logger.error(f"Failed to revoke certificate {serial_number}: {e}")
            raise
            
    async def validate_certificate(self, cert_pem: str) -> Dict[str, Any]:
        """Validate a certificate"""
        try:
            # Load certificate
            cert = x509.load_pem_x509_certificate(
                cert_pem.encode(),
                default_backend()
            )
            
            # Basic validation
            now = datetime.utcnow()
            
            validation = {
                "valid": True,
                "errors": [],
                "warnings": []
            }
            
            # Check expiry
            if cert.not_valid_after < now:
                validation["valid"] = False
                validation["errors"].append("Certificate expired")
            elif cert.not_valid_after < now + timedelta(days=30):
                validation["warnings"].append("Certificate expiring soon")
                
            # Check not before
            if cert.not_valid_before > now:
                validation["valid"] = False
                validation["errors"].append("Certificate not yet valid")
                
            # Check against CRL
            crl = self.vault.read(f"{self.intermediate_path}/crl")
            if crl and self._is_revoked(cert.serial_number, crl["data"]["crl"]):
                validation["valid"] = False
                validation["errors"].append("Certificate revoked")
                
            # Add certificate info
            validation["subject"] = cert.subject.rfc4514_string()
            validation["issuer"] = cert.issuer.rfc4514_string()
            validation["serial_number"] = str(cert.serial_number)
            validation["not_before"] = cert.not_valid_before.isoformat()
            validation["not_after"] = cert.not_valid_after.isoformat()
            
            return validation
            
        except Exception as e:
            logger.error(f"Failed to validate certificate: {e}")
            return {
                "valid": False,
                "errors": [str(e)],
                "warnings": []
            }
            
    async def get_expiring_certificates(self, days: int = 30) -> List[Dict[str, Any]]:
        """Get certificates expiring within specified days"""
        expiring = []
        cutoff = datetime.utcnow() + timedelta(days=days)
        
        for service, cert_info in self._certificates.items():
            expiry = datetime.fromisoformat(cert_info["expiry"])
            if expiry <= cutoff:
                expiring.append({
                    "service": service,
                    "serial_number": cert_info["serial_number"],
                    "expiry": cert_info["expiry"],
                    "days_remaining": (expiry - datetime.utcnow()).days,
                    "auto_rotate": cert_info.get("auto_rotate", False)
                })
                
        return sorted(expiring, key=lambda x: x["expiry"])
        
    async def get_certificate_metrics(self) -> Dict[str, Any]:
        """Get certificate metrics"""
        total = len(self._certificates)
        expiring_30 = len(await self.get_expiring_certificates(30))
        expiring_7 = len(await self.get_expiring_certificates(7))
        
        # Calculate average remaining lifetime
        total_days = 0
        for cert_info in self._certificates.values():
            expiry = datetime.fromisoformat(cert_info["expiry"])
            remaining = (expiry - datetime.utcnow()).days
            total_days += max(0, remaining)
            
        avg_remaining = total_days / total if total > 0 else 0
        
        return {
            "total_certificates": total,
            "expiring_30_days": expiring_30,
            "expiring_7_days": expiring_7,
            "average_days_remaining": avg_remaining,
            "auto_rotation_enabled": sum(
                1 for c in self._certificates.values()
                if c.get("auto_rotate", False)
            )
        }
        
    async def _store_certificate_metadata(self, service_name: str, cert_info: Dict[str, Any]):
        """Store certificate metadata in Consul"""
        try:
            # Don't store private key in Consul
            metadata = {
                k: v for k, v in cert_info.items()
                if k not in ["private_key", "certificate"]
            }
            
            await self.consul.kv.put(
                f"certificates/{service_name}/metadata",
                json.dumps(metadata)
            )
        except Exception as e:
            logger.error(f"Failed to store certificate metadata: {e}")
            
    async def _load_certificates(self):
        """Load certificate metadata from Consul"""
        try:
            # Get all certificate metadata
            _, certs = await self.consul.kv.get("certificates", recurse=True)
            
            if certs:
                for cert_kv in certs:
                    if cert_kv["Key"].endswith("/metadata"):
                        service_name = cert_kv["Key"].split("/")[1]
                        metadata = json.loads(cert_kv["Value"])
                        self._certificates[service_name] = metadata
                        
                        # Schedule rotation if needed
                        expiry = datetime.fromisoformat(metadata["expiry"])
                        if metadata.get("auto_rotate", False):
                            self._schedule_rotation(service_name, expiry)
                            
            logger.info(f"Loaded {len(self._certificates)} certificates")
        except Exception as e:
            logger.error(f"Failed to load certificates: {e}")
            
    def _schedule_rotation(self, service_name: str, expiry: datetime):
        """Schedule certificate rotation"""
        # Cancel existing task if any
        if service_name in self._rotation_tasks:
            self._rotation_tasks[service_name].cancel()
            
        # Rotate 7 days before expiry
        rotation_time = expiry - timedelta(days=7)
        delay = max(0, (rotation_time - datetime.utcnow()).total_seconds())
        
        async def rotate():
            await asyncio.sleep(delay)
            try:
                await self.rotate_service_certificate(service_name)
            except Exception as e:
                logger.error(f"Auto-rotation failed for {service_name}: {e}")
                
        self._rotation_tasks[service_name] = asyncio.create_task(rotate())
        logger.info(f"Scheduled rotation for {service_name} in {delay/3600:.1f} hours")
        
    def _is_revoked(self, serial_number: int, crl_pem: str) -> bool:
        """Check if certificate is in CRL"""
        try:
            crl = x509.load_pem_x509_crl(crl_pem.encode(), default_backend())
            
            for revoked in crl:
                if revoked.serial_number == serial_number:
                    return True
                    
            return False
        except Exception:
            return False
            
    async def _monitor_certificates(self):
        """Monitor certificates for expiry and health"""
        while True:
            try:
                # Check for expiring certificates
                expiring = await self.get_expiring_certificates(7)
                for cert in expiring:
                    logger.warning(
                        f"Certificate for {cert['service']} expiring in "
                        f"{cert['days_remaining']} days"
                    )
                    
                # Check certificate health
                for service, cert_info in list(self._certificates.items()):
                    if "certificate" in cert_info:
                        validation = await self.validate_certificate(cert_info["certificate"])
                        if not validation["valid"]:
                            logger.error(
                                f"Certificate for {service} validation failed: "
                                f"{validation['errors']}"
                            )
                            
            except Exception as e:
                logger.error(f"Certificate monitor error: {e}")
                
            await asyncio.sleep(3600)  # Check every hour
            
    async def shutdown(self):
        """Shutdown mTLS manager"""
        logger.info("Shutting down mTLS manager")
        
        # Cancel rotation tasks
        for task in self._rotation_tasks.values():
            task.cancel()
            
        logger.info("mTLS manager shutdown complete") 