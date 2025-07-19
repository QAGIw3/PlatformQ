"""
Vault and Consul Integration for Compliance Service

Manages secure configuration, credentials, and compliance certificates.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
import json
import base64
import hashlib

import hvac
import consul.aio
from cryptography import x509
from cryptography.x509.oid import NameOID, ExtensionOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.backends import default_backend

from platformq_shared.vault_consul_base import VaultConsulBase

logger = logging.getLogger(__name__)


class VaultConsulIntegration(VaultConsulBase):
    """
    Compliance service specific Vault and Consul integration.
    
    Features:
    - Audit log encryption and signing
    - Regulatory compliance certificates
    - Data protection keys (GDPR, HIPAA, PCI)
    - Compliance officer credentials
    - Secure configuration for compliance rules
    - Evidence storage encryption
    - Data retention policies
    - Cross-border data transfer keys
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(
            vault_addr=config["vault_addr"],
            vault_token=config.get("vault_token"),
            consul_addr=config["consul_addr"],
            service_name="compliance-service"
        )
        
        self.compliance_config = {}
        self._certificate_cache = {}
        self._key_cache = {}
        self._policy_watchers = {}
        
    async def initialize(self):
        """Initialize compliance-specific Vault and Consul features"""
        await super().initialize()
        
        logger.info("Initializing compliance Vault/Consul integration")
        
        # Enable compliance-specific secret engines
        await self._setup_compliance_secrets()
        
        # Load compliance configuration
        await self._load_compliance_config()
        
        # Setup compliance certificates
        await self._setup_compliance_certificates()
        
        # Setup policy watchers
        await self._setup_policy_watchers()
        
        logger.info("Compliance Vault/Consul integration initialized")
        
    async def _setup_compliance_secrets(self):
        """Setup compliance-specific secret engines"""
        try:
            # Enable Transit engine for encryption
            try:
                self.vault.sys.enable_secrets_engine(
                    backend_type="transit",
                    path="compliance-transit"
                )
            except Exception:
                pass  # Already enabled
                
            # Create compliance KV paths
            paths = [
                "secret/compliance/audit-logs",
                "secret/compliance/certificates",
                "secret/compliance/data-protection",
                "secret/compliance/officer-credentials",
                "secret/compliance/evidence",
                "secret/compliance/policies"
            ]
            
            for path in paths:
                try:
                    self.vault.write(f"{path}/config", initialized=True)
                except Exception:
                    pass  # Path might already exist
                    
            # Create encryption keys for different compliance frameworks
            frameworks = ["gdpr", "hipaa", "pci-dss", "sox", "iso27001"]
            for framework in frameworks:
                await self._create_framework_keys(framework)
                
        except Exception as e:
            logger.error(f"Failed to setup compliance secrets: {e}")
            
    async def _create_framework_keys(self, framework: str):
        """Create encryption keys for a compliance framework"""
        try:
            # Create audit log encryption key
            self.vault.write(
                f"compliance-transit/keys/audit-{framework}",
                type="aes256-gcm96",
                exportable=False,
                allow_plaintext_backup=False
            )
            
            # Create data protection key
            self.vault.write(
                f"compliance-transit/keys/data-{framework}",
                type="aes256-gcm96",
                exportable=True,  # For key escrow requirements
                allow_plaintext_backup=True
            )
            
            # Create signing key for evidence
            self.vault.write(
                f"compliance-transit/keys/sign-{framework}",
                type="rsa-4096",
                exportable=False,
                allow_plaintext_backup=False
            )
            
        except Exception:
            pass  # Keys might already exist
            
    async def encrypt_audit_log(self, 
                              log_data: Dict[str, Any],
                              framework: str = "gdpr") -> Dict[str, str]:
        """Encrypt audit log entry for compliance"""
        try:
            # Serialize log data
            log_json = json.dumps(log_data, sort_keys=True)
            log_bytes = log_json.encode()
            
            # Add integrity hash
            integrity_hash = hashlib.sha256(log_bytes).hexdigest()
            
            # Encrypt log data
            response = self.vault.write(
                f"compliance-transit/encrypt/audit-{framework}",
                plaintext=base64.b64encode(log_bytes).decode(),
                context=base64.b64encode(f"audit-{framework}".encode()).decode()
            )
            
            if response and "data" in response:
                return {
                    "ciphertext": response["data"]["ciphertext"],
                    "integrity_hash": integrity_hash,
                    "framework": framework,
                    "timestamp": datetime.utcnow().isoformat()
                }
                
            raise Exception("Encryption failed")
            
        except Exception as e:
            logger.error(f"Failed to encrypt audit log: {e}")
            raise
            
    async def decrypt_audit_log(self,
                              encrypted_data: Dict[str, str]) -> Dict[str, Any]:
        """Decrypt audit log entry"""
        try:
            framework = encrypted_data.get("framework", "gdpr")
            
            # Decrypt
            response = self.vault.write(
                f"compliance-transit/decrypt/audit-{framework}",
                ciphertext=encrypted_data["ciphertext"],
                context=base64.b64encode(f"audit-{framework}".encode()).decode()
            )
            
            if response and "data" in response:
                # Decode and verify
                log_bytes = base64.b64decode(response["data"]["plaintext"])
                
                # Verify integrity
                computed_hash = hashlib.sha256(log_bytes).hexdigest()
                if computed_hash != encrypted_data.get("integrity_hash"):
                    raise Exception("Integrity check failed")
                    
                return json.loads(log_bytes)
                
            raise Exception("Decryption failed")
            
        except Exception as e:
            logger.error(f"Failed to decrypt audit log: {e}")
            raise
            
    async def sign_evidence(self,
                          evidence_data: bytes,
                          framework: str = "gdpr") -> Dict[str, str]:
        """Sign evidence for compliance"""
        try:
            # Hash evidence
            evidence_hash = hashlib.sha256(evidence_data).hexdigest()
            
            # Sign hash
            response = self.vault.write(
                f"compliance-transit/sign/sign-{framework}",
                input=base64.b64encode(evidence_hash.encode()).decode(),
                hash_algorithm="sha2-256",
                signature_algorithm="pss"
            )
            
            if response and "data" in response:
                return {
                    "signature": response["data"]["signature"],
                    "hash": evidence_hash,
                    "framework": framework,
                    "algorithm": "RSA-PSS-SHA256",
                    "signed_at": datetime.utcnow().isoformat()
                }
                
            raise Exception("Signing failed")
            
        except Exception as e:
            logger.error(f"Failed to sign evidence: {e}")
            raise
            
    async def verify_evidence_signature(self,
                                      evidence_data: bytes,
                                      signature_data: Dict[str, str]) -> bool:
        """Verify evidence signature"""
        try:
            framework = signature_data.get("framework", "gdpr")
            
            # Hash evidence
            evidence_hash = hashlib.sha256(evidence_data).hexdigest()
            
            # Verify signature
            response = self.vault.write(
                f"compliance-transit/verify/sign-{framework}",
                input=base64.b64encode(evidence_hash.encode()).decode(),
                signature=signature_data["signature"],
                hash_algorithm="sha2-256",
                signature_algorithm="pss"
            )
            
            if response and "data" in response:
                return response["data"]["valid"]
                
            return False
            
        except Exception as e:
            logger.error(f"Failed to verify signature: {e}")
            return False
            
    async def get_compliance_certificate(self,
                                       framework: str,
                                       purpose: str = "audit") -> Dict[str, Any]:
        """Get compliance certificate for framework"""
        cache_key = f"{framework}_{purpose}"
        
        # Check cache
        if cache_key in self._certificate_cache:
            cached = self._certificate_cache[cache_key]
            if datetime.utcnow() < cached["expires"]:
                return cached["certificate"]
                
        try:
            # Get or generate certificate
            cert_path = f"secret/compliance/certificates/{framework}/{purpose}"
            response = self.vault.read(cert_path)
            
            if not response or "data" not in response:
                # Generate new certificate
                cert_data = await self._generate_compliance_certificate(framework, purpose)
                self.vault.write(cert_path, **cert_data)
            else:
                cert_data = response["data"]["data"]
                
            # Parse certificate to get expiry
            cert = x509.load_pem_x509_certificate(
                cert_data["certificate"].encode(),
                default_backend()
            )
            
            # Cache
            self._certificate_cache[cache_key] = {
                "certificate": cert_data,
                "expires": cert.not_valid_after
            }
            
            return cert_data
            
        except Exception as e:
            logger.error(f"Failed to get compliance certificate: {e}")
            raise
            
    async def encrypt_pii_data(self,
                             data: bytes,
                             data_subject_id: str,
                             purpose: str) -> Dict[str, str]:
        """Encrypt PII data with GDPR compliance"""
        try:
            # Create context with metadata
            context = {
                "data_subject_id": data_subject_id,
                "purpose": purpose,
                "encrypted_at": datetime.utcnow().isoformat()
            }
            
            # Encrypt
            response = self.vault.write(
                "compliance-transit/encrypt/data-gdpr",
                plaintext=base64.b64encode(data).decode(),
                context=base64.b64encode(json.dumps(context).encode()).decode()
            )
            
            if response and "data" in response:
                # Log encryption event for GDPR compliance
                await self._log_data_processing(
                    "encryption",
                    data_subject_id,
                    purpose
                )
                
                return {
                    "ciphertext": response["data"]["ciphertext"],
                    "data_subject_id": data_subject_id,
                    "purpose": purpose
                }
                
            raise Exception("PII encryption failed")
            
        except Exception as e:
            logger.error(f"Failed to encrypt PII: {e}")
            raise
            
    async def decrypt_pii_data(self,
                             encrypted_data: Dict[str, str],
                             purpose: str) -> bytes:
        """Decrypt PII data with audit logging"""
        try:
            # Verify purpose matches
            if purpose != encrypted_data.get("purpose"):
                raise Exception("Purpose mismatch - GDPR violation")
                
            # Create context
            context = {
                "data_subject_id": encrypted_data["data_subject_id"],
                "purpose": purpose,
                "encrypted_at": datetime.utcnow().isoformat()
            }
            
            # Decrypt
            response = self.vault.write(
                "compliance-transit/decrypt/data-gdpr",
                ciphertext=encrypted_data["ciphertext"],
                context=base64.b64encode(json.dumps(context).encode()).decode()
            )
            
            if response and "data" in response:
                # Log decryption event
                await self._log_data_processing(
                    "decryption",
                    encrypted_data["data_subject_id"],
                    purpose
                )
                
                return base64.b64decode(response["data"]["plaintext"])
                
            raise Exception("PII decryption failed")
            
        except Exception as e:
            logger.error(f"Failed to decrypt PII: {e}")
            raise
            
    async def get_data_retention_policy(self, 
                                      data_type: str,
                                      framework: str) -> Dict[str, Any]:
        """Get data retention policy for compliance framework"""
        try:
            # Get from Consul
            _, data = await self.consul.kv.get(
                f"compliance/retention-policies/{framework}/{data_type}"
            )
            
            if data and data["Value"]:
                return json.loads(data["Value"])
                
            # Default policies
            default_policies = {
                "gdpr": {
                    "audit_logs": {"days": 1095, "encrypted": True},  # 3 years
                    "pii_data": {"days": 0, "delete_on_request": True},
                    "consent_records": {"days": 2555, "encrypted": True}  # 7 years
                },
                "hipaa": {
                    "medical_records": {"days": 2190, "encrypted": True},  # 6 years
                    "audit_logs": {"days": 2190, "encrypted": True}
                },
                "pci-dss": {
                    "transaction_logs": {"days": 365, "encrypted": True},
                    "cardholder_data": {"days": 0, "tokenized": True}
                }
            }
            
            policy = default_policies.get(framework, {}).get(
                data_type,
                {"days": 365, "encrypted": True}
            )
            
            # Store default
            await self.consul.kv.put(
                f"compliance/retention-policies/{framework}/{data_type}",
                json.dumps(policy)
            )
            
            return policy
            
        except Exception as e:
            logger.error(f"Failed to get retention policy: {e}")
            return {"days": 365, "encrypted": True}
            
    async def create_data_export_package(self,
                                       data_subject_id: str,
                                       framework: str = "gdpr") -> Dict[str, Any]:
        """Create data export package for subject access requests"""
        try:
            # Generate export key
            export_key = await self._generate_export_key(data_subject_id)
            
            # Create secure package metadata
            package_id = f"export_{data_subject_id}_{datetime.utcnow().timestamp()}"
            
            metadata = {
                "package_id": package_id,
                "data_subject_id": data_subject_id,
                "framework": framework,
                "created_at": datetime.utcnow().isoformat(),
                "export_key": export_key["key_id"],
                "expiry": (datetime.utcnow() + timedelta(days=30)).isoformat()
            }
            
            # Store metadata
            self.vault.write(
                f"secret/compliance/data-exports/{package_id}",
                **metadata
            )
            
            return metadata
            
        except Exception as e:
            logger.error(f"Failed to create export package: {e}")
            raise
            
    async def get_cross_border_transfer_key(self,
                                          source_country: str,
                                          dest_country: str) -> Dict[str, Any]:
        """Get encryption key for cross-border data transfers"""
        try:
            transfer_id = f"{source_country}-{dest_country}".lower()
            
            # Check if transfer is allowed
            allowed = await self._check_transfer_allowed(source_country, dest_country)
            if not allowed:
                raise Exception(f"Transfer from {source_country} to {dest_country} not allowed")
                
            # Get or create transfer key
            response = self.vault.read(f"compliance-transit/keys/transfer-{transfer_id}")
            
            if not response:
                # Create transfer key
                self.vault.write(
                    f"compliance-transit/keys/transfer-{transfer_id}",
                    type="aes256-gcm96",
                    exportable=False,
                    allow_plaintext_backup=False
                )
                
            return {
                "key_name": f"transfer-{transfer_id}",
                "source": source_country,
                "destination": dest_country,
                "allowed": True
            }
            
        except Exception as e:
            logger.error(f"Failed to get transfer key: {e}")
            raise
            
    async def store_consent_record(self,
                                 data_subject_id: str,
                                 consent_data: Dict[str, Any]):
        """Store consent record with cryptographic proof"""
        try:
            # Add timestamp and hash
            consent_data["timestamp"] = datetime.utcnow().isoformat()
            consent_data["data_subject_id"] = data_subject_id
            
            # Create hash of consent
            consent_json = json.dumps(consent_data, sort_keys=True)
            consent_hash = hashlib.sha256(consent_json.encode()).hexdigest()
            
            # Sign consent
            signature = await self.sign_evidence(
                consent_json.encode(),
                framework="gdpr"
            )
            
            # Store encrypted
            encrypted = await self.encrypt_audit_log(consent_data, "gdpr")
            
            # Store in Vault
            self.vault.write(
                f"secret/compliance/consent-records/{data_subject_id}/{consent_hash}",
                encrypted_consent=encrypted["ciphertext"],
                signature=signature["signature"],
                hash=consent_hash,
                timestamp=consent_data["timestamp"]
            )
            
            # Also store reference in Consul for quick lookup
            await self.consul.kv.put(
                f"compliance/consent/{data_subject_id}/current",
                json.dumps({
                    "hash": consent_hash,
                    "timestamp": consent_data["timestamp"],
                    "purposes": consent_data.get("purposes", [])
                })
            )
            
        except Exception as e:
            logger.error(f"Failed to store consent record: {e}")
            raise
            
    async def _load_compliance_config(self):
        """Load compliance configuration from Consul"""
        try:
            configs = [
                "frameworks",
                "data-classifications", 
                "audit-requirements",
                "encryption-standards"
            ]
            
            for config_name in configs:
                _, data = await self.consul.kv.get(f"compliance/config/{config_name}")
                if data and data["Value"]:
                    self.compliance_config[config_name] = json.loads(data["Value"])
                    
            logger.info(f"Loaded {len(self.compliance_config)} compliance configurations")
            
        except Exception as e:
            logger.error(f"Failed to load compliance config: {e}")
            
    async def _setup_compliance_certificates(self):
        """Setup compliance certificates"""
        try:
            # Generate root CA for compliance if not exists
            response = self.vault.read("pki_compliance/ca/pem")
            
            if not response:
                # Mount PKI engine for compliance
                self.vault.sys.enable_secrets_engine(
                    backend_type="pki",
                    path="pki_compliance",
                    config={"max_lease_ttl": "87600h"}  # 10 years
                )
                
                # Generate root CA
                self.vault.write(
                    "pki_compliance/root/generate/internal",
                    common_name="PlatformQ Compliance CA",
                    ttl="87600h"
                )
                
                # Configure URLs
                self.vault.write(
                    "pki_compliance/config/urls",
                    issuing_certificates="http://vault:8200/v1/pki_compliance/ca",
                    crl_distribution_points="http://vault:8200/v1/pki_compliance/crl"
                )
                
        except Exception as e:
            logger.error(f"Failed to setup compliance certificates: {e}")
            
    async def _setup_policy_watchers(self):
        """Setup watchers for compliance policy changes"""
        async def watch_policy(policy_type: str):
            index = None
            while True:
                try:
                    index, data = await self.consul.kv.get(
                        f"compliance/policies/{policy_type}",
                        index=index,
                        wait="30s"
                    )
                    
                    if data and data["Value"]:
                        new_policy = json.loads(data["Value"])
                        await self._on_policy_change(policy_type, new_policy)
                        
                except Exception as e:
                    logger.error(f"Policy watcher error for {policy_type}: {e}")
                    await asyncio.sleep(10)
                    
        # Watch important policies
        for policy_type in ["data-retention", "encryption", "audit"]:
            self._policy_watchers[policy_type] = asyncio.create_task(
                watch_policy(policy_type)
            )
            
    async def _on_policy_change(self, policy_type: str, new_policy: Dict[str, Any]):
        """Handle policy changes"""
        logger.info(f"Compliance policy changed: {policy_type}")
        
        # Implement specific handlers
        if policy_type == "data-retention":
            # Update retention schedules
            pass
        elif policy_type == "encryption":
            # Update encryption requirements
            pass
        elif policy_type == "audit":
            # Update audit requirements
            pass
            
    async def _generate_compliance_certificate(self,
                                             framework: str,
                                             purpose: str) -> Dict[str, str]:
        """Generate compliance certificate"""
        # Generate key pair
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=4096,
            backend=default_backend()
        )
        
        # Certificate details
        subject = x509.Name([
            x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "PlatformQ"),
            x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, f"Compliance-{framework.upper()}"),
            x509.NameAttribute(NameOID.COMMON_NAME, f"compliance-{framework}-{purpose}")
        ])
        
        # Create certificate
        cert = x509.CertificateBuilder().subject_name(
            subject
        ).issuer_name(
            subject  # Self-signed for now
        ).public_key(
            private_key.public_key()
        ).serial_number(
            x509.random_serial_number()
        ).not_valid_before(
            datetime.utcnow()
        ).not_valid_after(
            datetime.utcnow() + timedelta(days=365)
        ).add_extension(
            x509.SubjectAlternativeName([
                x509.DNSName(f"compliance-{framework}.platformq.local"),
            ]),
            critical=False,
        ).add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_encipherment=True,
                content_commitment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False
            ),
            critical=True,
        ).sign(private_key, hashes.SHA256(), default_backend())
        
        # Convert to PEM
        cert_pem = cert.public_bytes(serialization.Encoding.PEM).decode()
        key_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ).decode()
        
        return {
            "certificate": cert_pem,
            "private_key": key_pem,
            "framework": framework,
            "purpose": purpose
        }
        
    async def _log_data_processing(self,
                                 operation: str,
                                 data_subject_id: str,
                                 purpose: str):
        """Log data processing for GDPR compliance"""
        log_entry = {
            "operation": operation,
            "data_subject_id": data_subject_id,
            "purpose": purpose,
            "timestamp": datetime.utcnow().isoformat(),
            "service": self.service_name,
            "node": self.node_id
        }
        
        # Encrypt and store
        encrypted = await self.encrypt_audit_log(log_entry, "gdpr")
        
        await self.consul.kv.put(
            f"compliance/gdpr-processing-log/{data_subject_id}/{datetime.utcnow().isoformat()}",
            json.dumps(encrypted)
        )
        
    async def _generate_export_key(self, data_subject_id: str) -> Dict[str, str]:
        """Generate temporary export key"""
        key_id = f"export_{data_subject_id}_{datetime.utcnow().timestamp()}"
        
        # Create temporary key
        self.vault.write(
            f"compliance-transit/keys/{key_id}",
            type="aes256-gcm96",
            exportable=True,
            deletion_allowed=True
        )
        
        # Set TTL
        self.vault.write(
            f"compliance-transit/keys/{key_id}/config",
            deletion_allowed=True,
            min_decryption_version=1,
            min_encryption_version=1,
            auto_rotate_period="0"  # No rotation for temp key
        )
        
        return {"key_id": key_id}
        
    async def _check_transfer_allowed(self,
                                    source_country: str,
                                    dest_country: str) -> bool:
        """Check if cross-border transfer is allowed"""
        # Get transfer rules from Consul
        _, data = await self.consul.kv.get("compliance/transfer-rules")
        
        if data and data["Value"]:
            rules = json.loads(data["Value"])
            
            # Check adequacy decisions
            if dest_country in rules.get("adequate_countries", []):
                return True
                
            # Check specific agreements
            agreements = rules.get("agreements", {})
            if source_country in agreements:
                if dest_country in agreements[source_country]:
                    return True
                    
        return False
        
    async def shutdown(self):
        """Cleanup resources"""
        # Cancel policy watchers
        for task in self._policy_watchers.values():
            task.cancel()
            
        await super().shutdown() 