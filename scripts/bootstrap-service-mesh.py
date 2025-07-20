#!/usr/bin/env python3
"""
Bootstrap script for PlatformQ Consul Connect Service Mesh

This script initializes the Consul cluster with:
- ACL policies for all services
- Service intentions based on dependencies
- Initial configuration in KV store
- Health check configurations
"""

import json
import subprocess
import time
import os
from pathlib import Path
from typing import Dict, List, Optional


class ConsulBootstrapper:
    """Bootstrap Consul cluster for PlatformQ"""
    
    def __init__(self, consul_addr: str = "localhost:8500"):
        self.consul_addr = consul_addr
        self.bootstrap_token = None
        self.service_tokens = {}
        
    def wait_for_consul(self, timeout: int = 60):
        """Wait for Consul to be ready"""
        print("Waiting for Consul to be ready...")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                result = subprocess.run(
                    ["consul", "members"],
                    capture_output=True,
                    text=True,
                    env={**os.environ, "CONSUL_HTTP_ADDR": self.consul_addr}
                )
                if result.returncode == 0:
                    print("Consul is ready!")
                    return True
            except Exception:
                pass
            time.sleep(2)
            
        raise TimeoutError("Consul failed to start within timeout period")
        
    def bootstrap_acl(self):
        """Bootstrap ACL system"""
        print("\nBootstrapping ACL system...")
        
        # Check if already bootstrapped
        token_file = Path("/tmp/consul-bootstrap-token")
        if token_file.exists():
            self.bootstrap_token = token_file.read_text().strip()
            print("Using existing bootstrap token")
            return
            
        # Bootstrap ACL
        result = subprocess.run(
            ["consul", "acl", "bootstrap", "-format=json"],
            capture_output=True,
            text=True,
            env={**os.environ, "CONSUL_HTTP_ADDR": self.consul_addr}
        )
        
        if result.returncode == 0:
            bootstrap_data = json.loads(result.stdout)
            self.bootstrap_token = bootstrap_data["SecretID"]
            
            # Save token securely
            token_file.write_text(self.bootstrap_token)
            token_file.chmod(0o600)
            
            print(f"ACL system bootstrapped successfully!")
            print(f"Bootstrap Token: {self.bootstrap_token}")
            print("IMPORTANT: This token has been saved to /tmp/consul-bootstrap-token")
        else:
            raise RuntimeError("Failed to bootstrap ACL system")
            
    def create_service_policy(self, service_name: str) -> str:
        """Create ACL policy for a service"""
        policy_name = f"{service_name}-policy"
        
        # Policy rules
        rules = f"""
service "{service_name}" {{
  policy = "write"
}}
service "{service_name}-sidecar-proxy" {{
  policy = "write"
}}
service_prefix "" {{
  policy = "read"
}}
node_prefix "" {{
  policy = "read"
}}
agent_prefix "" {{
  policy = "read"
}}
key_prefix "config/{service_name}/" {{
  policy = "write"
}}
"""
        
        # Create policy
        result = subprocess.run(
            ["consul", "acl", "policy", "create",
             "-name", policy_name,
             "-rules", rules],
            capture_output=True,
            text=True,
            env={
                **os.environ,
                "CONSUL_HTTP_ADDR": self.consul_addr,
                "CONSUL_HTTP_TOKEN": self.bootstrap_token
            }
        )
        
        if result.returncode == 0:
            print(f"Created policy: {policy_name}")
        else:
            # Policy might already exist
            print(f"Policy {policy_name} already exists or creation failed")
            
        return policy_name
        
    def create_service_token(self, service_name: str, policy_name: str) -> str:
        """Create ACL token for a service"""
        result = subprocess.run(
            ["consul", "acl", "token", "create",
             "-description", f"Token for {service_name}",
             "-policy-name", policy_name,
             "-format=json"],
            capture_output=True,
            text=True,
            env={
                **os.environ,
                "CONSUL_HTTP_ADDR": self.consul_addr,
                "CONSUL_HTTP_TOKEN": self.bootstrap_token
            }
        )
        
        if result.returncode == 0:
            token_data = json.loads(result.stdout)
            token = token_data["SecretID"]
            self.service_tokens[service_name] = token
            print(f"Created token for {service_name}")
            return token
        else:
            raise RuntimeError(f"Failed to create token for {service_name}")
            
    def create_intentions(self, intentions: List[Dict[str, str]]):
        """Create service intentions"""
        print("\nCreating service intentions...")
        
        for intention in intentions:
            source = intention["source"]
            destination = intention["destination"]
            action = intention.get("action", "allow")
            
            cmd = ["consul", "intention", "create"]
            if action == "allow":
                cmd.append("-allow")
            else:
                cmd.append("-deny")
                
            cmd.extend([source, destination])
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                env={
                    **os.environ,
                    "CONSUL_HTTP_ADDR": self.consul_addr,
                    "CONSUL_HTTP_TOKEN": self.bootstrap_token
                }
            )
            
            if result.returncode == 0:
                print(f"Created intention: {source} -> {destination} ({action})")
            else:
                # Intention might already exist
                print(f"Intention {source} -> {destination} already exists or creation failed")
                
    def load_kv_config(self, configs: Dict[str, Dict]):
        """Load initial configuration into Consul KV store"""
        print("\nLoading configuration into KV store...")
        
        for key, value in configs.items():
            json_value = json.dumps(value, indent=2)
            
            result = subprocess.run(
                ["consul", "kv", "put", key, json_value],
                capture_output=True,
                text=True,
                env={
                    **os.environ,
                    "CONSUL_HTTP_ADDR": self.consul_addr,
                    "CONSUL_HTTP_TOKEN": self.bootstrap_token
                }
            )
            
            if result.returncode == 0:
                print(f"Loaded config: {key}")
            else:
                print(f"Failed to load config: {key}")
                
    def generate_env_file(self):
        """Generate environment file with tokens"""
        env_file = Path("consul-tokens.env")
        
        with open(env_file, 'w') as f:
            f.write("# Consul Service Tokens\n")
            f.write("# Source this file to set environment variables\n\n")
            f.write(f"export CONSUL_HTTP_TOKEN={self.bootstrap_token}\n")
            f.write(f"export CONSUL_BOOTSTRAP_TOKEN={self.bootstrap_token}\n\n")
            
            for service, token in self.service_tokens.items():
                env_var = service.upper().replace('-', '_') + "_CONSUL_TOKEN"
                f.write(f"export {env_var}={token}\n")
                
        env_file.chmod(0o600)
        print(f"\nGenerated token file: {env_file}")
        print("Source this file to set tokens: source consul-tokens.env")


def main():
    """Main bootstrap function"""
    
    # Initialize bootstrapper
    bootstrapper = ConsulBootstrapper()
    
    # Wait for Consul
    bootstrapper.wait_for_consul()
    
    # Bootstrap ACL
    bootstrapper.bootstrap_acl()
    
    # Get all services from service definitions
    consul_services_dir = Path("consul/services")
    services = []
    
    for service_file in consul_services_dir.glob("*.json"):
        if service_file.stem in ["vault", "ignite-cache", "pulsar", "cassandra", 
                                  "elasticsearch", "minio", "janusgraph", "opa"]:
            continue
            
        with open(service_file) as f:
            service_def = json.load(f)
            services.append(service_def["service"]["name"])
    
    # Create policies and tokens for all services
    print(f"\nCreating policies and tokens for {len(services)} services...")
    
    for service in services:
        policy_name = bootstrapper.create_service_policy(service)
        bootstrapper.create_service_token(service, policy_name)
    
    # Define service intentions based on dependencies
    intentions = [
        # Allow all services to access auth-service
        {"source": "*", "destination": "auth-service", "action": "allow"},
        
        # Allow all services to access infrastructure
        {"source": "*", "destination": "ignite-cache", "action": "allow"},
        {"source": "*", "destination": "pulsar", "action": "allow"},
        
        # Specific service intentions
        {"source": "auth-service", "destination": "vault", "action": "allow"},
        {"source": "auth-service", "destination": "compliance-service", "action": "allow"},
        {"source": "auth-service", "destination": "security-service", "action": "allow"},
        
        {"source": "blockchain-gateway-service", "destination": "vault", "action": "allow"},
        {"source": "blockchain-gateway-service", "destination": "graph-intelligence-service", "action": "allow"},
        
        {"source": "data-platform-service", "destination": "cassandra", "action": "allow"},
        {"source": "data-platform-service", "destination": "elasticsearch", "action": "allow"},
        {"source": "data-platform-service", "destination": "minio", "action": "allow"},
        {"source": "data-platform-service", "destination": "janusgraph", "action": "allow"},
        
        {"source": "trading-platform-service", "destination": "order-matching-service", "action": "allow"},
        {"source": "trading-platform-service", "destination": "market-data-service", "action": "allow"},
        {"source": "trading-platform-service", "destination": "risk-management-service", "action": "allow"},
        
        {"source": "order-matching-service", "destination": "market-data-service", "action": "allow"},
        {"source": "order-matching-service", "destination": "risk-management-service", "action": "allow"},
        
        {"source": "risk-management-service", "destination": "risk-engine-service", "action": "allow"},
        
        {"source": "unified-ml-platform-service", "destination": "mlflow-server", "action": "allow"},
        {"source": "unified-ml-platform-service", "destination": "compute-allocation-service", "action": "allow"},
        
        # Default deny all
        {"source": "*", "destination": "*", "action": "deny"}
    ]
    
    bootstrapper.create_intentions(intentions)
    
    # Load initial configurations
    kv_configs = {
        "config/auth-service/settings": {
            "jwt_expiry": 3600,
            "refresh_token_expiry": 604800,
            "max_login_attempts": 5,
            "lockout_duration": 900,
            "password_min_length": 12,
            "require_2fa": False,
            "session_timeout": 1800
        },
        "config/market-data-service/settings": {
            "websocket_heartbeat_interval": 30,
            "market_data_cache_ttl": 5,
            "orderbook_depth": 20,
            "price_aggregation_interval": 1,
            "max_subscriptions_per_client": 100
        },
        "config/trading-platform-service/settings": {
            "max_open_orders": 100,
            "order_rate_limit": 100,
            "min_order_size": 0.001,
            "max_order_size": 10000,
            "maker_fee_bps": 10,
            "taker_fee_bps": 25
        },
        "config/risk-management-service/settings": {
            "max_leverage": 10,
            "margin_call_ratio": 0.5,
            "liquidation_ratio": 0.8,
            "position_limit": 1000000,
            "daily_loss_limit": 0.1,
            "risk_check_interval": 5
        },
        "config/data-platform-service/settings": {
            "batch_size": 1000,
            "retention_days": 365,
            "compression_enabled": True,
            "partitioning_strategy": "daily",
            "replication_factor": 3
        }
    }
    
    bootstrapper.load_kv_config(kv_configs)
    
    # Generate environment file
    bootstrapper.generate_env_file()
    
    print("\n" + "="*60)
    print("Consul Service Mesh Bootstrap Complete!")
    print("="*60)
    print(f"\nConsul UI: http://localhost:8500")
    print(f"Login with bootstrap token: {bootstrapper.bootstrap_token}")
    print("\nNext steps:")
    print("1. Source the token file: source consul-tokens.env")
    print("2. Start services: docker-compose -f docker-compose.service-mesh.yml up -d")
    print("3. Monitor services in Consul UI")


if __name__ == "__main__":
    main() 