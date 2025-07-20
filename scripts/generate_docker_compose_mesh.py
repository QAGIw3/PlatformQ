#!/usr/bin/env python3
"""
Generate comprehensive docker-compose file for Consul Connect service mesh
"""

import yaml
import json
from pathlib import Path

def generate_consul_servers():
    """Generate Consul server configurations"""
    servers = {}
    
    for i in range(1, 4):
        server_name = f"consul-server-{i}"
        config = {
            "image": "consul:1.16",
            "container_name": server_name,
            "restart": "unless-stopped",
            "volumes": [
                "./consul/config/consul.hcl:/consul/config/consul.hcl:ro",
                f"{server_name}-data:/consul/data"
            ],
            "networks": ["consul"],
            "environment": [
                "CONSUL_BIND_INTERFACE=eth0",
                "CONSUL_CLIENT_INTERFACE=eth0"
            ]
        }
        
        if i == 1:
            config["ports"] = [
                "8500:8500",
                "8600:8600/tcp",
                "8600:8600/udp"
            ]
            config["command"] = f"agent -server -ui -node={server_name} -bootstrap-expect=3 -config-file=/consul/config/consul.hcl"
        else:
            config["command"] = f"agent -server -node={server_name} -bootstrap-expect=3 -retry-join=consul-server-1 -config-file=/consul/config/consul.hcl"
            
        servers[server_name] = config
        
    return servers


def generate_infrastructure_services():
    """Generate infrastructure service configurations"""
    services = {
        "vault": {
            "image": "vault:1.14",
            "container_name": "vault",
            "restart": "unless-stopped",
            "networks": ["consul"],
            "ports": ["8200:8200"],
            "environment": [
                "VAULT_DEV_ROOT_TOKEN_ID=root",
                "VAULT_DEV_LISTEN_ADDRESS=0.0.0.0:8200"
            ],
            "cap_add": ["IPC_LOCK"]
        },
        "ignite": {
            "image": "apacheignite/ignite:2.15.0",
            "container_name": "ignite",
            "restart": "unless-stopped",
            "networks": ["consul"],
            "ports": ["10800:10800", "11211:11211"],
            "environment": ["IGNITE_QUIET=false"],
            "volumes": ["./infra/ignite:/opt/ignite/config:ro"]
        },
        "pulsar": {
            "image": "apachepulsar/pulsar:3.1.0",
            "container_name": "pulsar",
            "restart": "unless-stopped",
            "networks": ["consul"],
            "ports": ["6650:6650", "8080:8080"],
            "command": "bin/pulsar standalone"
        },
        "cassandra": {
            "image": "cassandra:4.1",
            "container_name": "cassandra",
            "restart": "unless-stopped",
            "networks": ["consul"],
            "ports": ["9042:9042"],
            "environment": [
                "CASSANDRA_CLUSTER_NAME=platformq",
                "CASSANDRA_DC=dc1",
                "CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch"
            ]
        },
        "elasticsearch": {
            "image": "elasticsearch:8.10.2",
            "container_name": "elasticsearch",
            "restart": "unless-stopped",
            "networks": ["consul"],
            "ports": ["9200:9200", "9300:9300"],
            "environment": [
                "discovery.type=single-node",
                "xpack.security.enabled=false",
                "ES_JAVA_OPTS=-Xms512m -Xmx512m"
            ]
        },
        "minio": {
            "image": "minio/minio:latest",
            "container_name": "minio",
            "restart": "unless-stopped",
            "networks": ["consul"],
            "ports": ["9000:9000", "9001:9001"],
            "environment": [
                "MINIO_ROOT_USER=minioadmin",
                "MINIO_ROOT_PASSWORD=minioadmin"
            ],
            "command": "server /data --console-address \":9001\""
        },
        "janusgraph": {
            "image": "janusgraph/janusgraph:latest",
            "container_name": "janusgraph",
            "restart": "unless-stopped",
            "networks": ["consul"],
            "ports": ["8182:8182"],
            "environment": [
                "janusgraph.storage.backend=cassandra",
                "janusgraph.storage.hostname=cassandra",
                "janusgraph.index.search.backend=elasticsearch",
                "janusgraph.index.search.hostname=elasticsearch"
            ],
            "depends_on": ["cassandra", "elasticsearch"]
        },
        "opa": {
            "image": "openpolicyagent/opa:latest",
            "container_name": "opa",
            "restart": "unless-stopped",
            "networks": ["consul"],
            "ports": ["8181:8181"],
            "command": "run --server --addr :8181",
            "volumes": ["./infra/docker-compose/opa:/policies:ro"]
        }
    }
    
    return services


def generate_service_configuration(service_name, sidecar_port, admin_port):
    """Generate configuration for a single service with Consul agent and Envoy sidecar"""
    
    # Clean service name for container names
    clean_name = service_name.replace("-service", "")
    
    config = {}
    
    # Main service
    config[service_name] = {
        "build": {
            "context": f"./services/{service_name}",
            "dockerfile": "Dockerfile"
        },
        "container_name": service_name,
        "restart": "unless-stopped",
        "networks": ["consul"],
        "environment": [
            f"CONSUL_HTTP_ADDR=consul-agent-{clean_name}:8500",
            f"SERVICE_NAME={service_name}",
            "SERVICE_PORT=8000"
        ],
        "depends_on": [f"consul-agent-{clean_name}"]
    }
    
    # Consul agent
    config[f"consul-agent-{clean_name}"] = {
        "image": "consul:1.16",
        "container_name": f"consul-agent-{clean_name}",
        "restart": "unless-stopped",
        "volumes": [
            "./consul/config/consul-client.hcl:/consul/config/consul-client.hcl:ro",
            f"./consul/services/{service_name}.json:/consul/config/{service_name}.json:ro"
        ],
        "networks": ["consul"],
        "environment": [
            "CONSUL_BIND_INTERFACE=eth0",
            "CONSUL_CLIENT_INTERFACE=eth0"
        ],
        "command": f"agent -node={clean_name}-node -config-file=/consul/config/consul-client.hcl -config-file=/consul/config/{service_name}.json",
        "depends_on": ["consul-server-1", "consul-server-2", "consul-server-3"]
    }
    
    # Envoy sidecar
    config[f"{clean_name}-sidecar"] = {
        "image": "envoyproxy/envoy-alpine:v1.27.0",
        "container_name": f"{clean_name}-sidecar",
        "restart": "unless-stopped",
        "networks": ["consul"],
        "environment": [
            f"CONSUL_HTTP_ADDR=consul-agent-{clean_name}:8500",
            f"SERVICE_NAME={service_name}",
            "SERVICE_PORT=8000"
        ],
        "command": f'sh -c "consul connect envoy -sidecar-for {service_name} -admin-bind 0.0.0.0:{admin_port}"',
        "depends_on": [
            f"consul-agent-{clean_name}",
            service_name
        ]
    }
    
    return config


def main():
    """Generate the complete docker-compose file"""
    
    # Read service definitions to get port mappings
    consul_services_dir = Path("consul/services")
    service_ports = {}
    
    for service_file in consul_services_dir.glob("*.json"):
        if service_file.stem in ["vault", "ignite-cache", "pulsar", "cassandra", 
                                  "elasticsearch", "minio", "janusgraph", "opa"]:
            continue
            
        with open(service_file) as f:
            service_def = json.load(f)
            service_name = service_def["service"]["name"]
            sidecar_port = service_def["service"]["connect"]["sidecar_service"]["port"]
            # Calculate Envoy admin port based on sidecar port
            admin_port = 19000 + (sidecar_port - 21000)
            service_ports[service_name] = {
                "sidecar_port": sidecar_port,
                "admin_port": admin_port
            }
    
    # Generate complete docker-compose configuration
    compose_config = {
        "version": "3.8",
        "services": {},
        "networks": {
            "consul": {
                "driver": "bridge"
            }
        },
        "volumes": {
            "consul-server-1-data": None,
            "consul-server-2-data": None,
            "consul-server-3-data": None
        }
    }
    
    # Add Consul servers
    compose_config["services"].update(generate_consul_servers())
    
    # Add infrastructure services
    compose_config["services"].update(generate_infrastructure_services())
    
    # Add all application services
    for service_name, ports in sorted(service_ports.items()):
        service_config = generate_service_configuration(
            service_name, 
            ports["sidecar_port"], 
            ports["admin_port"]
        )
        compose_config["services"].update(service_config)
    
    # Write docker-compose file
    output_file = Path("docker-compose.service-mesh.yml")
    with open(output_file, 'w') as f:
        yaml.dump(compose_config, f, default_flow_style=False, sort_keys=False)
    
    print(f"Generated docker-compose file: {output_file}")
    print(f"Total services: {len(service_ports)} application services + infrastructure")
    
    # Generate a minimal startup script
    startup_script = Path("scripts/start-service-mesh.sh")
    startup_script.write_text("""#!/bin/bash
# Start PlatformQ Service Mesh

echo "Starting Consul servers..."
docker-compose -f docker-compose.service-mesh.yml up -d consul-server-1 consul-server-2 consul-server-3

echo "Waiting for Consul cluster to form..."
sleep 10

echo "Starting infrastructure services..."
docker-compose -f docker-compose.service-mesh.yml up -d vault ignite pulsar cassandra elasticsearch minio

echo "Waiting for infrastructure to be ready..."
sleep 20

echo "Starting JanusGraph and OPA..."
docker-compose -f docker-compose.service-mesh.yml up -d janusgraph opa

echo "Starting all application services..."
docker-compose -f docker-compose.service-mesh.yml up -d

echo "Service mesh started! Consul UI available at http://localhost:8500"
""")
    startup_script.chmod(0o755)
    
    print(f"Generated startup script: {startup_script}")


if __name__ == "__main__":
    main() 