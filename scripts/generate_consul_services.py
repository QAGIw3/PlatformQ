#!/usr/bin/env python3
"""
Generate Consul service definitions for all PlatformQ services
"""

import json
import os
from pathlib import Path

# Service configurations with their dependencies
SERVICE_CONFIGS = {
    "analytics-service": {
        "tags": ["analytics", "reporting", "visualization", "api"],
        "port": 8000,
        "sidecar_port": 21020,
        "prometheus_port": 9122,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "analytics_engines": "spark,flink,trino"
        },
        "upstreams": [
            "auth-service", "data-platform-service", "ignite-cache", 
            "pulsar", "cassandra", "elasticsearch", "minio"
        ]
    },
    "compliance-service": {
        "tags": ["compliance", "regulatory", "audit", "api"],
        "port": 8000,
        "sidecar_port": 21021,
        "prometheus_port": 9123,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "compliance_standards": "kyc,aml,gdpr,mifid2"
        },
        "upstreams": [
            "auth-service", "blockchain-gateway-service", "ignite-cache",
            "pulsar", "cassandra", "graph-intelligence-service"
        ]
    },
    "compute-allocation-service": {
        "tags": ["compute", "resource-management", "scheduling", "api"],
        "port": 8000,
        "sidecar_port": 21022,
        "prometheus_port": 9124,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "compute_types": "cpu,gpu,tpu,quantum"
        },
        "upstreams": [
            "auth-service", "provisioning-service", "ignite-cache",
            "pulsar", "cassandra", "workflow-service"
        ]
    },
    "connector-service": {
        "tags": ["integration", "connector", "etl", "api"],
        "port": 8000,
        "sidecar_port": 21023,
        "prometheus_port": 9125,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "connectors": "seatunnel,flink,spark"
        },
        "upstreams": [
            "auth-service", "data-platform-service", "ignite-cache",
            "pulsar", "cassandra", "minio"
        ]
    },
    "data-platform-service": {
        "tags": ["data", "lake", "warehouse", "api"],
        "port": 8000,
        "sidecar_port": 21024,
        "prometheus_port": 9126,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "data_stores": "cassandra,minio,elasticsearch,janusgraph"
        },
        "upstreams": [
            "auth-service", "connector-service", "ignite-cache",
            "pulsar", "cassandra", "elasticsearch", "minio", "janusgraph"
        ]
    },
    "dataset-marketplace": {
        "tags": ["marketplace", "dataset", "trading", "api"],
        "port": 8000,
        "sidecar_port": 21025,
        "prometheus_port": 9127,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "dataset_types": "market,research,ai_training"
        },
        "upstreams": [
            "auth-service", "blockchain-gateway-service", "storage-service",
            "ignite-cache", "pulsar", "cassandra", "minio"
        ]
    },
    "defi-protocol-service": {
        "tags": ["defi", "protocol", "liquidity", "api"],
        "port": 8000,
        "sidecar_port": 21026,
        "prometheus_port": 9128,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "protocols": "lending,staking,yield_farming"
        },
        "upstreams": [
            "auth-service", "blockchain-gateway-service", "amm-service",
            "ignite-cache", "pulsar", "cassandra"
        ]
    },
    "derivatives-engine-service": {
        "tags": ["derivatives", "engine", "pricing", "api"],
        "port": 8000,
        "sidecar_port": 21027,
        "prometheus_port": 9129,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "derivatives": "options,futures,swaps,structured"
        },
        "upstreams": [
            "auth-service", "market-data-service", "risk-management-service",
            "ignite-cache", "pulsar", "cassandra"
        ]
    },
    "digital-asset-service": {
        "tags": ["digital-assets", "nft", "tokenization", "api"],
        "port": 8000,
        "sidecar_port": 21028,
        "prometheus_port": 9130,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "asset_types": "nft,rwa,synthetic"
        },
        "upstreams": [
            "auth-service", "blockchain-gateway-service", "storage-service",
            "ignite-cache", "pulsar", "cassandra", "minio"
        ]
    },
    "event-router-service": {
        "tags": ["events", "routing", "messaging", "api"],
        "port": 8000,
        "sidecar_port": 21029,
        "prometheus_port": 9131,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "event_types": "trade,market,system,blockchain"
        },
        "upstreams": [
            "auth-service", "pulsar", "ignite-cache"
        ]
    },
    "functions-service": {
        "tags": ["serverless", "functions", "compute", "api"],
        "port": 8000,
        "sidecar_port": 21030,
        "prometheus_port": 9132,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "runtime": "wasm,python,javascript"
        },
        "upstreams": [
            "auth-service", "compute-allocation-service", "storage-service",
            "ignite-cache", "pulsar"
        ]
    },
    "futures-service": {
        "tags": ["futures", "derivatives", "trading", "api"],
        "port": 8000,
        "sidecar_port": 21031,
        "prometheus_port": 9133,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "futures_types": "perpetual,dated,compute"
        },
        "upstreams": [
            "auth-service", "market-data-service", "derivatives-engine-service",
            "ignite-cache", "pulsar", "cassandra"
        ]
    },
    "governance-service": {
        "tags": ["governance", "dao", "voting", "api"],
        "port": 8000,
        "sidecar_port": 21032,
        "prometheus_port": 9134,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "governance_types": "token,reputation,quadratic"
        },
        "upstreams": [
            "auth-service", "blockchain-gateway-service", "graph-intelligence-service",
            "ignite-cache", "pulsar", "cassandra"
        ]
    },
    "graph-intelligence-service": {
        "tags": ["graph", "intelligence", "analytics", "api"],
        "port": 8000,
        "sidecar_port": 21033,
        "prometheus_port": 9135,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "graph_db": "janusgraph",
            "algorithms": "pagerank,community,pathfinding"
        },
        "upstreams": [
            "auth-service", "data-platform-service", "janusgraph",
            "ignite-cache", "pulsar", "elasticsearch"
        ]
    },
    "insurance-pool-service": {
        "tags": ["insurance", "risk-pool", "coverage", "api"],
        "port": 8000,
        "sidecar_port": 21034,
        "prometheus_port": 9136,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "coverage_types": "smart_contract,slashing,impermanent_loss"
        },
        "upstreams": [
            "auth-service", "blockchain-gateway-service", "risk-management-service",
            "ignite-cache", "pulsar", "cassandra"
        ]
    },
    "mlflow-server": {
        "tags": ["mlflow", "ml-tracking", "experiments", "api"],
        "port": 5000,
        "sidecar_port": 21035,
        "prometheus_port": 9137,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "ml_frameworks": "tensorflow,pytorch,sklearn"
        },
        "upstreams": [
            "auth-service", "storage-service", "minio",
            "cassandra"
        ]
    },
    "options-service": {
        "tags": ["options", "derivatives", "trading", "api"],
        "port": 8000,
        "sidecar_port": 21036,
        "prometheus_port": 9138,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "option_types": "european,american,exotic"
        },
        "upstreams": [
            "auth-service", "market-data-service", "derivatives-engine-service",
            "ignite-cache", "pulsar", "cassandra"
        ]
    },
    "order-matching-service": {
        "tags": ["order-matching", "exchange", "trading", "api"],
        "port": 8000,
        "sidecar_port": 21037,
        "prometheus_port": 9139,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "matching_engine": "clob,amm,rfq"
        },
        "upstreams": [
            "auth-service", "market-data-service", "risk-management-service",
            "ignite-cache", "pulsar", "cassandra"
        ]
    },
    "provisioning-service": {
        "tags": ["provisioning", "infrastructure", "cloudstack", "api"],
        "port": 8000,
        "sidecar_port": 21038,
        "prometheus_port": 9140,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "providers": "cloudstack,kubernetes,openstack"
        },
        "upstreams": [
            "auth-service", "compute-allocation-service", "vault",
            "ignite-cache", "pulsar"
        ]
    },
    "quantum-optimization-service": {
        "tags": ["quantum", "optimization", "compute", "api"],
        "port": 8000,
        "sidecar_port": 21039,
        "prometheus_port": 9141,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "quantum_algorithms": "vqe,qaoa,grover"
        },
        "upstreams": [
            "auth-service", "compute-allocation-service", "unified-ml-platform-service",
            "ignite-cache", "pulsar"
        ]
    },
    "risk-engine-service": {
        "tags": ["risk", "engine", "calculation", "api"],
        "port": 8000,
        "sidecar_port": 21040,
        "prometheus_port": 9142,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "risk_models": "var,cvar,montecarlo"
        },
        "upstreams": [
            "auth-service", "market-data-service", "graph-intelligence-service",
            "ignite-cache", "pulsar", "cassandra"
        ]
    },
    "risk-management-service": {
        "tags": ["risk-management", "limits", "monitoring", "api"],
        "port": 8000,
        "sidecar_port": 21041,
        "prometheus_port": 9143,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "risk_types": "market,credit,operational,liquidity"
        },
        "upstreams": [
            "auth-service", "risk-engine-service", "market-data-service",
            "ignite-cache", "pulsar", "cassandra"
        ]
    },
    "search-service": {
        "tags": ["search", "indexing", "discovery", "api"],
        "port": 8000,
        "sidecar_port": 21042,
        "prometheus_port": 9144,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "search_engine": "elasticsearch"
        },
        "upstreams": [
            "auth-service", "data-platform-service", "elasticsearch",
            "ignite-cache"
        ]
    },
    "security-service": {
        "tags": ["security", "monitoring", "threat-detection", "api"],
        "port": 8000,
        "sidecar_port": 21043,
        "prometheus_port": 9145,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "security_features": "waf,ids,dlp,siem"
        },
        "upstreams": [
            "auth-service", "vault", "opa",
            "ignite-cache", "pulsar", "elasticsearch"
        ]
    },
    "state-management-service": {
        "tags": ["state", "management", "synchronization", "api"],
        "port": 8000,
        "sidecar_port": 21044,
        "prometheus_port": 9146,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "state_stores": "ignite,cassandra"
        },
        "upstreams": [
            "auth-service", "ignite-cache", "cassandra",
            "pulsar"
        ]
    },
    "storage-service": {
        "tags": ["storage", "object", "files", "api"],
        "port": 8000,
        "sidecar_port": 21045,
        "prometheus_port": 9147,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "storage_backend": "minio"
        },
        "upstreams": [
            "auth-service", "minio", "vault",
            "ignite-cache"
        ]
    },
    "structured-products-service": {
        "tags": ["structured-products", "derivatives", "complex", "api"],
        "port": 8000,
        "sidecar_port": 21046,
        "prometheus_port": 9148,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "product_types": "autocallable,barrier,accumulator"
        },
        "upstreams": [
            "auth-service", "derivatives-engine-service", "market-data-service",
            "ignite-cache", "pulsar", "cassandra"
        ]
    },
    "trading-platform-service": {
        "tags": ["trading", "platform", "exchange", "api"],
        "port": 8000,
        "sidecar_port": 21047,
        "prometheus_port": 9149,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "trading_types": "spot,margin,futures,options"
        },
        "upstreams": [
            "auth-service", "order-matching-service", "market-data-service",
            "risk-management-service", "ignite-cache", "pulsar", "cassandra"
        ]
    },
    "unified-ml-platform-service": {
        "tags": ["ml", "ai", "platform", "api"],
        "port": 8000,
        "sidecar_port": 21048,
        "prometheus_port": 9150,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "ml_capabilities": "training,inference,automl,federated"
        },
        "upstreams": [
            "auth-service", "compute-allocation-service", "storage-service",
            "mlflow-server", "ignite-cache", "pulsar", "minio"
        ]
    },
    "verifiable-credential-service": {
        "tags": ["credentials", "did", "verifiable", "api"],
        "port": 8000,
        "sidecar_port": 21049,
        "prometheus_port": 9151,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "credential_types": "kyc,professional,academic"
        },
        "upstreams": [
            "auth-service", "blockchain-gateway-service", "storage-service",
            "ignite-cache", "pulsar", "cassandra"
        ]
    },
    "workflow-service": {
        "tags": ["workflow", "orchestration", "automation", "api"],
        "port": 8000,
        "sidecar_port": 21050,
        "prometheus_port": 9152,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "workflow_engine": "airflow"
        },
        "upstreams": [
            "auth-service", "event-router-service", "compute-allocation-service",
            "ignite-cache", "pulsar"
        ]
    },
    "collaboration-platform-service": {
        "tags": ["collaboration", "communication", "social", "api"],
        "port": 8000,
        "sidecar_port": 21051,
        "prometheus_port": 9153,
        "meta": {
            "version": "1.0.0",
            "protocol": "http",
            "features": "chat,video,screen_sharing,whiteboard"
        },
        "upstreams": [
            "auth-service", "storage-service", "event-router-service",
            "ignite-cache", "pulsar", "cassandra"
        ]
    }
}

# Infrastructure service configurations
INFRA_SERVICES = {
    "vault": {"protocol": "http", "port": 8200},
    "ignite-cache": {"protocol": "tcp", "port": 10800},
    "pulsar": {"protocol": "tcp", "port": 6650},
    "cassandra": {"protocol": "tcp", "port": 9042},
    "elasticsearch": {"protocol": "http", "port": 9200},
    "minio": {"protocol": "http", "port": 9000},
    "janusgraph": {"protocol": "http", "port": 8182},
    "opa": {"protocol": "http", "port": 8181}
}


def generate_service_definition(service_name, config):
    """Generate a Consul service definition"""
    
    # Build upstreams configuration
    upstreams = []
    local_bind_port = config.get("upstream_base_port", 5000)
    
    for upstream in config.get("upstreams", []):
        upstream_config = {
            "destination_name": upstream,
            "local_bind_port": local_bind_port,
            "config": {}
        }
        
        # Set protocol based on infrastructure service or default to http
        if upstream in INFRA_SERVICES:
            upstream_config["config"]["protocol"] = INFRA_SERVICES[upstream]["protocol"]
        else:
            upstream_config["config"]["protocol"] = "http"
            
        # Add connection limits for non-infrastructure services
        if upstream not in INFRA_SERVICES:
            upstream_config["config"]["limits"] = {
                "max_connections": 100,
                "max_pending_requests": 50,
                "max_concurrent_requests": 50
            }
            
        upstreams.append(upstream_config)
        local_bind_port += 1
    
    service_def = {
        "service": {
            "name": service_name,
            "tags": config.get("tags", []),
            "port": config.get("port", 8000),
            "meta": config.get("meta", {}),
            "check": {
                "id": f"{service_name}-health",
                "name": "HTTP Health Check",
                "http": f"http://localhost:{config.get('port', 8000)}/health",
                "method": "GET",
                "interval": "10s",
                "timeout": "5s"
            },
            "connect": {
                "sidecar_service": {
                    "port": config.get("sidecar_port", 21000),
                    "proxy": {
                        "local_service_address": "127.0.0.1",
                        "local_service_port": config.get("port", 8000),
                        "config": {
                            "protocol": "http",
                            "envoy_prometheus_bind_addr": f"0.0.0.0:{config.get('prometheus_port', 9102)}"
                        },
                        "upstreams": upstreams
                    },
                    "checks": [
                        {
                            "name": "Connect Sidecar Listening",
                            "tcp": f"127.0.0.1:{config.get('sidecar_port', 21000)}",
                            "interval": "10s"
                        },
                        {
                            "name": "Connect Sidecar Alive",
                            "alias_service": service_name
                        }
                    ]
                }
            }
        }
    }
    
    return service_def


def main():
    """Generate all service definitions"""
    
    # Create consul services directory if it doesn't exist
    consul_services_dir = Path("consul/services")
    consul_services_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate service definitions
    for service_name, config in SERVICE_CONFIGS.items():
        # Set upstream base port based on service
        config["upstream_base_port"] = config.get("sidecar_port", 21000) - 16000
        
        service_def = generate_service_definition(service_name, config)
        
        # Write service definition to file
        output_file = consul_services_dir / f"{service_name}.json"
        with open(output_file, 'w') as f:
            json.dump(service_def, f, indent=2)
        
        print(f"Generated service definition for {service_name}")
    
    # Generate infrastructure service definitions
    for infra_name, infra_config in INFRA_SERVICES.items():
        infra_def = {
            "service": {
                "name": infra_name,
                "tags": ["infrastructure", infra_config["protocol"]],
                "port": infra_config["port"],
                "meta": {
                    "version": "1.0.0",
                    "protocol": infra_config["protocol"]
                },
                "check": {
                    "id": f"{infra_name}-health",
                    "name": "TCP Check",
                    "tcp": f"localhost:{infra_config['port']}",
                    "interval": "10s",
                    "timeout": "5s"
                }
            }
        }
        
        output_file = consul_services_dir / f"{infra_name}.json"
        with open(output_file, 'w') as f:
            json.dump(infra_def, f, indent=2)
        
        print(f"Generated service definition for {infra_name}")
    
    print(f"\nGenerated {len(SERVICE_CONFIGS) + len(INFRA_SERVICES)} service definitions")


if __name__ == "__main__":
    main() 