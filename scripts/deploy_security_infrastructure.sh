#!/bin/bash

# Deploy Security Infrastructure for PlatformQ
# This script deploys and initializes Vault, Consul, Kong, and OPA

set -e

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║       PlatformQ Security Infrastructure Deployment           ║"
echo "║         Vault + Consul + Kong + OPA + Service Mesh          ║"
echo "╚══════════════════════════════════════════════════════════════╝"

# Configuration
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DOCKER_COMPOSE_DIR="$PROJECT_ROOT/infra/docker-compose"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed"
        exit 1
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose is not installed"
        exit 1
    fi
    
    # Check jq for JSON processing
    if ! command -v jq &> /dev/null; then
        log_warning "jq is not installed. Installing..."
        if [[ "$OSTYPE" == "darwin"* ]]; then
            brew install jq
        else
            sudo apt-get update && sudo apt-get install -y jq
        fi
    fi
    
    log_info "Prerequisites satisfied ✓"
}

# Deploy core infrastructure
deploy_core_infrastructure() {
    log_info "Deploying core infrastructure..."
    
    cd "$DOCKER_COMPOSE_DIR"
    
    # Start Vault and Consul first
    docker-compose -f docker-compose.yml up -d vault consul
    
    # Wait for services to be healthy
    log_info "Waiting for Vault and Consul to be ready..."
    sleep 10
    
    # Check Vault status
    until docker exec platformq-vault vault status &> /dev/null; do
        log_info "Waiting for Vault..."
        sleep 2
    done
    
    # Check Consul status
    until docker exec platformq-consul consul members &> /dev/null; do
        log_info "Waiting for Consul..."
        sleep 2
    done
    
    log_info "Core infrastructure deployed ✓"
}

# Initialize Vault
initialize_vault() {
    log_info "Initializing Vault..."
    
    # Check if already initialized
    if docker exec platformq-vault vault status 2>&1 | grep -q "Initialized.*true"; then
        log_info "Vault already initialized"
        return
    fi
    
    # Initialize Vault with 5 key shares and 3 key threshold
    INIT_OUTPUT=$(docker exec platformq-vault vault operator init \
        -key-shares=5 \
        -key-threshold=3 \
        -format=json)
    
    # Save keys securely
    echo "$INIT_OUTPUT" > "$PROJECT_ROOT/.vault-init.json"
    chmod 600 "$PROJECT_ROOT/.vault-init.json"
    
    log_warning "Vault initialization keys saved to .vault-init.json"
    log_warning "IMPORTANT: Back up this file securely and remove from the server!"
    
    # Extract unseal keys and root token
    UNSEAL_KEY_1=$(echo "$INIT_OUTPUT" | jq -r '.unseal_keys_b64[0]')
    UNSEAL_KEY_2=$(echo "$INIT_OUTPUT" | jq -r '.unseal_keys_b64[1]')
    UNSEAL_KEY_3=$(echo "$INIT_OUTPUT" | jq -r '.unseal_keys_b64[2]')
    ROOT_TOKEN=$(echo "$INIT_OUTPUT" | jq -r '.root_token')
    
    # Unseal Vault
    log_info "Unsealing Vault..."
    docker exec platformq-vault vault operator unseal "$UNSEAL_KEY_1"
    docker exec platformq-vault vault operator unseal "$UNSEAL_KEY_2"
    docker exec platformq-vault vault operator unseal "$UNSEAL_KEY_3"
    
    # Login with root token
    docker exec platformq-vault vault login "$ROOT_TOKEN"
    
    log_info "Vault initialized and unsealed ✓"
}

# Configure Vault
configure_vault() {
    log_info "Configuring Vault..."
    
    # Get root token
    ROOT_TOKEN=$(cat "$PROJECT_ROOT/.vault-init.json" | jq -r '.root_token')
    
    # Configure audit logging
    docker exec -e VAULT_TOKEN="$ROOT_TOKEN" platformq-vault \
        vault audit enable file file_path=/vault/logs/audit.log
    
    # Enable required secret engines
    docker exec -e VAULT_TOKEN="$ROOT_TOKEN" platformq-vault \
        vault secrets enable -path=secret kv-v2
    
    docker exec -e VAULT_TOKEN="$ROOT_TOKEN" platformq-vault \
        vault secrets enable transit
    
    docker exec -e VAULT_TOKEN="$ROOT_TOKEN" platformq-vault \
        vault secrets enable pki
    
    docker exec -e VAULT_TOKEN="$ROOT_TOKEN" platformq-vault \
        vault secrets enable database
    
    # Configure PKI for internal certificates
    docker exec -e VAULT_TOKEN="$ROOT_TOKEN" platformq-vault \
        vault secrets tune -max-lease-ttl=87600h pki
    
    # Generate root certificate
    docker exec -e VAULT_TOKEN="$ROOT_TOKEN" platformq-vault \
        vault write -field=certificate pki/root/generate/internal \
        common_name="PlatformQ Root CA" \
        ttl=87600h > "$PROJECT_ROOT/ca_cert.crt"
    
    # Configure PKI URLs
    docker exec -e VAULT_TOKEN="$ROOT_TOKEN" platformq-vault \
        vault write pki/config/urls \
        issuing_certificates="http://vault:8200/v1/pki/ca" \
        crl_distribution_points="http://vault:8200/v1/pki/crl"
    
    # Create role for internal services
    docker exec -e VAULT_TOKEN="$ROOT_TOKEN" platformq-vault \
        vault write pki/roles/internal-services \
        allowed_domains="platformq.internal,service.consul" \
        allow_subdomains=true \
        max_ttl="720h"
    
    # Enable AppRole auth method for services
    docker exec -e VAULT_TOKEN="$ROOT_TOKEN" platformq-vault \
        vault auth enable approle
    
    # Create policies for services
    create_vault_policies
    
    log_info "Vault configured ✓"
}

# Create Vault policies
create_vault_policies() {
    log_info "Creating Vault policies..."
    
    ROOT_TOKEN=$(cat "$PROJECT_ROOT/.vault-init.json" | jq -r '.root_token')
    
    # Auth service policy
    cat <<EOF | docker exec -i -e VAULT_TOKEN="$ROOT_TOKEN" platformq-vault \
        vault policy write auth-service -
path "secret/data/auth-service/*" {
  capabilities = ["read", "list"]
}

path "transit/encrypt/auth-service-*" {
  capabilities = ["update"]
}

path "transit/decrypt/auth-service-*" {
  capabilities = ["update"]
}

path "transit/keys/auth-service-*" {
  capabilities = ["create", "read", "update"]
}

path "pki/issue/internal-services" {
  capabilities = ["update"]
}
EOF
    
    # Create AppRole for auth service
    docker exec -e VAULT_TOKEN="$ROOT_TOKEN" platformq-vault \
        vault write auth/approle/role/auth-service \
        token_policies="auth-service" \
        token_ttl=1h \
        token_max_ttl=4h
    
    # Get role ID and secret ID for auth service
    ROLE_ID=$(docker exec -e VAULT_TOKEN="$ROOT_TOKEN" platformq-vault \
        vault read -field=role_id auth/approle/role/auth-service/role-id)
    
    SECRET_ID=$(docker exec -e VAULT_TOKEN="$ROOT_TOKEN" platformq-vault \
        vault write -field=secret_id -f auth/approle/role/auth-service/secret-id)
    
    # Save credentials for auth service
    cat <<EOF > "$PROJECT_ROOT/.auth-service-vault-creds.env"
VAULT_ADDR=http://vault:8200
VAULT_ROLE_ID=$ROLE_ID
VAULT_SECRET_ID=$SECRET_ID
EOF
    
    log_info "Vault policies created ✓"
}

# Configure Consul
configure_consul() {
    log_info "Configuring Consul..."
    
    # Bootstrap ACL system
    if ! docker exec platformq-consul consul acl bootstrap &> /dev/null; then
        log_info "Consul ACL already bootstrapped"
    else
        ACL_OUTPUT=$(docker exec platformq-consul consul acl bootstrap -format=json)
        echo "$ACL_OUTPUT" > "$PROJECT_ROOT/.consul-bootstrap.json"
        chmod 600 "$PROJECT_ROOT/.consul-bootstrap.json"
        log_warning "Consul ACL bootstrap token saved to .consul-bootstrap.json"
    fi
    
    # Get management token
    CONSUL_TOKEN=$(cat "$PROJECT_ROOT/.consul-bootstrap.json" | jq -r '.SecretID')
    
    # Create service defaults
    docker exec platformq-consul consul config write -token="$CONSUL_TOKEN" - <<EOF
Kind = "service-defaults"
Name = "*"
Protocol = "http"
Connect {
  SidecarProxy {
    Config {
      handshake_timeout_ms = 10000
    }
  }
}
EOF
    
    # Create intentions for service communication
    docker exec platformq-consul consul intention create -token="$CONSUL_TOKEN" \
        -allow auth-service "*"
    
    # Initialize KV store with default configurations
    docker exec platformq-consul consul kv put -token="$CONSUL_TOKEN" \
        "services/auth-service/config/rate-limits/login-attempts" "5"
    
    docker exec platformq-consul consul kv put -token="$CONSUL_TOKEN" \
        "services/auth-service/config/session/timeout-minutes" "30"
    
    docker exec platformq-consul consul kv put -token="$CONSUL_TOKEN" \
        "services/auth-service/config/features/oauth-enabled" "true"
    
    log_info "Consul configured ✓"
}

# Deploy security stack
deploy_security_stack() {
    log_info "Deploying security stack..."
    
    cd "$DOCKER_COMPOSE_DIR"
    
    # Deploy Kong, OPA, and other security components
    docker-compose -f docker-compose.security.yml up -d
    
    # Wait for services to be ready
    sleep 20
    
    # Configure Kong
    configure_kong
    
    # Configure OPA
    configure_opa
    
    log_info "Security stack deployed ✓"
}

# Configure Kong API Gateway
configure_kong() {
    log_info "Configuring Kong API Gateway..."
    
    # Wait for Kong to be ready
    until curl -s http://localhost:8001 &> /dev/null; do
        log_info "Waiting for Kong admin API..."
        sleep 2
    done
    
    # Add auth service upstream
    curl -X POST http://localhost:8001/upstreams \
        -H "Content-Type: application/json" \
        -d '{
            "name": "auth-service-upstream",
            "slots": 100,
            "healthchecks": {
                "active": {
                    "healthy": {
                        "interval": 10
                    }
                }
            }
        }'
    
    # Add auth service target
    curl -X POST http://localhost:8001/upstreams/auth-service-upstream/targets \
        -H "Content-Type: application/json" \
        -d '{"target": "auth-service:8000", "weight": 100}'
    
    # Create service
    curl -X POST http://localhost:8001/services \
        -H "Content-Type: application/json" \
        -d '{
            "name": "auth-service",
            "host": "auth-service-upstream",
            "port": 80,
            "protocol": "http",
            "path": "/",
            "retries": 3,
            "connect_timeout": 60000,
            "write_timeout": 60000,
            "read_timeout": 60000
        }'
    
    # Create route
    curl -X POST http://localhost:8001/services/auth-service/routes \
        -H "Content-Type: application/json" \
        -d '{
            "name": "auth-route",
            "paths": ["/auth"],
            "strip_path": true,
            "preserve_host": false
        }'
    
    # Enable rate limiting plugin
    curl -X POST http://localhost:8001/services/auth-service/plugins \
        -H "Content-Type: application/json" \
        -d '{
            "name": "rate-limiting",
            "config": {
                "minute": 100,
                "hour": 1000,
                "policy": "local"
            }
        }'
    
    log_info "Kong configured ✓"
}

# Configure OPA
configure_opa() {
    log_info "Configuring OPA..."
    
    # Create initial policies bundle
    mkdir -p "$PROJECT_ROOT/opa-policies"
    
    cat <<'EOF' > "$PROJECT_ROOT/opa-policies/authz.rego"
package platformq.authz

default allow = false

# Allow health checks
allow {
    input.path == "/health"
}

# Allow authenticated users to access their own data
allow {
    input.method == "GET"
    input.path == sprintf("/api/v1/users/%s", [input.user.id])
    input.user.id == input.path_params.user_id
}

# Allow admins to access all user data
allow {
    input.user.roles[_] == "admin"
    startswith(input.path, "/api/v1/users")
}

# Service-to-service communication
allow {
    input.service_name != ""
    input.service_token != ""
    valid_service_token
}

valid_service_token {
    # Verify service token with Vault
    # This would be implemented with actual Vault integration
    input.service_token != ""
}
EOF
    
    # Upload policy bundle
    cd "$PROJECT_ROOT/opa-policies"
    tar -czf bundle.tar.gz *.rego
    
    # Start simple HTTP server for bundle
    python3 -m http.server 8081 &
    BUNDLE_SERVER_PID=$!
    
    sleep 2
    
    # Configure OPA to use bundle
    curl -X PUT http://localhost:8181/v1/config \
        -H "Content-Type: application/json" \
        -d '{
            "bundles": {
                "authz": {
                    "resource": "/bundle.tar.gz",
                    "service": "bundle-server",
                    "persist": true,
                    "polling": {
                        "min_delay_seconds": 10,
                        "max_delay_seconds": 20
                    }
                }
            },
            "services": {
                "bundle-server": {
                    "url": "http://host.docker.internal:8081"
                }
            }
        }'
    
    # Stop bundle server
    kill $BUNDLE_SERVER_PID
    
    log_info "OPA configured ✓"
}

# Deploy example services
deploy_example_services() {
    log_info "Deploying example services with security integration..."
    
    cd "$PROJECT_ROOT"
    
    # Build auth service with Vault/Consul integration
    docker build -t platformq/auth-service:secure -f services/auth-service/Dockerfile services/auth-service/
    
    # Run auth service with security integration
    docker run -d \
        --name auth-service-secure \
        --network platformq-network \
        --env-file .auth-service-vault-creds.env \
        -e CONSUL_HOST=consul \
        -e CONSUL_PORT=8500 \
        -p 8010:8000 \
        platformq/auth-service:secure
    
    log_info "Example services deployed ✓"
}

# Generate summary report
generate_summary() {
    log_info "Generating deployment summary..."
    
    cat <<EOF > "$PROJECT_ROOT/security-deployment-summary.txt"
╔══════════════════════════════════════════════════════════════╗
║          PlatformQ Security Infrastructure Summary           ║
╚══════════════════════════════════════════════════════════════╝

DEPLOYMENT STATUS: ✓ Complete

SERVICES DEPLOYED:
- Vault (Secret Management): http://localhost:8200
- Consul (Service Discovery): http://localhost:8500
- Kong (API Gateway): http://localhost:8000
- OPA (Policy Engine): http://localhost:8181

IMPORTANT FILES:
- Vault Init Keys: .vault-init.json (SECURE THIS!)
- Consul Bootstrap: .consul-bootstrap.json (SECURE THIS!)
- Auth Service Creds: .auth-service-vault-creds.env

NEXT STEPS:
1. Secure the initialization files
2. Set up monitoring
3. Configure backups
4. Deploy remaining services
5. Enable mTLS everywhere

ACCESS URLS:
- Vault UI: http://localhost:8200/ui
- Consul UI: http://localhost:8500/ui
- Kong Admin: http://localhost:8001
- Example Auth Service: http://localhost:8010

TESTING:
# Test auth service health
curl http://localhost:8010/health

# Test through Kong
curl http://localhost:8000/auth/health

DOCUMENTATION:
- Integration Guides: docs/integration-guides/
- Deployment Guide: docs/security/ZERO_TRUST_DEPLOYMENT_GUIDE.md
EOF
    
    cat "$PROJECT_ROOT/security-deployment-summary.txt"
}

# Main deployment flow
main() {
    log_info "Starting PlatformQ Security Infrastructure deployment..."
    
    check_prerequisites
    deploy_core_infrastructure
    initialize_vault
    configure_vault
    configure_consul
    deploy_security_stack
    deploy_example_services
    generate_summary
    
    log_info "✅ Security infrastructure deployment complete!"
    log_warning "⚠️  Remember to secure the initialization files!"
}

# Run main function
main "$@" 