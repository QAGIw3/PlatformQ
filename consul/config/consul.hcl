# Consul Server Configuration for PlatformQ Service Mesh

# Data directory
data_dir = "/consul/data"

# Server mode
server = true
bootstrap_expect = 3

# Datacenter
datacenter = "platformq-dc1"

# Client address (bind to all interfaces)
client_addr = "0.0.0.0"

# UI enabled
ui_config {
  enabled = true
}

# Connect configuration (Service Mesh)
connect {
  enabled = true
  
  # Enable built-in CA
  ca_provider = "consul"
  
  ca_config {
    # CA certificate TTL
    leaf_cert_ttl = "72h"
    
    # Root certificate TTL  
    root_cert_ttl = "87600h"  # 10 years
    
    # Intermediate certificate TTL
    intermediate_cert_ttl = "8760h"  # 1 year
    
    # Private key type and bits
    private_key_type = "ec"
    private_key_bits = 256
  }
}

# ACL configuration
acl {
  enabled = true
  default_policy = "deny"
  enable_token_persistence = true
  
  tokens {
    # Master token should be set via environment variable
    # master = "YOUR_MASTER_TOKEN"
    
    # Default agent token
    # agent = "YOUR_AGENT_TOKEN"
  }
}

# Performance tuning
performance {
  # RPC hold timeout
  rpc_hold_timeout = "7s"
  
  # Raft multiplier for timing
  raft_multiplier = 1
}

# Telemetry configuration
telemetry {
  # StatsD compatible metrics
  statsd_address = "127.0.0.1:8125"
  
  # Prometheus metrics
  prometheus_retention_time = "24h"
  disable_hostname = true
  
  # Metrics prefix
  metrics_prefix = "consul"
}

# Enable script checks (for health checks)
enable_script_checks = true

# Enable local script checks
enable_local_script_checks = true

# DNS configuration
dns_config {
  # Allow stale reads for better performance
  allow_stale = true
  max_stale = "10s"
  
  # DNS caching
  use_cache = true
  cache_max_age = "60s"
}

# Limits configuration
limits {
  # HTTP request limits
  http_max_conns_per_client = 200
  
  # RPC rate limiting
  rpc_rate = -1
  rpc_max_burst = 1000
}

# Autopilot configuration for automated operator tasks
autopilot {
  # Automatic removal of dead servers
  cleanup_dead_servers = true
  
  # Server health checking
  last_contact_threshold = "200ms"
  max_trailing_logs = 250
  min_quorum = 3
  
  # Server stabilization time
  server_stabilization_time = "10s"
}

# Watches for configuration changes
watches = [
  {
    type = "services"
    handler_type = "http"
    http_handler_config {
      path = "http://localhost:8500/v1/health/service/"
      method = "GET"
      timeout = "10s"
    }
  }
]

# Gossip encryption (set via environment variable)
# encrypt = "YOUR_GOSSIP_KEY"

# TLS configuration for agent communication
# tls {
#   defaults {
#     verify_incoming = true
#     verify_outgoing = true
#     ca_file = "/consul/config/consul-ca.pem"
#     cert_file = "/consul/config/consul-cert.pem"
#     key_file = "/consul/config/consul-key.pem"
#   }
# } 