# Consul Client Configuration for PlatformQ Services

# Data directory
data_dir = "/consul/data"

# Client mode
server = false

# Datacenter
datacenter = "platformq-dc1"

# Client address
client_addr = "0.0.0.0"

# Retry join the Consul servers
retry_join = ["consul-server-1", "consul-server-2", "consul-server-3"]

# Connect configuration (Service Mesh)
connect {
  enabled = true
}

# Ports configuration
ports {
  grpc = 8502
  http = 8500
  https = -1
  dns = 8600
  serf_lan = 8301
  serf_wan = -1
  server = 8300
}

# ACL configuration
acl {
  enabled = true
  default_policy = "deny"
  enable_token_persistence = true
  
  tokens {
    # Agent token should be provided via environment
    # agent = "YOUR_AGENT_TOKEN"
  }
}

# Telemetry
telemetry {
  prometheus_retention_time = "24h"
  disable_hostname = true
  metrics_prefix = "consul_client"
}

# Service defaults
services {
  # Default service check interval
  check_update_interval = "5s"
}

# Performance
performance {
  rpc_hold_timeout = "7s"
}

# Enable script checks
enable_script_checks = true
enable_local_script_checks = true

# DNS configuration
dns_config {
  allow_stale = true
  max_stale = "10s"
  use_cache = true
  cache_max_age = "60s"
}

# Log level
log_level = "INFO"
log_json = true

# Disable update checks
disable_update_check = true

# Node metadata
node_meta {
  platform = "platformq"
  environment = "production"
} 