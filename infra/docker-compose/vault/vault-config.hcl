ui = true

listener "tcp" {
  address     = "0.0.0.0:8200"
  tls_disable = "true"
}

storage "consul" {
  address = "consul:8500"
  path    = "vault/"
}

api_addr = "http://vault:8200"
cluster_addr = "https://vault:8201"

max_lease_ttl = "10h"
default_lease_ttl = "1h"

# Enable audit logging
audit {
  enabled = true
}

# Performance tuning
cache_size = 131072

# Telemetry configuration
telemetry {
  prometheus_retention_time = "30s"
  disable_hostname = true
} 