# ACL Policies for PlatformQ Services

# Base policy for all services - allows service registration and health checks
agent_prefix "" {
  policy = "read"
}

node_prefix "" {
  policy = "read"
}

service_prefix "" {
  policy = "read"
}

# Market Data Service Policy
service "market-data-service" {
  policy = "write"
}

service "market-data-service-sidecar-proxy" {
  policy = "write"
}

# AMM Service Policy
service "amm-service" {
  policy = "write"
}

service "amm-service-sidecar-proxy" {
  policy = "write"
}

# Social Trading Service Policy
service "social-trading-service" {
  policy = "write"
}

service "social-trading-service-sidecar-proxy" {
  policy = "write"
}

# Options Service Policy
service "options-service" {
  policy = "write"
}

service "options-service-sidecar-proxy" {
  policy = "write"
}

# Futures Service Policy
service "futures-service" {
  policy = "write"
}

service "futures-service-sidecar-proxy" {
  policy = "write"
}

# Risk Service Policy
service "risk-service" {
  policy = "write"
}

service "risk-service-sidecar-proxy" {
  policy = "write"
}

# Key-Value Store Access for Configuration
key_prefix "config/" {
  policy = "read"
}

key_prefix "config/market-data-service/" {
  policy = "write"
}

key_prefix "config/amm-service/" {
  policy = "write"
}

key_prefix "config/social-trading-service/" {
  policy = "write"
}

# Session management for health checks
session_prefix "" {
  policy = "write"
}

# Access to metrics
key_prefix "metrics/" {
  policy = "read"
}

# Service mesh configuration
mesh = "write"

namespace_prefix "" {
  service_prefix "" {
    policy = "read"
  }
  
  node_prefix "" {
    policy = "read"
  }
} 