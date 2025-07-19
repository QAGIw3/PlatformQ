# Default configuration for all services
Kind = "service-defaults"
Name = "*"

Protocol = "http"

# Enable Connect for all services
Connect {
  # Enable sidecar proxy injection
  SidecarService {}
}

# Default upstream configuration
UpstreamConfig {
  Defaults {
    Protocol = "http"
    ConnectTimeout = "5s"
    
    Limits {
      MaxConnections = 100
      MaxPendingRequests = 100
      MaxConcurrentRequests = 100
    }
  }
}

# Enable health checking
HealthCheck {
  Interval = "10s"
  Timeout = "5s"
} 