# Service intentions for credential services

# Core Credential Service can call other credential services
Kind = "service-intentions"
Name = "core-credential-service"
Sources = [
  {
    Name = "api-gateway"
    Action = "allow"
  },
  {
    Name = "presentation-service"
    Action = "allow"
  },
  {
    Name = "sbt-service"
    Action = "allow"
  }
]

# DID Service
Kind = "service-intentions"
Name = "did-service"
Sources = [
  {
    Name = "core-credential-service"
    Action = "allow"
  },
  {
    Name = "presentation-service"
    Action = "allow"
  },
  {
    Name = "zkp-service"
    Action = "allow"
  },
  {
    Name = "api-gateway"
    Action = "allow"
  }
]

# ZKP Service
Kind = "service-intentions"
Name = "zkp-service"
Sources = [
  {
    Name = "core-credential-service"
    Action = "allow"
  },
  {
    Name = "presentation-service"
    Action = "allow"
  },
  {
    Name = "api-gateway"
    Action = "allow"
  }
]

# SBT Service
Kind = "service-intentions"
Name = "sbt-service"
Sources = [
  {
    Name = "core-credential-service"
    Action = "allow"
  },
  {
    Name = "api-gateway"
    Action = "allow"
  }
]

# Presentation Service
Kind = "service-intentions"
Name = "presentation-service"
Sources = [
  {
    Name = "api-gateway"
    Action = "allow"
  }
]

# Key Management Service (used by credential services)
Kind = "service-intentions"
Name = "key-management-service"
Sources = [
  {
    Name = "core-credential-service"
    Action = "allow"
  },
  {
    Name = "did-service"
    Action = "allow"
  },
  {
    Name = "zkp-service"
    Action = "allow"
  }
]

# Blockchain Connector Service (used by credential services)
Kind = "service-intentions"
Name = "blockchain-connector-service"
Sources = [
  {
    Name = "core-credential-service"
    Action = "allow"
  },
  {
    Name = "did-service"
    Action = "allow"
  },
  {
    Name = "sbt-service"
    Action = "allow"
  }
]

# Storage Service (used by credential services)
Kind = "service-intentions"
Name = "storage-service"
Sources = [
  {
    Name = "core-credential-service"
    Action = "allow"
  },
  {
    Name = "sbt-service"
    Action = "allow"
  }
] 