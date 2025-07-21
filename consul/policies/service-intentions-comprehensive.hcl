# Comprehensive Service Intentions for PlatformQ Service Mesh
# Defines which services can communicate with each other

# Core Infrastructure Access
# Allow all services to access infrastructure components
Kind = "service-intentions"
Name = "ignite-cache"
Sources = [
  {
    Name        = "*"
    Permissions = [
      {
        Action = "allow"
        HTTP {
          PathPrefix = "/"
        }
      }
    ]
  }
]
---
Kind = "service-intentions"
Name = "pulsar"
Sources = [
  {
    Name        = "*"
    Permissions = [
      {
        Action = "allow"
      }
    ]
  }
]
---
Kind = "service-intentions"
Name = "vault"
Sources = [
  {
    Name = "auth-service"
    Action = "allow"
  },
  {
    Name = "blockchain-gateway-service"
    Action = "allow"
  },
  {
    Name = "security-service"
    Action = "allow"
  },
  {
    Name = "provisioning-service"
    Action = "allow"
  },
  {
    Name = "storage-service"
    Action = "allow"
  }
]
---
# Auth Service - Central authentication/authorization
Kind = "service-intentions"
Name = "auth-service"
Sources = [
  {
    Name = "*"
    Action = "allow"
    Description = "All services need authentication"
  }
]
---
# Data Platform Access
Kind = "service-intentions"
Name = "cassandra"
Sources = [
  {
    Name = "market-making-service"
    Action = "allow"
  },
  {
    Name = "social-trading-service"
    Action = "allow"
  },
  {
    Name = "analytics-service"
    Action = "allow"
  },
  {
    Name = "compliance-service"
    Action = "allow"
  },
  {
    Name = "data-platform-service"
    Action = "allow"
  },
  {
    Name = "derivatives-engine-service"
    Action = "allow"
  },
  {
    Name = "market-data-service"
    Action = "allow"
  },
  {
    Name = "order-matching-service"
    Action = "allow"
  },
  {
    Name = "trading-platform-service"
    Action = "allow"
  },
  {
    Name = "mlflow-server"
    Action = "allow"
  },
  {
    Name = "digital-asset-service"
    Action = "allow"
  },
  {
    Name = "verifiable-credential-service"
    Action = "allow"
  }
]
---
Kind = "service-intentions"
Name = "elasticsearch"
Sources = [
  {
    Name = "search-service"
    Action = "allow"
  },
  {
    Name = "analytics-service"
    Action = "allow"
  },
  {
    Name = "data-platform-service"
    Action = "allow"
  },
  {
    Name = "graph-intelligence-service"
    Action = "allow"
  },
  {
    Name = "security-service"
    Action = "allow"
  }
]
---
Kind = "service-intentions"
Name = "minio"
Sources = [
  {
    Name = "storage-service"
    Action = "allow"
  },
  {
    Name = "analytics-service"
    Action = "allow"
  },
  {
    Name = "data-platform-service"
    Action = "allow"
  },
  {
    Name = "connector-service"
    Action = "allow"
  },
  {
    Name = "dataset-marketplace"
    Action = "allow"
  },
  {
    Name = "digital-asset-service"
    Action = "allow"
  },
  {
    Name = "unified-ml-platform-service"
    Action = "allow"
  },
  {
    Name = "mlflow-server"
    Action = "allow"
  }
]
---
Kind = "service-intentions"
Name = "janusgraph"
Sources = [
  {
    Name = "graph-intelligence-service"
    Action = "allow"
  },
  {
    Name = "social-trading-service"
    Action = "allow"
  },
  {
    Name = "data-platform-service"
    Action = "allow"
  }
]
---
Kind = "service-intentions"
Name = "opa"
Sources = [
  {
    Name = "security-service"
    Action = "allow"
  }
]
---
# Service-to-Service Communications
# Blockchain Gateway dependencies
Kind = "service-intentions"
Name = "blockchain-gateway-service"
Sources = [
  {
    Name = "social-trading-service"
    Action = "allow"
  },
  {
    Name = "dataset-marketplace"
    Action = "allow"
  },
  {
    Name = "defi-protocol-service"
    Action = "allow"
  },
  {
    Name = "digital-asset-service"
    Action = "allow"
  },
  {
    Name = "governance-service"
    Action = "allow"
  },
  {
    Name = "insurance-pool-service"
    Action = "allow"
  },
  {
    Name = "verifiable-credential-service"
    Action = "allow"
  },
  {
    Name = "compliance-service"
    Action = "allow"
  }
]
---
# Market Data Service access
Kind = "service-intentions"
Name = "market-data-service"
Sources = [
  {
    Name = "market-making-service"
    Action = "allow"
  },
  {
    Name = "derivatives-engine-service"
    Action = "allow"
  },
  {
    Name = "futures-service"
    Action = "allow"
  },
  {
    Name = "options-service"
    Action = "allow"
  },
  {
    Name = "order-matching-service"
    Action = "allow"
  },
  {
    Name = "risk-engine-service"
    Action = "allow"
  },
  {
    Name = "risk-management-service"
    Action = "allow"
  },
  {
    Name = "structured-products-service"
    Action = "allow"
  },
  {
    Name = "trading-platform-service"
    Action = "allow"
  }
]
---
# Order Matching Service access
Kind = "service-intentions"
Name = "order-matching-service"
Sources = [
  {
    Name = "trading-platform-service"
    Action = "allow"
  },
  {
    Name = "social-trading-service"
    Action = "allow"
  }
]
---
# Risk Management Service access
Kind = "service-intentions"
Name = "risk-management-service"
Sources = [
  {
    Name = "derivatives-engine-service"
    Action = "allow"
  },
  {
    Name = "insurance-pool-service"
    Action = "allow"
  },
  {
    Name = "order-matching-service"
    Action = "allow"
  },
  {
    Name = "trading-platform-service"
    Action = "allow"
  }
]
---
Kind = "service-intentions"
Name = "risk-engine-service"
Sources = [
  {
    Name = "risk-management-service"
    Action = "allow"
  }
]
---
# Data Platform Service access
Kind = "service-intentions"
Name = "data-platform-service"
Sources = [
  {
    Name = "analytics-service"
    Action = "allow"
  },
  {
    Name = "connector-service"
    Action = "allow"
  },
  {
    Name = "graph-intelligence-service"
    Action = "allow"
  },
  {
    Name = "search-service"
    Action = "allow"
  }
]
---
# Compute Allocation Service access
Kind = "service-intentions"
Name = "compute-allocation-service"
Sources = [
  {
    Name = "functions-service"
    Action = "allow"
  },
  {
    Name = "provisioning-service"
    Action = "allow"
  },
  {
    Name = "quantum-optimization-service"
    Action = "allow"
  },
  {
    Name = "unified-ml-platform-service"
    Action = "allow"
  },
  {
    Name = "workflow-service"
    Action = "allow"
  }
]
---
# Provisioning Service access
Kind = "service-intentions"
Name = "provisioning-service"
Sources = [
  {
    Name = "compute-allocation-service"
    Action = "allow"
  }
]
---
# Event Router Service access
Kind = "service-intentions"
Name = "event-router-service"
Sources = [
  {
    Name = "workflow-service"
    Action = "allow"
  },
  {
    Name = "collaboration-platform-service"
    Action = "allow"
  }
]
---
# Storage Service access
Kind = "service-intentions"
Name = "storage-service"
Sources = [
  {
    Name = "dataset-marketplace"
    Action = "allow"
  },
  {
    Name = "digital-asset-service"
    Action = "allow"
  },
  {
    Name = "functions-service"
    Action = "allow"
  },
  {
    Name = "mlflow-server"
    Action = "allow"
  },
  {
    Name = "unified-ml-platform-service"
    Action = "allow"
  },
  {
    Name = "verifiable-credential-service"
    Action = "allow"
  },
  {
    Name = "collaboration-platform-service"
    Action = "allow"
  }
]
---
# Connector Service access
Kind = "service-intentions"
Name = "connector-service"
Sources = [
  {
    Name = "data-platform-service"
    Action = "allow"
  }
]
---
# Graph Intelligence Service access
Kind = "service-intentions"
Name = "graph-intelligence-service"
Sources = [
  {
    Name = "compliance-service"
    Action = "allow"
  },
  {
    Name = "governance-service"
    Action = "allow"
  },
  {
    Name = "risk-engine-service"
    Action = "allow"
  },
  {
    Name = "blockchain-gateway-service"
    Action = "allow"
  }
]
---
# Compliance Service access
Kind = "service-intentions"
Name = "compliance-service"
Sources = [
  {
    Name = "auth-service"
    Action = "allow"
  },
  {
    Name = "blockchain-gateway-service"
    Action = "allow"
  }
]
---
# Security Service access
Kind = "service-intentions"
Name = "security-service"
Sources = [
  {
    Name = "auth-service"
    Action = "allow"
  }
]
---
# AMM Service access
Kind = "service-intentions"
Name = "market-making-service"
Sources = [
  {
    Name = "defi-protocol-service"
    Action = "allow"
  }
]
---
# Derivatives Engine Service access
Kind = "service-intentions"
Name = "derivatives-engine-service"
Sources = [
  {
    Name = "futures-service"
    Action = "allow"
  },
  {
    Name = "options-service"
    Action = "allow"
  },
  {
    Name = "structured-products-service"
    Action = "allow"
  }
]
---
# MLflow Server access
Kind = "service-intentions"
Name = "mlflow-server"
Sources = [
  {
    Name = "unified-ml-platform-service"
    Action = "allow"
  }
]
---
# Unified ML Platform Service access
Kind = "service-intentions"
Name = "unified-ml-platform-service"
Sources = [
  {
    Name = "quantum-optimization-service"
    Action = "allow"
  }
]
---
# Workflow Service access
Kind = "service-intentions"
Name = "workflow-service"
Sources = [
  {
    Name = "compute-allocation-service"
    Action = "allow"
  }
]
---
# Default deny all other connections
Kind = "service-intentions"
Name = "*"
Sources = [
  {
    Name   = "*"
    Action = "deny"
    Description = "Default deny for all unspecified connections"
  }
] 