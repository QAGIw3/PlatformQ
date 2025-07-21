# Service Intentions for PlatformQ Service Mesh
# Defines which services can communicate with each other

# Allow market-data-service to access options and futures services
service_intentions = [
  {
    name = "options-service"
    sources = [
      {
        name   = "market-data-service"
        action = "allow"
      },
      {
        name   = "market-making-service"
        action = "allow"
      }
    ]
  },
  {
    name = "futures-service"
    sources = [
      {
        name   = "market-data-service"
        action = "allow"
      },
      {
        name   = "market-making-service"
        action = "allow"
      }
    ]
  }
]

# Allow AMM service to access pricing oracles
service_intentions = [
  {
    name = "oracle-service"
    sources = [
      {
        name   = "market-making-service"
        action = "allow"
      },
      {
        name   = "market-data-service"
        action = "allow"
      }
    ]
  }
]

# Allow social trading to access order matching and risk services
service_intentions = [
  {
    name = "order-matching-service"
    sources = [
      {
        name   = "social-trading-service"
        action = "allow"
      }
    ]
  },
  {
    name = "risk-service"
    sources = [
      {
        name   = "social-trading-service"
        action = "allow"
      },
      {
        name   = "order-matching-service"
        action = "allow"
      }
    ]
  }
]

# Allow social trading to access blockchain and graph services
service_intentions = [
  {
    name = "blockchain-gateway"
    sources = [
      {
        name   = "social-trading-service"
        action = "allow"
      }
    ]
  },
  {
    name = "graph-intelligence"
    sources = [
      {
        name   = "social-trading-service"
        action = "allow"
      },
      {
        name   = "risk-service"
        action = "allow"
      }
    ]
  }
]

# Infrastructure services - allow from all trading services
service_intentions = [
  {
    name = "ignite-cache"
    sources = [
      {
        name   = "*"
        action = "allow"
      }
    ]
  },
  {
    name = "pulsar"
    sources = [
      {
        name   = "*"
        action = "allow"
      }
    ]
  },
  {
    name = "cassandra"
    sources = [
      {
        name   = "market-making-service"
        action = "allow"
      },
      {
        name   = "social-trading-service"
        action = "allow"
      }
    ]
  },
  {
    name = "janusgraph"
    sources = [
      {
        name   = "social-trading-service"
        action = "allow"
      },
      {
        name   = "graph-intelligence"
        action = "allow"
      }
    ]
  }
]

# Default deny all other connections
service_intentions = [
  {
    name = "*"
    sources = [
      {
        name   = "*"
        action = "deny"
      }
    ]
  }
] 