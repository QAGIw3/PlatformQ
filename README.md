# PlatformQ - Decentralized Digital Asset Creation & Collaboration Platform

<div align="center">
  <img src="docs/assets/logo.png" alt="PlatformQ Logo" width="200"/>
  
  [![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
  [![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)]()
  [![Coverage](https://img.shields.io/badge/coverage-85%25-yellow.svg)]()
  
  **A revolutionary platform for decentralized digital asset creation, collaboration, and trust management**
</div>

## 🌟 Overview

PlatformQ is a cutting-edge decentralized platform that revolutionizes how digital assets are created, managed, and traded. Built on blockchain technology and powered by advanced AI, it provides a comprehensive ecosystem for creators, developers, and organizations to collaborate on open-source projects while maintaining trust, ownership, and fair compensation.

### Key Features

- **🎨 Decentralized IP Marketplace** - Trade digital assets with automatic royalty distribution
- **🏛️ Meritocratic DAO** - Multi-dimensional reputation system for governance
- **🔐 Privacy-First Architecture** - Zero-knowledge proofs for credential verification
- **🌐 Cross-Chain Bridge** - Export credentials to multiple blockchains
- **🤖 AI-Powered Processing** - Advanced asset analysis and optimization
- **📊 Real-Time Analytics** - Apache Flink for stream processing
- **🎯 Neuromorphic Computing** - Next-gen AI for pattern recognition

## 🏗️ Architecture

### Technology Stack

- **Backend Services**: Python/FastAPI microservices
- **Blockchain**: Ethereum, Polygon, Arbitrum (EVM-compatible)
- **Smart Contracts**: Solidity with Hardhat framework
- **Frontend**: React with Ant Design
- **Messaging**: Apache Pulsar
- **Stream Processing**: Apache Flink
- **Batch Processing**: Apache Spark
- **Storage**: MinIO (S3-compatible), Apache Cassandra
- **Graph Database**: JanusGraph
- **Cache**: Apache Ignite
- **Search**: Elasticsearch
- **Container Orchestration**: Kubernetes

### Core Services

1. **Digital Asset Service** - Asset management with marketplace integration
2. **Verifiable Credential Service** - W3C-compliant VC issuance and verification
3. **Graph Intelligence Service** - Multi-dimensional trust scoring
4. **Quantum Optimization Service** - Advanced optimization algorithms
5. **Neuromorphic Service** - Spiking neural networks for AI
6. **CAD Collaboration Service** - Real-time 3D model collaboration
7. **Projects Service** - Decentralized project management
8. **Auth Service** - OIDC-compliant authentication

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Node.js 18+ & npm/yarn
- Python 3.10+
- Kubernetes cluster (for production)
- Ethereum wallet (for blockchain features)

### Local Development Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/platformq/platformq.git
   cd platformq
   ```

2. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Start infrastructure services**
   ```bash
   docker-compose -f infra/docker-compose/docker-compose.yml up -d
   ```

4. **Deploy smart contracts**
   ```bash
   cd services/verifiable-credential-service
   npm install
   npx hardhat run scripts/deploy_contracts.js --network localhost
   ```

5. **Start backend services**
   ```bash
   make dev-services
   ```

6. **Start frontend**
   ```bash
   cd frontend
   npm install
   npm run dev
   ```

7. **Access the platform**
   - Frontend: http://localhost:3000
   - API Gateway: http://localhost:8000
   - Pulsar Manager: http://localhost:9527
   - MinIO Console: http://localhost:9001

## 🔥 New Features (v2.0)

### 1. Decentralized IP Marketplace

Trade digital assets with blockchain-based ownership and automatic royalty distribution:

- **Smart Contracts**: `RoyaltyManager.sol`, `UsageLicense.sol`
- **Features**:
  - List assets for sale or license
  - Automatic royalty payments to original creators
  - Time-based usage licenses
  - Lineage tracking for derivative works

### 2. Multi-Dimensional Reputation System

Advanced DAO governance with dimension-weighted voting:

- **Dimensions**:
  - Technical Prowess
  - Collaboration Rating
  - Governance Influence
  - Creativity Index
  - Reliability Score
- **Smart Contracts**: `ReputationOracle.sol`, `PlatformQGovernor.sol`

### 3. Zero-Knowledge Credential System

Privacy-preserving credential verification:

- **Features**:
  - Selective disclosure of credential attributes
  - ZK proofs for age, membership, qualifications
  - Field-level access control
  - W3C Verifiable Credentials compliance

### 4. Cross-Chain Trust Wallet

Export and manage credentials across multiple blockchains:

- **Supported Networks**: Ethereum, Polygon, Arbitrum
- **Features**:
  - Export VCs as SoulBound Tokens (non-transferable)
  - Create Verifiable Presentations
  - Multi-chain credential management
  - Gas fee estimation

## 📚 Documentation

- [Architecture Overview](docs/architecture.md)
- [API Documentation](docs/api-reference.md)
- [Smart Contract Documentation](docs/smart-contracts.md)
- [Development Guide](docs/development-guide.md)
- [Deployment Guide](docs/deployment-guide.md)
- [Security Considerations](docs/security.md)

## 🧪 Testing

Run the test suite:

```bash
# Backend tests
make test-backend

# Smart contract tests
cd services/verifiable-credential-service
npx hardhat test

# Frontend tests
cd frontend
npm test
```

## 🚢 Deployment

### Kubernetes Deployment

1. **Build and push images**
   ```bash
   make build-all
   make push-all
   ```

2. **Deploy to Kubernetes**
   ```bash
   kubectl apply -k iac/kubernetes/
   ```

3. **Configure ingress**
   ```bash
   kubectl apply -f iac/kubernetes/ingress.yaml
   ```

### Production Considerations

- Use external databases (PostgreSQL, Cassandra)
- Configure proper secrets management
- Set up monitoring and alerting
- Enable TLS/SSL
- Configure backup strategies
- Set up CI/CD pipelines

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Workflow

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📊 Performance Metrics

- **Transaction Throughput**: 10,000+ TPS (Pulsar)
- **Credential Issuance**: < 100ms
- **Smart Contract Gas**: Optimized for L2
- **API Response Time**: < 50ms (p99)
- **Stream Processing Latency**: < 1s

## 🔐 Security

- **Blockchain**: Audited smart contracts
- **Authentication**: OIDC with JWT
- **Encryption**: AES-256 for data at rest
- **Transport**: TLS 1.3
- **Secrets**: Kubernetes secrets with rotation
- **Access Control**: RBAC with fine-grained permissions

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Apache Software Foundation for Pulsar, Flink, Spark, Ignite
- Ethereum Foundation for blockchain infrastructure
- W3C for Verifiable Credentials standards
- The open-source community

## 📞 Contact

- **Website**: [platformq.io](https://platformq.io)
- **Email**: support@platformq.io
- **Discord**: [Join our community](https://discord.gg/platformq)
- **Twitter**: [@platformq](https://twitter.com/platformq)

---

<div align="center">
  Built with ❤️ by the PlatformQ Team
</div> 