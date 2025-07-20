# Verifiable Credential Service Refactoring Plan

## Overview

This document outlines the comprehensive refactoring of the monolithic verifiable-credential-service into a microservices architecture, leveraging existing blockchain services and Apache Ignite for improved performance and maintainability.

## Current Progress

### ✅ Completed

1. **Shared Libraries**
   - Created `platformq-vc-common` library with:
     - W3C VC data models (VerifiableCredentialModel, VerifiablePresentationModel)
     - Utility functions for credential operations
     - Standards compliance (JSON-LD contexts, proof types)
     - Cryptographic utilities for signing and verification

2. **Core Credential Service (Partial)**
   - Created service structure and configuration
   - Implemented API endpoints for credential operations
   - Integrated with Apache Ignite for caching
   - Set up integration points with other services

### 🚧 In Progress

1. **Core Credential Service**
   - Need to complete core business logic (credential_manager.py)
   - Implement storage layer
   - Add cache management
   - Event publishing

### 📋 Remaining Services

## Service Breakdown

### 1. Core Credential Service ✅ (Partial)
**Purpose**: Handle W3C VC issuance, verification, and management

**Responsibilities**:
- Issue verifiable credentials
- Verify credential signatures and validity
- Manage credential lifecycle (revocation, status)
- Store credential metadata
- Publish credential events

**Integrations**:
- Storage Service (IPFS/MinIO)
- Key Management Service (signing)
- Blockchain Services (anchoring)
- Apache Ignite (caching)

### 2. DID Service 📋
**Purpose**: Manage Decentralized Identifiers

**Responsibilities**:
- Create DIDs (web, key, ethr methods)
- Resolve DIDs to DID documents
- Manage DID keys and verification methods
- Update DID documents

**Integrations**:
- Key Management Service
- Blockchain Services (for did:ethr)

### 3. ZKP Service 📋
**Purpose**: Zero-Knowledge Proof generation and verification

**Responsibilities**:
- Generate ZK proofs (BBS+, range proofs, etc.)
- Verify ZK proofs
- Manage proof circuits
- Distributed proof generation via Ignite

**Key Features**:
- KYC proof generation (age > 18, jurisdiction, etc.)
- AML compliance proofs
- Selective disclosure
- Batch proof generation

**Integrations**:
- Apache Ignite Compute Grid
- Core Credential Service

### 4. SBT Service 📋
**Purpose**: SoulBound Token operations

**Responsibilities**:
- Export credentials as SBTs
- Manage SBT metadata
- Cross-chain SBT transfers
- Track SBT ownership

**Integrations**:
- Transaction Processor Service
- Cross-Chain Bridge Service
- Gas Optimization Service

### 5. Presentation Service 📋
**Purpose**: Verifiable Presentation management

**Responsibilities**:
- Create verifiable presentations
- Verify presentations
- Manage presentation templates
- Handle challenge-response flows

**Integrations**:
- Core Credential Service
- ZKP Service (for selective disclosure)

## Migration Strategy

### Phase 1: Core Services (Current)
1. ✅ Create shared libraries
2. 🚧 Implement Core Credential Service
3. 📋 Implement DID Service

### Phase 2: Advanced Features
1. 📋 Implement ZKP Service with Ignite integration
2. 📋 Implement Presentation Service
3. 📋 Create migration scripts for existing data

### Phase 3: Blockchain Integration
1. 📋 Implement SBT Service
2. 📋 Integrate with existing blockchain services
3. 📋 Move smart contracts to blockchain/contracts

### Phase 4: Cleanup
1. 📋 Deprecate monolithic service
2. 📋 Update all dependent services
3. 📋 Archive old code

## Performance Improvements

### Current Issues
- Synchronous blockchain operations blocking requests
- Heavy cryptographic operations in main thread
- No distributed processing for ZKP generation
- Tight coupling making scaling difficult

### Improvements
1. **Async Operations**: All blockchain and storage operations are async
2. **Distributed Computing**: ZKP generation distributed via Ignite
3. **Caching**: Aggressive caching of credentials and DIDs
4. **Event-Driven**: Eventual consistency with Pulsar events
5. **Service Mesh**: Each service can scale independently

## Integration Points

### Existing Services Used
- **blockchain-connector-service**: Multi-chain connections
- **transaction-processor-service**: Transaction management
- **gas-optimization-service**: Gas price optimization
- **cross-chain-bridge-service**: Cross-chain transfers
- **key-management-service**: Key storage and signing
- **event-monitoring-service**: Blockchain event tracking
- **storage-service**: IPFS/MinIO integration

### New Integration Patterns
1. **Event-Driven**: All state changes published to Pulsar
2. **Service Discovery**: Via Consul
3. **Circuit Breakers**: For resilient service calls
4. **Distributed Tracing**: OpenTelemetry integration

## Configuration Management

Each service has its own configuration with:
- Environment-specific settings
- Feature flags
- Integration endpoints
- Performance tuning parameters

Configurations are managed through:
- Consul for dynamic configuration
- Vault for secrets
- Environment variables for deployment

## Testing Strategy

1. **Unit Tests**: Each service has comprehensive unit tests
2. **Integration Tests**: Test service interactions
3. **Contract Tests**: Ensure API compatibility
4. **Performance Tests**: Benchmark improvements
5. **Migration Tests**: Validate data migration

## Rollout Plan

1. **Development Environment**: Deploy all services in parallel with old service
2. **Staging**: Run side-by-side with traffic splitting
3. **Production**: Gradual rollout with feature flags
4. **Rollback Plan**: Keep old service available for 30 days

## Success Metrics

- **Performance**: 
  - Credential issuance < 50ms (from 100ms)
  - ZKP generation < 100ms distributed (from 500ms)
  - Verification < 30ms (from 50ms)

- **Scalability**:
  - Each service can scale independently
  - Linear scaling with Ignite nodes
  - Support for 10x current load

- **Maintainability**:
  - Clear service boundaries
  - Independent deployments
  - Reduced coupling

## Next Steps

1. Complete core-credential-service implementation
2. Create DID service
3. Implement ZKP service with Ignite compute grid
4. Create comprehensive integration tests
5. Document API changes for consumers 