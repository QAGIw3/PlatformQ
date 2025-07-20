# Verifiable Credential Smart Contracts

This directory contains smart contracts for on-chain verifiable credential operations in the PlatformQ ecosystem.

## Contracts

### CredentialRegistry.sol
Main registry contract for storing and managing verifiable credential metadata on-chain. Features:
- Credential issuance tracking
- Revocation management
- Issuer authorization
- Metadata anchoring

### CredentialVerifier.sol
On-chain verification contract for validating credential proofs and signatures. Features:
- Signature verification
- Proof validation
- Multi-sig verification support
- ZKP verification hooks

### SoulBoundToken.sol
Implementation of non-transferable tokens (SBTs) for identity and credential representation. Features:
- Non-transferable by design
- Revocable by issuer
- Metadata extensions
- Recovery mechanisms

### CrossChainBridge.sol
Bridge contract for cross-chain credential portability. Features:
- Cross-chain message passing
- Credential state synchronization
- Multi-chain registry support
- Relay verification

## Integration

These contracts integrate with:
- **blockchain-connector-service**: For deployment and interaction
- **core-credential-service**: For off-chain credential management
- **zkp-service**: For zero-knowledge proof verification

## Deployment

Contracts are deployed via the blockchain-connector-service which manages:
- Multi-chain deployment
- Gas optimization
- Contract upgrades
- Access control

## Security

All contracts follow security best practices:
- OpenZeppelin standards
- Reentrancy protection
- Access control
- Upgrade safety
- Audit recommendations 