# Blockchain Smart Contracts

This directory contains all smart contracts used by PlatformQ services, organized by their primary function.

## Directory Structure

```
contracts/
├── credentials/          # Verifiable Credential contracts
│   ├── CredentialRegistry.sol
│   ├── CredentialVerifier.sol
│   └── SoulBoundToken.sol
├── bridges/             # Cross-chain bridge contracts
│   ├── CrossChainBridge.sol
│   └── TokenBridge.sol
├── governance/          # DAO and governance contracts
│   ├── PlatformQGovernor.sol
│   └── VotingPower.sol
├── defi/               # DeFi protocol contracts
│   ├── LendingPool.sol
│   ├── AuctionProtocol.sol
│   └── AMM.sol
├── oracles/            # Oracle contracts
│   └── ReputationOracle.sol
├── libraries/          # Shared contract libraries
│   ├── SafeMath.sol
│   └── AccessControl.sol
└── interfaces/         # Contract interfaces
    ├── ICredentialRegistry.sol
    └── IBridge.sol
```

## Contract Categories

### Credential Contracts
- **CredentialRegistry.sol**: Anchors credential hashes on-chain
- **CredentialVerifier.sol**: Automated credential verification rules
- **SoulBoundToken.sol**: Non-transferable tokens for credentials

### Bridge Contracts
- **CrossChainBridge.sol**: HTLC-based cross-chain transfers
- **TokenBridge.sol**: Token locking and minting across chains

### Governance Contracts
- **PlatformQGovernor.sol**: DAO governance implementation
- **VotingPower.sol**: Voting power calculation and delegation

### DeFi Contracts
- **LendingPool.sol**: Lending and borrowing protocol
- **AuctionProtocol.sol**: Auction mechanisms
- **AMM.sol**: Automated Market Maker

## Deployment

Contracts are deployed using Hardhat:

```bash
# Compile all contracts
npx hardhat compile

# Deploy to local network
npx hardhat run scripts/deploy.js --network localhost

# Deploy to testnet
npx hardhat run scripts/deploy.js --network sepolia
```

## Security

All contracts follow:
- OpenZeppelin standards
- Formal verification where applicable
- Multi-sig deployment
- Upgradeable proxy patterns (where needed)

## Migration from Services

These contracts were previously scattered across various services. They have been consolidated here for:
- Centralized auditing
- Shared deployment scripts
- Consistent upgrade patterns
- Better dependency management 