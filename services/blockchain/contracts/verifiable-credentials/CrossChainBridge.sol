// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/utils/cryptography/ECDSA.sol";

/**
 * @title CrossChainCredentialBridge
 * @dev Manages cross-chain credential transfers with cryptographic proofs
 */
contract CrossChainCredentialBridge is Ownable, ReentrancyGuard {
    using ECDSA for bytes32;

    enum TransferStatus {
        PENDING,
        LOCKED,
        COMPLETED,
        FAILED,
        REFUNDED
    }

    struct BridgeTransfer {
        bytes32 credentialHash;
        address initiator;
        string sourceChain;
        string targetChain;
        uint256 lockTime;
        uint256 unlockTime;
        TransferStatus status;
        bytes32 secretHash;
        bytes secret;
    }

    // Mapping from transfer ID to transfer details
    mapping(bytes32 => BridgeTransfer) public transfers;
    
    // Mapping to track used proofs to prevent replay attacks
    mapping(bytes32 => bool) public usedProofs;
    
    // Trusted bridge operators for different chains
    mapping(string => address) public bridgeOperators;
    
    // Lock duration for transfers (default 24 hours)
    uint256 public lockDuration = 24 hours;
    
    // Events
    event TransferInitiated(
        bytes32 indexed transferId,
        bytes32 indexed credentialHash,
        string sourceChain,
        string targetChain,
        address initiator
    );
    
    event TransferLocked(
        bytes32 indexed transferId,
        bytes32 secretHash,
        uint256 unlockTime
    );
    
    event TransferCompleted(
        bytes32 indexed transferId,
        bytes secret
    );
    
    event TransferRefunded(
        bytes32 indexed transferId
    );
    
    event BridgeOperatorUpdated(
        string chain,
        address operator
    );

    constructor() {
        // The deployer is the initial owner.
    }

    /**
     * @dev Set bridge operator for a specific chain
     */
    function setBridgeOperator(string memory chain, address operator) external onlyOwner {
        bridgeOperators[chain] = operator;
        emit BridgeOperatorUpdated(chain, operator);
    }

    /**
     * @dev Initiate a cross-chain credential transfer
     */
    function initiateTransfer(
        bytes32 credentialHash,
        string memory targetChain,
        bytes32 secretHash
    ) external nonReentrant returns (bytes32 transferId) {
        require(bytes(targetChain).length > 0, "Invalid target chain");
        require(bridgeOperators[targetChain] != address(0), "No operator for target chain");
        
        transferId = keccak256(
            abi.encodePacked(
                credentialHash,
                msg.sender,
                block.chainid,
                targetChain,
                block.timestamp
            )
        );
        
        require(transfers[transferId].lockTime == 0, "Transfer already exists");
        
        transfers[transferId] = BridgeTransfer({
            credentialHash: credentialHash,
            initiator: msg.sender,
            sourceChain: getChainName(block.chainid),
            targetChain: targetChain,
            lockTime: block.timestamp,
            unlockTime: block.timestamp + lockDuration,
            status: TransferStatus.LOCKED,
            secretHash: secretHash,
            secret: ""
        });
        
        emit TransferInitiated(
            transferId,
            credentialHash,
            getChainName(block.chainid),
            targetChain,
            msg.sender
        );
        
        emit TransferLocked(
            transferId,
            secretHash,
            block.timestamp + lockDuration
        );
    }

    /**
     * @dev Complete transfer by revealing the secret
     */
    function completeTransfer(
        bytes32 transferId,
        bytes memory secret
    ) external nonReentrant {
        BridgeTransfer storage transfer = transfers[transferId];
        
        require(transfer.status == TransferStatus.LOCKED, "Transfer not locked");
        require(block.timestamp < transfer.unlockTime, "Transfer expired");
        
        // Verify the secret matches the hash
        bytes32 calculatedHash = keccak256(secret);
        require(calculatedHash == transfer.secretHash, "Invalid secret");
        
        transfer.status = TransferStatus.COMPLETED;
        transfer.secret = secret;
        
        emit TransferCompleted(transferId, secret);
    }

    /**
     * @dev Refund transfer after timeout
     */
    function refundTransfer(bytes32 transferId) external nonReentrant {
        BridgeTransfer storage transfer = transfers[transferId];
        
        require(transfer.status == TransferStatus.LOCKED, "Transfer not locked");
        require(block.timestamp >= transfer.unlockTime, "Transfer not expired");
        require(msg.sender == transfer.initiator, "Only initiator can refund");
        
        transfer.status = TransferStatus.REFUNDED;
        
        emit TransferRefunded(transferId);
    }

    /**
     * @dev Mint credential on target chain with proof from source chain
     */
    function mintWithProof(
        bytes32 credentialHash,
        string memory sourceChain,
        bytes memory proof,
        bytes memory signature
    ) external nonReentrant {
        require(bridgeOperators[sourceChain] != address(0), "Unknown source chain");
        
        // Verify the proof hasn't been used before
        bytes32 proofHash = keccak256(proof);
        require(!usedProofs[proofHash], "Proof already used");
        
        // Verify signature from source chain operator
        bytes32 messageHash = keccak256(
            abi.encodePacked(
                credentialHash,
                sourceChain,
                getChainName(block.chainid),
                proof
            )
        );
        
        address signer = messageHash.toEthSignedMessageHash().recover(signature);
        require(signer == bridgeOperators[sourceChain], "Invalid signature");
        
        // Mark proof as used
        usedProofs[proofHash] = true;
        
        // In a real implementation, this would mint or unlock the credential
        // For now, we just emit an event
        emit TransferCompleted(
            keccak256(abi.encodePacked(credentialHash, sourceChain)),
            proof
        );
    }

    /**
     * @dev Get chain name from chain ID
     */
    function getChainName(uint256 chainId) internal pure returns (string memory) {
        if (chainId == 1) return "ethereum";
        if (chainId == 137) return "polygon";
        if (chainId == 42161) return "arbitrum";
        if (chainId == 10) return "optimism";
        if (chainId == 56) return "bsc";
        if (chainId == 43114) return "avalanche";
        return "unknown";
    }

    /**
     * @dev Update lock duration
     */
    function setLockDuration(uint256 _duration) external onlyOwner {
        require(_duration >= 1 hours && _duration <= 7 days, "Invalid duration");
        lockDuration = _duration;
    }

    /**
     * @dev Get transfer details
     */
    function getTransfer(bytes32 transferId) external view returns (
        bytes32 credentialHash,
        address initiator,
        string memory sourceChain,
        string memory targetChain,
        uint256 lockTime,
        uint256 unlockTime,
        TransferStatus status
    ) {
        BridgeTransfer memory transfer = transfers[transferId];
        return (
            transfer.credentialHash,
            transfer.initiator,
            transfer.sourceChain,
            transfer.targetChain,
            transfer.lockTime,
            transfer.unlockTime,
            transfer.status
        );
    }
} 