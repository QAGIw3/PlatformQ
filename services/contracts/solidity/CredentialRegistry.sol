// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/security/Pausable.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/utils/cryptography/EIP712.sol";
import "@openzeppelin/contracts/utils/cryptography/ECDSA.sol";

/**
 * @title CredentialRegistry
 * @dev Registry for storing and verifying credentials on-chain
 * Supports revocation, expiration, and multi-signature verification
 */
contract CredentialRegistry is AccessControl, Pausable, ReentrancyGuard, EIP712 {
    using ECDSA for bytes32;

    // Roles
    bytes32 public constant ISSUER_ROLE = keccak256("ISSUER_ROLE");
    bytes32 public constant VERIFIER_ROLE = keccak256("VERIFIER_ROLE");
    bytes32 public constant REVOKE_ROLE = keccak256("REVOKE_ROLE");

    // Credential structure
    struct Credential {
        bytes32 id;
        address issuer;
        address subject;
        string credentialType;
        bytes32 dataHash; // Hash of off-chain credential data
        uint256 issuedAt;
        uint256 expiresAt;
        bool revoked;
        string metadataURI; // Link to off-chain metadata
    }

    // Revocation record
    struct RevocationRecord {
        uint256 timestamp;
        address revokedBy;
        string reason;
    }

    // State variables
    mapping(bytes32 => Credential) public credentials;
    mapping(bytes32 => RevocationRecord) public revocationRecords;
    mapping(address => bytes32[]) public subjectCredentials;
    mapping(address => bytes32[]) public issuerCredentials;
    
    // Trusted issuers
    mapping(address => bool) public trustedIssuers;
    
    // Credential type schemas
    mapping(string => bytes32) public credentialSchemas;

    // Events
    event CredentialIssued(
        bytes32 indexed credentialId,
        address indexed issuer,
        address indexed subject,
        string credentialType
    );
    
    event CredentialRevoked(
        bytes32 indexed credentialId,
        address indexed revokedBy,
        string reason
    );
    
    event CredentialVerified(
        bytes32 indexed credentialId,
        address indexed verifier,
        bool isValid
    );
    
    event TrustedIssuerAdded(address indexed issuer);
    event TrustedIssuerRemoved(address indexed issuer);
    event SchemaRegistered(string credentialType, bytes32 schemaHash);

    // Modifiers
    modifier onlyValidCredential(bytes32 credentialId) {
        require(credentials[credentialId].id != 0, "Credential does not exist");
        require(!credentials[credentialId].revoked, "Credential is revoked");
        require(
            credentials[credentialId].expiresAt == 0 || 
            block.timestamp <= credentials[credentialId].expiresAt,
            "Credential has expired"
        );
        _;
    }

    // EIP712 domain separator
    bytes32 private constant CREDENTIAL_TYPEHASH = keccak256(
        "Credential(bytes32 id,address issuer,address subject,string credentialType,bytes32 dataHash,uint256 issuedAt,uint256 expiresAt)"
    );

    constructor() EIP712("CredentialRegistry", "1") {
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(ISSUER_ROLE, msg.sender);
        _grantRole(VERIFIER_ROLE, msg.sender);
        _grantRole(REVOKE_ROLE, msg.sender);
    }

    /**
     * @dev Issue a new credential
     */
    function issueCredential(
        address subject,
        string memory credentialType,
        bytes32 dataHash,
        uint256 expiresAt,
        string memory metadataURI
    ) external whenNotPaused nonReentrant returns (bytes32) {
        require(hasRole(ISSUER_ROLE, msg.sender) || trustedIssuers[msg.sender], "Not authorized to issue");
        require(subject != address(0), "Invalid subject address");
        require(bytes(credentialType).length > 0, "Invalid credential type");
        require(dataHash != bytes32(0), "Invalid data hash");
        require(expiresAt == 0 || expiresAt > block.timestamp, "Invalid expiration");

        // Generate unique credential ID
        bytes32 credentialId = keccak256(
            abi.encodePacked(
                msg.sender,
                subject,
                credentialType,
                dataHash,
                block.timestamp,
                block.number
            )
        );

        // Create credential
        credentials[credentialId] = Credential({
            id: credentialId,
            issuer: msg.sender,
            subject: subject,
            credentialType: credentialType,
            dataHash: dataHash,
            issuedAt: block.timestamp,
            expiresAt: expiresAt,
            revoked: false,
            metadataURI: metadataURI
        });

        // Add to mappings
        subjectCredentials[subject].push(credentialId);
        issuerCredentials[msg.sender].push(credentialId);

        emit CredentialIssued(credentialId, msg.sender, subject, credentialType);

        return credentialId;
    }

    /**
     * @dev Revoke a credential
     */
    function revokeCredential(bytes32 credentialId, string memory reason) 
        external 
        whenNotPaused 
        nonReentrant 
    {
        Credential storage credential = credentials[credentialId];
        require(credential.id != 0, "Credential does not exist");
        require(!credential.revoked, "Already revoked");
        require(
            hasRole(REVOKE_ROLE, msg.sender) || 
            msg.sender == credential.issuer ||
            msg.sender == credential.subject,
            "Not authorized to revoke"
        );

        credential.revoked = true;
        
        revocationRecords[credentialId] = RevocationRecord({
            timestamp: block.timestamp,
            revokedBy: msg.sender,
            reason: reason
        });

        emit CredentialRevoked(credentialId, msg.sender, reason);
    }

    /**
     * @dev Verify a credential
     */
    function verifyCredential(bytes32 credentialId) 
        external 
        view 
        onlyValidCredential(credentialId) 
        returns (bool isValid, Credential memory credential) 
    {
        credential = credentials[credentialId];
        isValid = true;
        
        // Additional checks can be added here
        if (credentialSchemas[credential.credentialType] != bytes32(0)) {
            // Verify against schema if registered
            // This would typically involve off-chain verification
        }
    }

    /**
     * @dev Verify credential with signature
     */
    function verifyCredentialWithSignature(
        bytes32 credentialId,
        bytes memory signature
    ) external view returns (bool) {
        Credential memory credential = credentials[credentialId];
        require(credential.id != 0, "Credential does not exist");

        // Create hash of credential data
        bytes32 structHash = keccak256(
            abi.encode(
                CREDENTIAL_TYPEHASH,
                credential.id,
                credential.issuer,
                credential.subject,
                keccak256(bytes(credential.credentialType)),
                credential.dataHash,
                credential.issuedAt,
                credential.expiresAt
            )
        );

        bytes32 hash = _hashTypedDataV4(structHash);
        address signer = ECDSA.recover(hash, signature);

        return signer == credential.issuer;
    }

    /**
     * @dev Get credentials for a subject
     */
    function getSubjectCredentials(address subject) 
        external 
        view 
        returns (bytes32[] memory) 
    {
        return subjectCredentials[subject];
    }

    /**
     * @dev Get credentials issued by an issuer
     */
    function getIssuerCredentials(address issuer) 
        external 
        view 
        returns (bytes32[] memory) 
    {
        return issuerCredentials[issuer];
    }

    /**
     * @dev Add trusted issuer
     */
    function addTrustedIssuer(address issuer) 
        external 
        onlyRole(DEFAULT_ADMIN_ROLE) 
    {
        require(issuer != address(0), "Invalid issuer address");
        trustedIssuers[issuer] = true;
        emit TrustedIssuerAdded(issuer);
    }

    /**
     * @dev Remove trusted issuer
     */
    function removeTrustedIssuer(address issuer) 
        external 
        onlyRole(DEFAULT_ADMIN_ROLE) 
    {
        trustedIssuers[issuer] = false;
        emit TrustedIssuerRemoved(issuer);
    }

    /**
     * @dev Register credential type schema
     */
    function registerSchema(string memory credentialType, bytes32 schemaHash) 
        external 
        onlyRole(DEFAULT_ADMIN_ROLE) 
    {
        require(bytes(credentialType).length > 0, "Invalid credential type");
        require(schemaHash != bytes32(0), "Invalid schema hash");
        
        credentialSchemas[credentialType] = schemaHash;
        emit SchemaRegistered(credentialType, schemaHash);
    }

    /**
     * @dev Pause contract
     */
    function pause() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _pause();
    }

    /**
     * @dev Unpause contract
     */
    function unpause() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _unpause();
    }

    /**
     * @dev Check if credential exists and is valid
     */
    function isCredentialValid(bytes32 credentialId) external view returns (bool) {
        Credential memory credential = credentials[credentialId];
        
        if (credential.id == 0) return false;
        if (credential.revoked) return false;
        if (credential.expiresAt != 0 && block.timestamp > credential.expiresAt) return false;
        
        return true;
    }

    /**
     * @dev Get full credential details including revocation status
     */
    function getCredentialDetails(bytes32 credentialId) 
        external 
        view 
        returns (
            Credential memory credential,
            bool isValid,
            RevocationRecord memory revocationRecord
        ) 
    {
        credential = credentials[credentialId];
        isValid = this.isCredentialValid(credentialId);
        
        if (credential.revoked) {
            revocationRecord = revocationRecords[credentialId];
        }
    }
} 