// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

/**
 * @title CredentialRegistry
 * @dev A registry for anchoring verifiable credentials on blockchain
 */
contract CredentialRegistry {
    // Struct to store credential information
    struct Credential {
        string credentialHash;
        uint256 timestamp;
        uint256 blockNumber;
        address issuer;
        string tenantId;
        bool revoked;
        string revocationReason;
        uint256 revocationTimestamp;
    }
    
    // Mapping from credential ID to credential data
    mapping(string => Credential) public credentials;
    
    // Mapping to track credential versions (for updates)
    mapping(string => uint256) public credentialVersions;
    
    // Mapping from issuer to their credentials
    mapping(address => string[]) public issuerCredentials;
    
    // Mapping from tenant to their credentials
    mapping(string => string[]) public tenantCredentials;
    
    // Events
    event CredentialAnchored(
        string indexed credentialId,
        string credentialHash,
        address indexed issuer,
        string indexed tenantId,
        uint256 timestamp
    );
    
    event CredentialRevoked(
        string indexed credentialId,
        address indexed revoker,
        string reason,
        uint256 timestamp
    );
    
    event CredentialUpdated(
        string indexed credentialId,
        string newHash,
        uint256 version,
        uint256 timestamp
    );
    
    // Modifiers
    modifier onlyIssuer(string memory credentialId) {
        require(
            credentials[credentialId].issuer == msg.sender,
            "Only the issuer can perform this action"
        );
        _;
    }
    
    modifier notRevoked(string memory credentialId) {
        require(
            !credentials[credentialId].revoked,
            "Credential has been revoked"
        );
        _;
    }
    
    /**
     * @dev Anchor a new credential on the blockchain
     * @param credentialId Unique identifier for the credential
     * @param credentialHash Hash of the credential content
     * @param tenantId Tenant identifier
     */
    function anchorCredential(
        string memory credentialId,
        string memory credentialHash,
        string memory tenantId
    ) public {
        require(
            bytes(credentials[credentialId].credentialHash).length == 0,
            "Credential already exists"
        );
        
        credentials[credentialId] = Credential({
            credentialHash: credentialHash,
            timestamp: block.timestamp,
            blockNumber: block.number,
            issuer: msg.sender,
            tenantId: tenantId,
            revoked: false,
            revocationReason: "",
            revocationTimestamp: 0
        });
        
        credentialVersions[credentialId] = 1;
        issuerCredentials[msg.sender].push(credentialId);
        tenantCredentials[tenantId].push(credentialId);
        
        emit CredentialAnchored(
            credentialId,
            credentialHash,
            msg.sender,
            tenantId,
            block.timestamp
        );
    }
    
    /**
     * @dev Update an existing credential (creates new version)
     * @param credentialId Credential to update
     * @param newHash New credential hash
     */
    function updateCredential(
        string memory credentialId,
        string memory newHash
    ) public onlyIssuer(credentialId) notRevoked(credentialId) {
        credentials[credentialId].credentialHash = newHash;
        credentials[credentialId].timestamp = block.timestamp;
        credentialVersions[credentialId]++;
        
        emit CredentialUpdated(
            credentialId,
            newHash,
            credentialVersions[credentialId],
            block.timestamp
        );
    }
    
    /**
     * @dev Revoke a credential
     * @param credentialId Credential to revoke
     * @param reason Reason for revocation
     */
    function revokeCredential(
        string memory credentialId,
        string memory reason
    ) public onlyIssuer(credentialId) {
        require(
            bytes(credentials[credentialId].credentialHash).length > 0,
            "Credential not found"
        );
        
        credentials[credentialId].revoked = true;
        credentials[credentialId].revocationReason = reason;
        credentials[credentialId].revocationTimestamp = block.timestamp;
        
        emit CredentialRevoked(
            credentialId,
            msg.sender,
            reason,
            block.timestamp
        );
    }
    
    /**
     * @dev Get credential details
     * @param credentialId Credential identifier
     */
    function getCredential(string memory credentialId)
        public
        view
        returns (
            string memory credentialHash,
            uint256 timestamp,
            address issuer,
            bool revoked,
            uint256 version
        )
    {
        Credential memory cred = credentials[credentialId];
        return (
            cred.credentialHash,
            cred.timestamp,
            cred.issuer,
            cred.revoked,
            credentialVersions[credentialId]
        );
    }
    
    /**
     * @dev Check if a credential is valid (exists and not revoked)
     * @param credentialId Credential to check
     */
    function isValid(string memory credentialId) public view returns (bool) {
        return bytes(credentials[credentialId].credentialHash).length > 0 &&
               !credentials[credentialId].revoked;
    }
    
    /**
     * @dev Get all credentials for an issuer
     * @param issuer Address of the issuer
     */
    function getIssuerCredentials(address issuer)
        public
        view
        returns (string[] memory)
    {
        return issuerCredentials[issuer];
    }
    
    /**
     * @dev Get all credentials for a tenant
     * @param tenantId Tenant identifier
     */
    function getTenantCredentials(string memory tenantId)
        public
        view
        returns (string[] memory)
    {
        return tenantCredentials[tenantId];
    }
    
    /**
     * @dev Batch anchor multiple credentials
     * @param credentialIds Array of credential IDs
     * @param credentialHashes Array of credential hashes
     * @param tenantId Tenant identifier
     */
    function batchAnchorCredentials(
        string[] memory credentialIds,
        string[] memory credentialHashes,
        string memory tenantId
    ) public {
        require(
            credentialIds.length == credentialHashes.length,
            "Arrays must have same length"
        );
        
        for (uint256 i = 0; i < credentialIds.length; i++) {
            anchorCredential(credentialIds[i], credentialHashes[i], tenantId);
        }
    }
} 