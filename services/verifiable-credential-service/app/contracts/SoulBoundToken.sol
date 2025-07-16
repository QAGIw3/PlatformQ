// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/utils/Counters.sol";

/**
 * @title SoulBoundCredentialToken
 * @dev Implementation of non-transferable credential tokens (SoulBound Tokens)
 * Based on EIP-5192: Minimal Soulbound NFTs
 */
contract SoulBoundCredentialToken is Ownable {
    using Counters for Counters.Counter;
    
    struct SoulBoundToken {
        uint256 tokenId;
        address soul;           // The address this token is bound to
        address issuer;         // The credential issuer
        bytes32 credentialHash; // Hash of the credential data
        string metadataURI;     // IPFS URI for credential metadata
        uint256 issuedAt;       // Timestamp of issuance
        bool revoked;           // Revocation status
        string revocationReason;// Reason for revocation if applicable
    }
    
    // Token ID counter
    Counters.Counter private _tokenIdCounter;
    
    // Mapping from token ID to token data
    mapping(uint256 => SoulBoundToken) public tokens;
    
    // Mapping from soul address to list of token IDs
    mapping(address => uint256[]) public soulTokens;
    
    // Mapping from credential hash to token ID (ensures uniqueness)
    mapping(bytes32 => uint256) public credentialToToken;
    
    // Mapping to track authorized issuers
    mapping(address => bool) public authorizedIssuers;
    
    // Burn authorization settings
    enum BurnAuth { OWNER_ONLY, ISSUER_ONLY, OWNER_OR_ISSUER, NEITHER }
    mapping(uint256 => BurnAuth) public burnAuthorization;
    
    // Events
    event Issued(
        uint256 indexed tokenId,
        address indexed soul,
        address indexed issuer,
        bytes32 credentialHash
    );
    
    event Revoked(
        uint256 indexed tokenId,
        address indexed revokedBy,
        string reason
    );
    
    event Burned(
        uint256 indexed tokenId,
        address indexed burnedBy
    );
    
    event IssuerAuthorized(address indexed issuer);
    event IssuerDeauthorized(address indexed issuer);
    
    // EIP-5192 events
    event Locked(uint256 tokenId);
    event Unlocked(uint256 tokenId);
    
    // Errors
    error Soulbound();
    error NotAuthorized();
    error TokenNotFound();
    error TokenRevoked();
    error CredentialExists();
    
    modifier onlyAuthorizedIssuer() {
        if (!authorizedIssuers[msg.sender] && msg.sender != owner()) {
            revert NotAuthorized();
        }
        _;
    }
    
    modifier tokenExists(uint256 tokenId) {
        if (tokens[tokenId].soul == address(0)) {
            revert TokenNotFound();
        }
        _;
    }
    
    constructor() Ownable(msg.sender) {
        // Owner is automatically an authorized issuer
        authorizedIssuers[msg.sender] = true;
    }
    
    /**
     * @dev Authorize an address to issue SBTs
     */
    function authorizeIssuer(address issuer) external onlyOwner {
        authorizedIssuers[issuer] = true;
        emit IssuerAuthorized(issuer);
    }
    
    /**
     * @dev Deauthorize an issuer
     */
    function deauthorizeIssuer(address issuer) external onlyOwner {
        authorizedIssuers[issuer] = false;
        emit IssuerDeauthorized(issuer);
    }
    
    /**
     * @dev Issue a new SoulBound Token
     */
    function issue(
        address soul,
        bytes32 credentialHash,
        string memory metadataURI,
        BurnAuth _burnAuth
    ) external onlyAuthorizedIssuer returns (uint256) {
        // Check if credential already exists
        if (credentialToToken[credentialHash] != 0) {
            revert CredentialExists();
        }
        
        _tokenIdCounter.increment();
        uint256 tokenId = _tokenIdCounter.current();
        
        tokens[tokenId] = SoulBoundToken({
            tokenId: tokenId,
            soul: soul,
            issuer: msg.sender,
            credentialHash: credentialHash,
            metadataURI: metadataURI,
            issuedAt: block.timestamp,
            revoked: false,
            revocationReason: ""
        });
        
        soulTokens[soul].push(tokenId);
        credentialToToken[credentialHash] = tokenId;
        burnAuthorization[tokenId] = _burnAuth;
        
        // Emit EIP-5192 Locked event to indicate non-transferability
        emit Locked(tokenId);
        emit Issued(tokenId, soul, msg.sender, credentialHash);
        
        return tokenId;
    }
    
    /**
     * @dev Revoke a token (does not burn it, keeps record)
     */
    function revoke(uint256 tokenId, string memory reason) 
        external 
        tokenExists(tokenId) 
    {
        SoulBoundToken storage token = tokens[tokenId];
        
        // Only issuer can revoke
        if (msg.sender != token.issuer && msg.sender != owner()) {
            revert NotAuthorized();
        }
        
        token.revoked = true;
        token.revocationReason = reason;
        
        emit Revoked(tokenId, msg.sender, reason);
    }
    
    /**
     * @dev Burn a token (permanent deletion)
     */
    function burn(uint256 tokenId) external tokenExists(tokenId) {
        SoulBoundToken memory token = tokens[tokenId];
        BurnAuth auth = burnAuthorization[tokenId];
        
        bool canBurn = false;
        
        if (auth == BurnAuth.OWNER_ONLY && msg.sender == token.soul) {
            canBurn = true;
        } else if (auth == BurnAuth.ISSUER_ONLY && msg.sender == token.issuer) {
            canBurn = true;
        } else if (auth == BurnAuth.OWNER_OR_ISSUER && 
                  (msg.sender == token.soul || msg.sender == token.issuer)) {
            canBurn = true;
        } else if (msg.sender == owner()) {
            canBurn = true; // Contract owner can always burn
        }
        
        if (!canBurn) {
            revert NotAuthorized();
        }
        
        // Remove from soul's token list
        uint256[] storage userTokens = soulTokens[token.soul];
        for (uint i = 0; i < userTokens.length; i++) {
            if (userTokens[i] == tokenId) {
                userTokens[i] = userTokens[userTokens.length - 1];
                userTokens.pop();
                break;
            }
        }
        
        // Clear mappings
        delete credentialToToken[token.credentialHash];
        delete tokens[tokenId];
        delete burnAuthorization[tokenId];
        
        emit Burned(tokenId, msg.sender);
    }
    
    /**
     * @dev Get all tokens for a soul address
     */
    function tokensOf(address soul) external view returns (uint256[] memory) {
        return soulTokens[soul];
    }
    
    /**
     * @dev Get token details
     */
    function getToken(uint256 tokenId) 
        external 
        view 
        tokenExists(tokenId) 
        returns (SoulBoundToken memory) 
    {
        return tokens[tokenId];
    }
    
    /**
     * @dev Verify a credential hash belongs to a soul
     */
    function verify(address soul, bytes32 credentialHash) 
        external 
        view 
        returns (bool valid, uint256 tokenId) 
    {
        tokenId = credentialToToken[credentialHash];
        if (tokenId == 0) {
            return (false, 0);
        }
        
        SoulBoundToken memory token = tokens[tokenId];
        valid = token.soul == soul && !token.revoked;
        
        return (valid, tokenId);
    }
    
    /**
     * @dev Check if a token is locked (always true for SBTs)
     * Implements EIP-5192
     */
    function locked(uint256 tokenId) external view returns (bool) {
        return tokens[tokenId].soul != address(0);
    }
    
    /**
     * @dev Prevent transfers (will always revert)
     */
    function transferFrom(address, address, uint256) external pure {
        revert Soulbound();
    }
    
    function safeTransferFrom(address, address, uint256) external pure {
        revert Soulbound();
    }
    
    function safeTransferFrom(address, address, uint256, bytes memory) external pure {
        revert Soulbound();
    }
    
    /**
     * @dev Get total number of tokens issued
     */
    function totalSupply() external view returns (uint256) {
        return _tokenIdCounter.current();
    }
} 