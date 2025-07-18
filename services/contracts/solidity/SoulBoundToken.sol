// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC721/ERC721.sol";
import "@openzeppelin/contracts/token/ERC721/extensions/ERC721URIStorage.sol";
import "@openzeppelin/contracts/security/Pausable.sol";
import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/utils/cryptography/EIP712.sol";
import "@openzeppelin/contracts/utils/cryptography/ECDSA.sol";
import "@openzeppelin/contracts/utils/Counters.sol";

/**
 * @title SoulBoundToken
 * @dev Non-transferable NFT implementation for identity and reputation
 * Based on ERC-5192: Minimal Soulbound NFTs
 */
contract SoulBoundToken is ERC721, ERC721URIStorage, Pausable, AccessControl, EIP712 {
    using Counters for Counters.Counter;
    using ECDSA for bytes32;

    // Roles
    bytes32 public constant MINTER_ROLE = keccak256("MINTER_ROLE");
    bytes32 public constant BURNER_ROLE = keccak256("BURNER_ROLE");
    bytes32 public constant URI_SETTER_ROLE = keccak256("URI_SETTER_ROLE");

    // Token ID counter
    Counters.Counter private _tokenIdCounter;

    // SBT metadata
    struct SoulBoundData {
        uint256 issuedAt;
        address issuer;
        string tokenType; // e.g., "KYC", "Achievement", "Reputation"
        uint256 level; // For upgradeable SBTs
        bytes32 dataHash; // Hash of off-chain data
        bool revocable;
        bool upgradeable;
    }

    // Mappings
    mapping(uint256 => SoulBoundData) public tokenData;
    mapping(address => uint256[]) public userTokens;
    mapping(string => uint256) public tokenTypeCount;
    mapping(address => mapping(string => uint256)) public userTokenTypeCount;
    
    // Recovery mechanism
    mapping(uint256 => address) public recoveryAddresses;
    mapping(uint256 => uint256) public recoveryDeadlines;
    
    // Revocation records
    mapping(uint256 => bool) public revokedTokens;
    mapping(uint256 => string) public revocationReasons;

    // Events
    event Locked(uint256 tokenId);
    event Unlocked(uint256 tokenId);
    event SoulBoundTokenIssued(
        uint256 indexed tokenId,
        address indexed to,
        address indexed issuer,
        string tokenType
    );
    event SoulBoundTokenRevoked(uint256 indexed tokenId, string reason);
    event SoulBoundTokenUpgraded(uint256 indexed tokenId, uint256 newLevel);
    event RecoveryInitiated(uint256 indexed tokenId, address newOwner, uint256 deadline);
    event RecoveryCompleted(uint256 indexed tokenId, address oldOwner, address newOwner);

    // EIP-5192 events
    event Locked(uint256 tokenId);
    event Unlocked(uint256 tokenId);

    // Custom errors
    error TokenIsSoulBound();
    error TokenNotRevocable();
    error TokenNotUpgradeable();
    error InvalidRecovery();
    error TokenRevoked();

    // EIP712 type hashes
    bytes32 private constant MINT_TYPEHASH = keccak256(
        "Mint(address to,string tokenType,uint256 level,bytes32 dataHash,uint256 nonce,uint256 deadline)"
    );

    constructor(
        string memory name,
        string memory symbol
    ) ERC721(name, symbol) EIP712(name, "1") {
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(MINTER_ROLE, msg.sender);
        _grantRole(BURNER_ROLE, msg.sender);
        _grantRole(URI_SETTER_ROLE, msg.sender);
    }

    /**
     * @dev Issue a new SoulBound Token
     */
    function issueSoulBoundToken(
        address to,
        string memory tokenType,
        string memory uri,
        uint256 level,
        bytes32 dataHash,
        bool revocable,
        bool upgradeable
    ) public onlyRole(MINTER_ROLE) whenNotPaused returns (uint256) {
        require(to != address(0), "Cannot mint to zero address");
        require(bytes(tokenType).length > 0, "Invalid token type");

        uint256 tokenId = _tokenIdCounter.current();
        _tokenIdCounter.increment();

        _safeMint(to, tokenId);
        _setTokenURI(tokenId, uri);

        tokenData[tokenId] = SoulBoundData({
            issuedAt: block.timestamp,
            issuer: msg.sender,
            tokenType: tokenType,
            level: level,
            dataHash: dataHash,
            revocable: revocable,
            upgradeable: upgradeable
        });

        userTokens[to].push(tokenId);
        tokenTypeCount[tokenType]++;
        userTokenTypeCount[to][tokenType]++;

        emit SoulBoundTokenIssued(tokenId, to, msg.sender, tokenType);
        emit Locked(tokenId); // Lock immediately after minting

        return tokenId;
    }

    /**
     * @dev Issue with signature (allows gasless minting)
     */
    function issueSoulBoundTokenWithSignature(
        address to,
        string memory tokenType,
        string memory uri,
        uint256 level,
        bytes32 dataHash,
        bool revocable,
        bool upgradeable,
        uint256 nonce,
        uint256 deadline,
        bytes memory signature
    ) external whenNotPaused returns (uint256) {
        require(deadline >= block.timestamp, "Signature expired");

        bytes32 structHash = keccak256(
            abi.encode(
                MINT_TYPEHASH,
                to,
                keccak256(bytes(tokenType)),
                level,
                dataHash,
                nonce,
                deadline
            )
        );

        bytes32 hash = _hashTypedDataV4(structHash);
        address signer = ECDSA.recover(hash, signature);

        require(hasRole(MINTER_ROLE, signer), "Invalid signer");

        return issueSoulBoundToken(to, tokenType, uri, level, dataHash, revocable, upgradeable);
    }

    /**
     * @dev Revoke a SoulBound Token
     */
    function revokeSoulBoundToken(uint256 tokenId, string memory reason) 
        external 
        onlyRole(BURNER_ROLE) 
    {
        require(_exists(tokenId), "Token does not exist");
        require(tokenData[tokenId].revocable, "Token is not revocable");
        require(!revokedTokens[tokenId], "Token already revoked");

        revokedTokens[tokenId] = true;
        revocationReasons[tokenId] = reason;

        emit SoulBoundTokenRevoked(tokenId, reason);
    }

    /**
     * @dev Upgrade a SoulBound Token level
     */
    function upgradeSoulBoundToken(uint256 tokenId, uint256 newLevel) 
        external 
        onlyRole(MINTER_ROLE) 
    {
        require(_exists(tokenId), "Token does not exist");
        require(tokenData[tokenId].upgradeable, "Token is not upgradeable");
        require(!revokedTokens[tokenId], "Token is revoked");
        require(newLevel > tokenData[tokenId].level, "New level must be higher");

        tokenData[tokenId].level = newLevel;

        emit SoulBoundTokenUpgraded(tokenId, newLevel);
    }

    /**
     * @dev Initiate recovery process
     */
    function initiateRecovery(uint256 tokenId, address newOwner) 
        external 
        onlyRole(DEFAULT_ADMIN_ROLE) 
    {
        require(_exists(tokenId), "Token does not exist");
        require(newOwner != address(0), "Invalid new owner");
        require(ownerOf(tokenId) != newOwner, "Already the owner");

        uint256 deadline = block.timestamp + 7 days; // 7 day recovery period
        recoveryAddresses[tokenId] = newOwner;
        recoveryDeadlines[tokenId] = deadline;

        emit RecoveryInitiated(tokenId, newOwner, deadline);
    }

    /**
     * @dev Complete recovery process
     */
    function completeRecovery(uint256 tokenId) external {
        require(_exists(tokenId), "Token does not exist");
        require(recoveryAddresses[tokenId] != address(0), "No recovery initiated");
        require(block.timestamp >= recoveryDeadlines[tokenId], "Recovery period not over");

        address oldOwner = ownerOf(tokenId);
        address newOwner = recoveryAddresses[tokenId];

        // Force transfer (bypassing soul-bound restriction for recovery)
        _transfer(oldOwner, newOwner, tokenId);

        // Update user tokens mapping
        _removeTokenFromUser(oldOwner, tokenId);
        userTokens[newOwner].push(tokenId);

        // Clear recovery data
        delete recoveryAddresses[tokenId];
        delete recoveryDeadlines[tokenId];

        emit RecoveryCompleted(tokenId, oldOwner, newOwner);
    }

    /**
     * @dev Override transfer functions to prevent transfers
     */
    function _beforeTokenTransfer(
        address from,
        address to,
        uint256 tokenId,
        uint256 batchSize
    ) internal override whenNotPaused {
        super._beforeTokenTransfer(from, to, tokenId, batchSize);
        
        // Allow minting and burning, but not transfers
        if (from != address(0) && to != address(0)) {
            // Check if this is a recovery transfer
            if (recoveryAddresses[tokenId] != to) {
                revert TokenIsSoulBound();
            }
        }

        // Check if token is revoked
        if (revokedTokens[tokenId]) {
            revert TokenRevoked();
        }
    }

    /**
     * @dev Get user's tokens
     */
    function getUserTokens(address user) external view returns (uint256[] memory) {
        return userTokens[user];
    }

    /**
     * @dev Get user's tokens by type
     */
    function getUserTokensByType(address user, string memory tokenType) 
        external 
        view 
        returns (uint256[] memory) 
    {
        uint256[] memory allTokens = userTokens[user];
        uint256 count = 0;

        // Count matching tokens
        for (uint256 i = 0; i < allTokens.length; i++) {
            if (keccak256(bytes(tokenData[allTokens[i]].tokenType)) == keccak256(bytes(tokenType))) {
                count++;
            }
        }

        // Create result array
        uint256[] memory result = new uint256[](count);
        uint256 index = 0;

        for (uint256 i = 0; i < allTokens.length; i++) {
            if (keccak256(bytes(tokenData[allTokens[i]].tokenType)) == keccak256(bytes(tokenType))) {
                result[index] = allTokens[i];
                index++;
            }
        }

        return result;
    }

    /**
     * @dev Check if token is locked (always true for SBTs)
     */
    function locked(uint256 tokenId) external view returns (bool) {
        require(_exists(tokenId), "Token does not exist");
        return true; // All SBTs are locked
    }

    /**
     * @dev Remove token from user's array
     */
    function _removeTokenFromUser(address user, uint256 tokenId) private {
        uint256[] storage tokens = userTokens[user];
        for (uint256 i = 0; i < tokens.length; i++) {
            if (tokens[i] == tokenId) {
                tokens[i] = tokens[tokens.length - 1];
                tokens.pop();
                break;
            }
        }
    }

    /**
     * @dev Burn a token (only by owner or authorized burner)
     */
    function burn(uint256 tokenId) public {
        require(_exists(tokenId), "Token does not exist");
        require(
            ownerOf(tokenId) == msg.sender || hasRole(BURNER_ROLE, msg.sender),
            "Not authorized to burn"
        );

        address owner = ownerOf(tokenId);
        _burn(tokenId);
        _removeTokenFromUser(owner, tokenId);
        
        string memory tokenType = tokenData[tokenId].tokenType;
        tokenTypeCount[tokenType]--;
        userTokenTypeCount[owner][tokenType]--;
    }

    // Required overrides
    function _burn(uint256 tokenId) internal override(ERC721, ERC721URIStorage) {
        super._burn(tokenId);
    }

    function tokenURI(uint256 tokenId)
        public
        view
        override(ERC721, ERC721URIStorage)
        returns (string memory)
    {
        return super.tokenURI(tokenId);
    }

    function supportsInterface(bytes4 interfaceId)
        public
        view
        override(ERC721, AccessControl)
        returns (bool)
    {
        return super.supportsInterface(interfaceId);
    }

    // Admin functions
    function pause() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _pause();
    }

    function unpause() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _unpause();
    }
} 