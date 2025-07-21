// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC1155/ERC1155.sol";
import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/security/Pausable.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/utils/Counters.sol";
import "@openzeppelin/contracts/utils/math/SafeMath.sol";

/**
 * @title ResourceToken
 * @notice ERC-1155 token for tokenizing compute resources (CPU, GPU, Storage, Bandwidth, Memory)
 * @dev Implements Infrastructure DeFi for PlatformQ
 */
contract ResourceToken is ERC1155, AccessControl, Pausable, ReentrancyGuard {
    using SafeMath for uint256;
    using Counters for Counters.Counter;
    
    // Roles
    bytes32 public constant MINTER_ROLE = keccak256("MINTER_ROLE");
    bytes32 public constant BURNER_ROLE = keccak256("BURNER_ROLE");
    bytes32 public constant SLASHER_ROLE = keccak256("SLASHER_ROLE");
    bytes32 public constant ORACLE_ROLE = keccak256("ORACLE_ROLE");
    
    // Resource types
    enum ResourceType {
        CPU_HOURS,          // 0: CPU compute hours
        GPU_HOURS,          // 1: GPU compute hours
        STORAGE_GB_HOURS,   // 2: Storage GB-hours
        BANDWIDTH_TB,       // 3: Bandwidth in TB
        MEMORY_GB_HOURS     // 4: Memory GB-hours
    }
    
    // Service tiers
    enum ServiceTier {
        STANDARD,   // 0: Best effort
        PREMIUM,    // 1: Guaranteed performance
        GUARANTEED  // 2: Dedicated resources with SLA
    }
    
    // Resource specification
    struct ResourceSpec {
        ResourceType resourceType;
        uint256 amount;             // Amount of resource (e.g., 100 CPU hours)
        uint256 validFrom;          // Unix timestamp when resource becomes valid
        uint256 validUntil;         // Unix timestamp when resource expires
        string region;              // Geographic region (e.g., "us-east-1")
        ServiceTier tier;           // Service quality tier
        address provider;           // Resource provider address
        bytes32 slaHash;           // Hash of SLA terms
        bool isActive;             // Whether the resource is active
        uint256 slashedAmount;     // Amount slashed for SLA violations
    }
    
    // Provider information
    struct Provider {
        bool isActive;
        uint256 totalMinted;        // Total resources minted
        uint256 totalBurned;        // Total resources consumed
        uint256 totalSlashed;       // Total resources slashed
        uint256 reputationScore;    // Provider reputation (0-1000)
        mapping(ResourceType => uint256) resourceCapacity;  // Available capacity by type
    }
    
    // Token ID counter
    Counters.Counter private _tokenIdCounter;
    
    // Storage
    mapping(uint256 => ResourceSpec) public resourceSpecs;
    mapping(address => Provider) public providers;
    mapping(address => bool) public authorizedMinters;
    mapping(address => bool) public authorizedBurners;
    mapping(uint256 => uint256) public tokenPrices; // Token ID to price in wei
    
    // Slashing parameters
    uint256 public slashingPercentage = 1000; // 10% default (basis points)
    uint256 public constant MAX_SLASHING = 5000; // 50% maximum
    
    // Events
    event ResourceMinted(
        uint256 indexed tokenId,
        address indexed provider,
        ResourceType resourceType,
        uint256 amount,
        string region,
        ServiceTier tier
    );
    
    event ResourceBurned(
        uint256 indexed tokenId,
        address indexed consumer,
        uint256 amount
    );
    
    event ResourceSlashed(
        uint256 indexed tokenId,
        address indexed provider,
        uint256 slashedAmount,
        string reason
    );
    
    event ProviderRegistered(address indexed provider);
    event ProviderDeactivated(address indexed provider);
    event PriceUpdated(uint256 indexed tokenId, uint256 price);
    event SLAViolation(uint256 indexed tokenId, bytes32 violationHash);
    
    /**
     * @dev Constructor
     * @param uri Base URI for token metadata
     */
    constructor(string memory uri) ERC1155(uri) {
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(MINTER_ROLE, msg.sender);
        _grantRole(BURNER_ROLE, msg.sender);
        _grantRole(SLASHER_ROLE, msg.sender);
    }
    
    /**
     * @notice Mint new resource tokens
     * @param provider Address of the resource provider
     * @param resourceType Type of resource
     * @param amount Amount of resource
     * @param validFrom Start timestamp
     * @param validUntil Expiry timestamp
     * @param region Geographic region
     * @param tier Service tier
     * @param slaHash Hash of SLA terms
     * @return tokenId The ID of the minted token
     */
    function mintResource(
        address provider,
        ResourceType resourceType,
        uint256 amount,
        uint256 validFrom,
        uint256 validUntil,
        string memory region,
        ServiceTier tier,
        bytes32 slaHash
    ) external onlyRole(MINTER_ROLE) nonReentrant whenNotPaused returns (uint256) {
        require(provider != address(0), "Invalid provider");
        require(amount > 0, "Amount must be positive");
        require(validUntil > validFrom, "Invalid validity period");
        require(validFrom >= block.timestamp, "Cannot mint past resources");
        require(providers[provider].isActive, "Provider not active");
        
        // Check provider capacity
        require(
            providers[provider].resourceCapacity[resourceType] >= amount,
            "Insufficient provider capacity"
        );
        
        // Generate token ID
        _tokenIdCounter.increment();
        uint256 tokenId = _tokenIdCounter.current();
        
        // Create resource specification
        resourceSpecs[tokenId] = ResourceSpec({
            resourceType: resourceType,
            amount: amount,
            validFrom: validFrom,
            validUntil: validUntil,
            region: region,
            tier: tier,
            provider: provider,
            slaHash: slaHash,
            isActive: true,
            slashedAmount: 0
        });
        
        // Update provider stats
        providers[provider].totalMinted = providers[provider].totalMinted.add(amount);
        providers[provider].resourceCapacity[resourceType] = 
            providers[provider].resourceCapacity[resourceType].sub(amount);
        
        // Mint tokens to provider (they can then be sold/transferred)
        _mint(provider, tokenId, amount, "");
        
        emit ResourceMinted(tokenId, provider, resourceType, amount, region, tier);
        
        return tokenId;
    }
    
    /**
     * @notice Burn resource tokens upon consumption
     * @param tokenId Token ID to burn
     * @param amount Amount to burn
     */
    function burnResource(
        uint256 tokenId,
        uint256 amount
    ) external onlyRole(BURNER_ROLE) nonReentrant {
        ResourceSpec storage spec = resourceSpecs[tokenId];
        require(spec.isActive, "Resource not active");
        require(block.timestamp >= spec.validFrom, "Resource not yet valid");
        require(block.timestamp <= spec.validUntil, "Resource expired");
        
        address consumer = msg.sender;
        require(balanceOf(consumer, tokenId) >= amount, "Insufficient balance");
        
        // Burn tokens
        _burn(consumer, tokenId, amount);
        
        // Update provider stats
        providers[spec.provider].totalBurned = providers[spec.provider].totalBurned.add(amount);
        
        // Return capacity to provider after consumption
        providers[spec.provider].resourceCapacity[spec.resourceType] = 
            providers[spec.provider].resourceCapacity[spec.resourceType].add(amount);
        
        emit ResourceBurned(tokenId, consumer, amount);
    }
    
    /**
     * @notice Slash tokens for SLA violations
     * @param tokenId Token ID to slash
     * @param violationSeverity Severity of violation (0-10000 basis points)
     * @param reason Reason for slashing
     */
    function slashResource(
        uint256 tokenId,
        uint256 violationSeverity,
        string memory reason
    ) external onlyRole(SLASHER_ROLE) nonReentrant {
        ResourceSpec storage spec = resourceSpecs[tokenId];
        require(spec.isActive, "Resource not active");
        require(violationSeverity <= MAX_SLASHING, "Severity too high");
        
        // Calculate slash amount based on remaining supply
        uint256 totalSupply = totalSupply(tokenId);
        uint256 slashAmount = totalSupply.mul(violationSeverity).div(10000);
        
        if (slashAmount > 0) {
            // Update slashed amount
            spec.slashedAmount = spec.slashedAmount.add(slashAmount);
            providers[spec.provider].totalSlashed = providers[spec.provider].totalSlashed.add(slashAmount);
            
            // Reduce provider reputation
            uint256 reputationPenalty = violationSeverity.div(10); // 1/10 of violation severity
            if (providers[spec.provider].reputationScore > reputationPenalty) {
                providers[spec.provider].reputationScore = 
                    providers[spec.provider].reputationScore.sub(reputationPenalty);
            } else {
                providers[spec.provider].reputationScore = 0;
            }
            
            emit ResourceSlashed(tokenId, spec.provider, slashAmount, reason);
            emit SLAViolation(tokenId, keccak256(abi.encodePacked(reason)));
        }
    }
    
    /**
     * @notice Register a new provider
     * @param provider Provider address
     * @param initialReputation Initial reputation score (0-1000)
     */
    function registerProvider(
        address provider,
        uint256 initialReputation
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(provider != address(0), "Invalid provider");
        require(initialReputation <= 1000, "Invalid reputation");
        require(!providers[provider].isActive, "Provider already active");
        
        providers[provider].isActive = true;
        providers[provider].reputationScore = initialReputation;
        
        emit ProviderRegistered(provider);
    }
    
    /**
     * @notice Set provider capacity for a resource type
     * @param provider Provider address
     * @param resourceType Type of resource
     * @param capacity Available capacity
     */
    function setProviderCapacity(
        address provider,
        ResourceType resourceType,
        uint256 capacity
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(providers[provider].isActive, "Provider not active");
        providers[provider].resourceCapacity[resourceType] = capacity;
    }
    
    /**
     * @notice Update token price
     * @param tokenId Token ID
     * @param price Price in wei
     */
    function updatePrice(
        uint256 tokenId,
        uint256 price
    ) external onlyRole(ORACLE_ROLE) {
        require(resourceSpecs[tokenId].isActive, "Resource not active");
        tokenPrices[tokenId] = price;
        emit PriceUpdated(tokenId, price);
    }
    
    /**
     * @notice Get resource specification
     * @param tokenId Token ID
     * @return Resource specification
     */
    function getResourceSpec(uint256 tokenId) external view returns (ResourceSpec memory) {
        return resourceSpecs[tokenId];
    }
    
    /**
     * @notice Get provider stats
     * @param provider Provider address
     * @return isActive Whether provider is active
     * @return totalMinted Total resources minted
     * @return totalBurned Total resources burned
     * @return totalSlashed Total resources slashed
     * @return reputationScore Provider reputation
     */
    function getProviderStats(address provider) external view returns (
        bool isActive,
        uint256 totalMinted,
        uint256 totalBurned,
        uint256 totalSlashed,
        uint256 reputationScore
    ) {
        Provider storage p = providers[provider];
        return (
            p.isActive,
            p.totalMinted,
            p.totalBurned,
            p.totalSlashed,
            p.reputationScore
        );
    }
    
    /**
     * @notice Check if resource token is valid for use
     * @param tokenId Token ID
     * @return Whether token is valid
     */
    function isResourceValid(uint256 tokenId) external view returns (bool) {
        ResourceSpec memory spec = resourceSpecs[tokenId];
        return spec.isActive && 
               block.timestamp >= spec.validFrom && 
               block.timestamp <= spec.validUntil;
    }
    
    /**
     * @notice Emergency pause
     */
    function pause() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _pause();
    }
    
    /**
     * @notice Unpause
     */
    function unpause() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _unpause();
    }
    
    /**
     * @dev See {IERC165-supportsInterface}
     */
    function supportsInterface(bytes4 interfaceId)
        public
        view
        override(ERC1155, AccessControl)
        returns (bool)
    {
        return super.supportsInterface(interfaceId);
    }
    
    /**
     * @dev Hook that is called before any token transfer
     */
    function _beforeTokenTransfer(
        address operator,
        address from,
        address to,
        uint256[] memory ids,
        uint256[] memory amounts,
        bytes memory data
    ) internal override whenNotPaused {
        super._beforeTokenTransfer(operator, from, to, ids, amounts, data);
        
        // Additional checks can be added here
        for (uint256 i = 0; i < ids.length; i++) {
            ResourceSpec memory spec = resourceSpecs[ids[i]];
            // Ensure tokens are not transferred after expiry
            require(
                block.timestamp <= spec.validUntil || to == address(0),
                "Cannot transfer expired tokens"
            );
        }
    }
} 