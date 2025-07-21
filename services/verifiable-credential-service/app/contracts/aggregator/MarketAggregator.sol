// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/security/Pausable.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";

/**
 * @title MarketAggregator
 * @notice Manages bundled resource allocations across quantum, AI, and network markets
 */
contract MarketAggregator is AccessControl, ReentrancyGuard, Pausable {
    bytes32 public constant OPERATOR_ROLE = keccak256("OPERATOR_ROLE");
    bytes32 public constant ARBITRAGEUR_ROLE = keccak256("ARBITRAGEUR_ROLE");
    
    // Resource types
    enum ResourceType {
        QUANTUM,
        AI,
        NETWORK
    }
    
    // Bundle status
    enum BundleStatus {
        PENDING,
        ACTIVE,
        EXPIRED,
        CANCELLED
    }
    
    // Bundle structure
    struct Bundle {
        string bundleId;
        address creator;
        ResourceType[] resourceTypes;
        uint256[] resourceIds;
        uint256 totalCost;
        uint256 bundleDiscount;
        uint256 finalCost;
        uint256 createdAt;
        uint256 expiresAt;
        BundleStatus status;
    }
    
    // Arbitrage opportunity
    struct ArbitrageOpportunity {
        string opportunityId;
        ResourceType resourceType;
        uint256 resourceId;
        address marketA;
        address marketB;
        uint256 priceA;
        uint256 priceB;
        uint256 quantity;
        uint256 potentialProfit;
        uint256 expiresAt;
        bool executed;
    }
    
    // Storage
    mapping(string => Bundle) public bundles;
    mapping(string => ArbitrageOpportunity) public arbitrageOpportunities;
    mapping(address => string[]) public userBundles;
    mapping(address => uint256) public arbitrageProfits;
    
    // Market contracts
    address public quantumMarket;
    address public aiMarket;
    address public networkMarket;
    
    // Payment token
    IERC20 public paymentToken;
    
    // Configuration
    uint256 public baseBundleDiscount = 500; // 5%
    uint256 public crossResourceDiscount = 300; // 3%
    uint256 public minArbitrageProfitBps = 200; // 2%
    uint256 public arbitrageFee = 1000; // 10% of profit
    
    // Events
    event BundleCreated(
        string indexed bundleId,
        address indexed creator,
        uint256 totalCost,
        uint256 finalCost
    );
    
    event BundleAllocated(
        string indexed bundleId,
        address indexed user,
        uint256 payment
    );
    
    event BundleExpired(string indexed bundleId);
    
    event ArbitrageOpportunityFound(
        string indexed opportunityId,
        ResourceType resourceType,
        uint256 potentialProfit
    );
    
    event ArbitrageExecuted(
        string indexed opportunityId,
        address indexed executor,
        uint256 actualProfit,
        uint256 fee
    );
    
    constructor(
        address _paymentToken,
        address _quantumMarket,
        address _aiMarket,
        address _networkMarket
    ) {
        paymentToken = IERC20(_paymentToken);
        quantumMarket = _quantumMarket;
        aiMarket = _aiMarket;
        networkMarket = _networkMarket;
        
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(OPERATOR_ROLE, msg.sender);
    }
    
    /**
     * @notice Create a resource bundle
     * @param bundleId Unique bundle identifier
     * @param resourceTypes Array of resource types
     * @param resourceIds Array of resource IDs
     * @param costs Array of individual resource costs
     * @param duration Bundle duration in seconds
     */
    function createBundle(
        string memory bundleId,
        ResourceType[] memory resourceTypes,
        uint256[] memory resourceIds,
        uint256[] memory costs,
        uint256 duration
    ) external whenNotPaused {
        require(bytes(bundleId).length > 0, "Invalid bundle ID");
        require(
            resourceTypes.length == resourceIds.length &&
            resourceTypes.length == costs.length,
            "Array length mismatch"
        );
        require(resourceTypes.length > 0, "Empty bundle");
        require(duration > 0, "Invalid duration");
        
        // Calculate total cost
        uint256 totalCost = 0;
        for (uint256 i = 0; i < costs.length; i++) {
            totalCost += costs[i];
        }
        
        // Calculate discount
        uint256 discount = baseBundleDiscount;
        
        // Additional discount for cross-resource bundles
        if (_hasCrossResources(resourceTypes)) {
            discount += crossResourceDiscount;
        }
        
        uint256 finalCost = totalCost * (10000 - discount) / 10000;
        
        // Create bundle
        bundles[bundleId] = Bundle({
            bundleId: bundleId,
            creator: msg.sender,
            resourceTypes: resourceTypes,
            resourceIds: resourceIds,
            totalCost: totalCost,
            bundleDiscount: discount,
            finalCost: finalCost,
            createdAt: block.timestamp,
            expiresAt: block.timestamp + duration,
            status: BundleStatus.PENDING
        });
        
        userBundles[msg.sender].push(bundleId);
        
        emit BundleCreated(bundleId, msg.sender, totalCost, finalCost);
    }
    
    /**
     * @notice Allocate and pay for a bundle
     * @param bundleId Bundle to allocate
     */
    function allocateBundle(
        string memory bundleId
    ) external nonReentrant whenNotPaused {
        Bundle storage bundle = bundles[bundleId];
        require(bundle.createdAt > 0, "Bundle not found");
        require(bundle.status == BundleStatus.PENDING, "Bundle not pending");
        require(block.timestamp < bundle.expiresAt, "Bundle expired");
        
        // Transfer payment
        require(
            paymentToken.transferFrom(msg.sender, address(this), bundle.finalCost),
            "Payment failed"
        );
        
        // Allocate resources in each market
        for (uint256 i = 0; i < bundle.resourceTypes.length; i++) {
            _allocateResource(
                bundle.resourceTypes[i],
                bundle.resourceIds[i],
                msg.sender
            );
        }
        
        bundle.status = BundleStatus.ACTIVE;
        
        emit BundleAllocated(bundleId, msg.sender, bundle.finalCost);
    }
    
    /**
     * @notice Record an arbitrage opportunity
     * @param opportunityId Unique opportunity identifier
     * @param resourceType Type of resource
     * @param resourceId Resource identifier
     * @param marketA First market address
     * @param marketB Second market address
     * @param priceA Price in market A
     * @param priceB Price in market B
     * @param quantity Quantity available
     * @param duration Opportunity duration
     */
    function recordArbitrageOpportunity(
        string memory opportunityId,
        ResourceType resourceType,
        uint256 resourceId,
        address marketA,
        address marketB,
        uint256 priceA,
        uint256 priceB,
        uint256 quantity,
        uint256 duration
    ) external onlyRole(OPERATOR_ROLE) {
        require(bytes(opportunityId).length > 0, "Invalid opportunity ID");
        require(marketA != marketB, "Same market");
        require(priceA != priceB, "No price difference");
        
        uint256 potentialProfit;
        if (priceB > priceA) {
            potentialProfit = (priceB - priceA) * quantity;
        } else {
            potentialProfit = (priceA - priceB) * quantity;
        }
        
        // Check minimum profit threshold
        uint256 minProfit = ((priceA < priceB ? priceA : priceB) * quantity * minArbitrageProfitBps) / 10000;
        require(potentialProfit >= minProfit, "Profit too low");
        
        arbitrageOpportunities[opportunityId] = ArbitrageOpportunity({
            opportunityId: opportunityId,
            resourceType: resourceType,
            resourceId: resourceId,
            marketA: marketA,
            marketB: marketB,
            priceA: priceA,
            priceB: priceB,
            quantity: quantity,
            potentialProfit: potentialProfit,
            expiresAt: block.timestamp + duration,
            executed: false
        });
        
        emit ArbitrageOpportunityFound(opportunityId, resourceType, potentialProfit);
    }
    
    /**
     * @notice Execute an arbitrage opportunity
     * @param opportunityId Opportunity to execute
     */
    function executeArbitrage(
        string memory opportunityId
    ) external onlyRole(ARBITRAGEUR_ROLE) nonReentrant whenNotPaused {
        ArbitrageOpportunity storage opp = arbitrageOpportunities[opportunityId];
        require(opp.potentialProfit > 0, "Opportunity not found");
        require(!opp.executed, "Already executed");
        require(block.timestamp < opp.expiresAt, "Opportunity expired");
        
        // Mark as executed
        opp.executed = true;
        
        // Calculate fee
        uint256 fee = (opp.potentialProfit * arbitrageFee) / 10000;
        uint256 netProfit = opp.potentialProfit - fee;
        
        // Record profit
        arbitrageProfits[msg.sender] += netProfit;
        
        // In production, would execute actual trades
        // For now, just transfer profit
        require(
            paymentToken.transfer(msg.sender, netProfit),
            "Profit transfer failed"
        );
        
        emit ArbitrageExecuted(opportunityId, msg.sender, netProfit, fee);
    }
    
    /**
     * @notice Cancel an expired bundle
     * @param bundleId Bundle to cancel
     */
    function cancelExpiredBundle(string memory bundleId) external {
        Bundle storage bundle = bundles[bundleId];
        require(bundle.createdAt > 0, "Bundle not found");
        require(bundle.status == BundleStatus.PENDING, "Bundle not pending");
        require(block.timestamp >= bundle.expiresAt, "Bundle not expired");
        
        bundle.status = BundleStatus.EXPIRED;
        
        emit BundleExpired(bundleId);
    }
    
    /**
     * @notice Get bundle details
     * @param bundleId Bundle identifier
     */
    function getBundle(string memory bundleId) external view returns (
        address creator,
        uint256 totalCost,
        uint256 finalCost,
        uint256 expiresAt,
        BundleStatus status,
        ResourceType[] memory resourceTypes,
        uint256[] memory resourceIds
    ) {
        Bundle storage bundle = bundles[bundleId];
        return (
            bundle.creator,
            bundle.totalCost,
            bundle.finalCost,
            bundle.expiresAt,
            bundle.status,
            bundle.resourceTypes,
            bundle.resourceIds
        );
    }
    
    /**
     * @notice Get user's bundles
     * @param user User address
     */
    function getUserBundles(address user) external view returns (string[] memory) {
        return userBundles[user];
    }
    
    /**
     * @notice Update configuration
     * @param _baseBundleDiscount Base bundle discount in basis points
     * @param _crossResourceDiscount Cross-resource discount in basis points
     * @param _minArbitrageProfitBps Minimum arbitrage profit in basis points
     * @param _arbitrageFee Arbitrage fee in basis points
     */
    function updateConfiguration(
        uint256 _baseBundleDiscount,
        uint256 _crossResourceDiscount,
        uint256 _minArbitrageProfitBps,
        uint256 _arbitrageFee
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(_baseBundleDiscount <= 2000, "Discount too high"); // Max 20%
        require(_crossResourceDiscount <= 1000, "Discount too high"); // Max 10%
        require(_arbitrageFee <= 5000, "Fee too high"); // Max 50%
        
        baseBundleDiscount = _baseBundleDiscount;
        crossResourceDiscount = _crossResourceDiscount;
        minArbitrageProfitBps = _minArbitrageProfitBps;
        arbitrageFee = _arbitrageFee;
    }
    
    /**
     * @notice Update market contracts
     * @param _quantumMarket Quantum market contract
     * @param _aiMarket AI market contract
     * @param _networkMarket Network market contract
     */
    function updateMarkets(
        address _quantumMarket,
        address _aiMarket,
        address _networkMarket
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(_quantumMarket != address(0), "Invalid quantum market");
        require(_aiMarket != address(0), "Invalid AI market");
        require(_networkMarket != address(0), "Invalid network market");
        
        quantumMarket = _quantumMarket;
        aiMarket = _aiMarket;
        networkMarket = _networkMarket;
    }
    
    /**
     * @notice Pause contract
     */
    function pause() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _pause();
    }
    
    /**
     * @notice Unpause contract
     */
    function unpause() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _unpause();
    }
    
    /**
     * @notice Withdraw accumulated fees
     * @param to Recipient address
     * @param amount Amount to withdraw
     */
    function withdrawFees(
        address to,
        uint256 amount
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(paymentToken.transfer(to, amount), "Transfer failed");
    }
    
    // Internal functions
    
    function _hasCrossResources(
        ResourceType[] memory resourceTypes
    ) private pure returns (bool) {
        if (resourceTypes.length < 2) return false;
        
        for (uint256 i = 1; i < resourceTypes.length; i++) {
            if (resourceTypes[i] != resourceTypes[0]) {
                return true;
            }
        }
        return false;
    }
    
    function _allocateResource(
        ResourceType resourceType,
        uint256 resourceId,
        address user
    ) private {
        // In production, would call respective market contracts
        // For now, just emit events or state changes
        if (resourceType == ResourceType.QUANTUM) {
            // IQuantumMarket(quantumMarket).allocate(resourceId, user);
        } else if (resourceType == ResourceType.AI) {
            // IAIMarket(aiMarket).allocate(resourceId, user);
        } else if (resourceType == ResourceType.NETWORK) {
            // INetworkMarket(networkMarket).allocate(resourceId, user);
        }
    }
} 