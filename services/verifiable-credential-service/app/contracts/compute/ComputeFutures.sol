// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts-upgradeable/access/AccessControlUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/security/PausableUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/security/ReentrancyGuardUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";
import "@chainlink/contracts/src/v0.8/interfaces/AggregatorV3Interface.sol";

interface IERC20 {
    function transfer(address to, uint256 amount) external returns (bool);
    function transferFrom(address from, address to, uint256 amount) external returns (bool);
    function balanceOf(address account) external view returns (uint256);
    function decimals() external view returns (uint8);
    function approve(address spender, uint256 amount) external returns (bool);
}

interface IComputeRegistry {
    function registerResource(string memory resourceId, address provider, uint256 capacity) external;
    function allocateResource(string memory resourceId, address user, uint256 amount) external;
    function releaseResource(string memory resourceId, address user, uint256 amount) external;
    function getResourceInfo(string memory resourceId) external view returns (address provider, uint256 capacity, uint256 allocated);
}

/**
 * @title ComputeFutures
 * @notice Handles futures contracts for compute resources with physical settlement
 * @dev Implements day-ahead markets, capacity auctions, and SLA enforcement
 */
contract ComputeFutures is 
    Initializable,
    AccessControlUpgradeable,
    PausableUpgradeable,
    ReentrancyGuardUpgradeable,
    UUPSUpgradeable 
{
    bytes32 public constant OPERATOR_ROLE = keccak256("OPERATOR_ROLE");
    bytes32 public constant ORACLE_ROLE = keccak256("ORACLE_ROLE");
    
    enum ResourceType {
        GPU,
        CPU,
        MEMORY,
        STORAGE,
        BANDWIDTH
    }
    
    enum ContractStatus {
        PENDING,
        ACTIVE,
        SETTLED,
        DEFAULTED,
        CANCELLED
    }
    
    struct FuturesContract {
        uint256 contractId;
        address buyer;
        address seller;
        ResourceType resourceType;
        uint256 quantity; // Units depend on resource type
        uint256 price; // Price per unit in settlement token
        uint256 deliveryTime;
        uint256 duration; // Duration of resource usage
        ContractStatus status;
        uint256 collateralAmount;
        string resourceSpecification; // JSON spec for detailed requirements
        bool physicallySettled;
        uint256 slaLevel; // 0-100 representing uptime percentage
        uint256 actualPerformance; // Actual performance delivered
    }
    
    struct DayAheadMarket {
        uint256 marketId;
        uint256 deliveryHour; // Unix timestamp of delivery hour
        ResourceType resourceType;
        mapping(address => uint256) bids; // User -> bid quantity
        mapping(address => uint256) offers; // Provider -> offer quantity
        mapping(address => uint256) bidPrices; // User -> max price willing to pay
        mapping(address => uint256) offerPrices; // Provider -> min price willing to accept
        uint256 clearingPrice;
        bool isCleared;
        uint256 totalBidQuantity;
        uint256 totalOfferQuantity;
    }
    
    struct QualityDerivative {
        uint256 derivativeId;
        uint256 futuresContractId;
        string derivativeType; // "latency", "uptime", "performance"
        uint256 strikeValue; // Target metric value
        uint256 premium;
        address buyer;
        bool isExercised;
        uint256 payout;
    }
    
    // State variables
    IERC20 public settlementToken;
    IComputeRegistry public computeRegistry;
    AggregatorV3Interface public priceOracle;
    
    uint256 public nextContractId;
    uint256 public nextMarketId;
    uint256 public nextDerivativeId;
    
    mapping(uint256 => FuturesContract) public futuresContracts;
    mapping(uint256 => DayAheadMarket) public dayAheadMarkets;
    mapping(uint256 => QualityDerivative) public qualityDerivatives;
    
    mapping(address => uint256[]) public userContracts;
    mapping(address => uint256[]) public providerContracts;
    mapping(address => uint256) public userCollateral;
    mapping(address => uint256) public providerReputation;
    
    // Market parameters
    uint256 public minContractDuration = 1 hours;
    uint256 public maxContractDuration = 30 days;
    uint256 public collateralRatio = 120; // 120% collateral requirement
    uint256 public slaPenaltyRate = 10; // 10% penalty per 1% SLA breach
    uint256 public settlementGracePeriod = 1 hours;
    
    // Events
    event ContractCreated(
        uint256 indexed contractId,
        address indexed buyer,
        address indexed seller,
        ResourceType resourceType,
        uint256 quantity,
        uint256 price,
        uint256 deliveryTime
    );
    
    event ContractSettled(
        uint256 indexed contractId,
        bool physicallySettled,
        uint256 actualPerformance
    );
    
    event DayAheadMarketCreated(
        uint256 indexed marketId,
        uint256 deliveryHour,
        ResourceType resourceType
    );
    
    event MarketCleared(
        uint256 indexed marketId,
        uint256 clearingPrice,
        uint256 clearedQuantity
    );
    
    event QualityDerivativeCreated(
        uint256 indexed derivativeId,
        uint256 indexed futuresContractId,
        string derivativeType,
        uint256 strikeValue
    );
    
    event SLABreach(
        uint256 indexed contractId,
        uint256 expectedPerformance,
        uint256 actualPerformance,
        uint256 penaltyAmount
    );
    
    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }
    
    function initialize(
        address _settlementToken,
        address _computeRegistry,
        address _priceOracle
    ) public initializer {
        __AccessControl_init();
        __Pausable_init();
        __ReentrancyGuard_init();
        __UUPSUpgradeable_init();
        
        settlementToken = IERC20(_settlementToken);
        computeRegistry = IComputeRegistry(_computeRegistry);
        priceOracle = AggregatorV3Interface(_priceOracle);
        
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(OPERATOR_ROLE, msg.sender);
    }
    
    /**
     * @notice Create a futures contract for compute resources
     */
    function createFuturesContract(
        address seller,
        ResourceType resourceType,
        uint256 quantity,
        uint256 price,
        uint256 deliveryTime,
        uint256 duration,
        string memory resourceSpec,
        uint256 slaLevel
    ) external nonReentrant whenNotPaused returns (uint256) {
        require(deliveryTime > block.timestamp, "Delivery time must be in future");
        require(duration >= minContractDuration && duration <= maxContractDuration, "Invalid duration");
        require(slaLevel > 0 && slaLevel <= 100, "Invalid SLA level");
        
        uint256 contractValue = quantity * price;
        uint256 requiredCollateral = (contractValue * collateralRatio) / 100;
        
        // Transfer collateral from buyer
        require(
            settlementToken.transferFrom(msg.sender, address(this), requiredCollateral),
            "Collateral transfer failed"
        );
        
        uint256 contractId = nextContractId++;
        
        futuresContracts[contractId] = FuturesContract({
            contractId: contractId,
            buyer: msg.sender,
            seller: seller,
            resourceType: resourceType,
            quantity: quantity,
            price: price,
            deliveryTime: deliveryTime,
            duration: duration,
            status: ContractStatus.PENDING,
            collateralAmount: requiredCollateral,
            resourceSpecification: resourceSpec,
            physicallySettled: false,
            slaLevel: slaLevel,
            actualPerformance: 0
        });
        
        userContracts[msg.sender].push(contractId);
        providerContracts[seller].push(contractId);
        
        emit ContractCreated(contractId, msg.sender, seller, resourceType, quantity, price, deliveryTime);
        
        return contractId;
    }
    
    /**
     * @notice Create a day-ahead market for a specific delivery hour
     */
    function createDayAheadMarket(
        uint256 deliveryHour,
        ResourceType resourceType
    ) external onlyRole(OPERATOR_ROLE) returns (uint256) {
        require(deliveryHour > block.timestamp, "Delivery hour must be in future");
        require(deliveryHour % 3600 == 0, "Must be start of hour");
        
        uint256 marketId = nextMarketId++;
        
        DayAheadMarket storage market = dayAheadMarkets[marketId];
        market.marketId = marketId;
        market.deliveryHour = deliveryHour;
        market.resourceType = resourceType;
        market.isCleared = false;
        
        emit DayAheadMarketCreated(marketId, deliveryHour, resourceType);
        
        return marketId;
    }
    
    /**
     * @notice Submit a bid to day-ahead market
     */
    function submitBid(
        uint256 marketId,
        uint256 quantity,
        uint256 maxPrice
    ) external nonReentrant whenNotPaused {
        DayAheadMarket storage market = dayAheadMarkets[marketId];
        require(block.timestamp < market.deliveryHour - 1 hours, "Bidding closed");
        require(!market.isCleared, "Market already cleared");
        
        market.bids[msg.sender] = quantity;
        market.bidPrices[msg.sender] = maxPrice;
        market.totalBidQuantity += quantity;
    }
    
    /**
     * @notice Submit an offer to day-ahead market
     */
    function submitOffer(
        uint256 marketId,
        uint256 quantity,
        uint256 minPrice
    ) external nonReentrant whenNotPaused {
        DayAheadMarket storage market = dayAheadMarkets[marketId];
        require(block.timestamp < market.deliveryHour - 1 hours, "Offering closed");
        require(!market.isCleared, "Market already cleared");
        
        market.offers[msg.sender] = quantity;
        market.offerPrices[msg.sender] = minPrice;
        market.totalOfferQuantity += quantity;
    }
    
    /**
     * @notice Clear the day-ahead market and match bids with offers
     */
    function clearDayAheadMarket(uint256 marketId) external onlyRole(OPERATOR_ROLE) {
        DayAheadMarket storage market = dayAheadMarkets[marketId];
        require(!market.isCleared, "Market already cleared");
        require(block.timestamp >= market.deliveryHour - 1 hours, "Too early to clear");
        
        // Simple uniform price auction clearing
        // In production, this would use more sophisticated algorithms
        uint256 clearingPrice = _findClearingPrice(market);
        market.clearingPrice = clearingPrice;
        market.isCleared = true;
        
        // Create futures contracts for matched pairs
        // This is simplified - real implementation would be more complex
        _matchBidsAndOffers(market, clearingPrice);
        
        emit MarketCleared(marketId, clearingPrice, market.totalBidQuantity);
    }
    
    /**
     * @notice Settle a futures contract with physical delivery
     */
    function settleContract(
        uint256 contractId,
        string memory resourceAllocationId,
        uint256 actualPerformance
    ) external onlyRole(OPERATOR_ROLE) {
        FuturesContract storage futuresContract = futuresContracts[contractId];
        require(futuresContract.status == ContractStatus.ACTIVE, "Contract not active");
        require(block.timestamp >= futuresContract.deliveryTime, "Too early to settle");
        
        futuresContract.actualPerformance = actualPerformance;
        
        // Allocate resources through registry
        computeRegistry.allocateResource(
            resourceAllocationId,
            futuresContract.buyer,
            futuresContract.quantity
        );
        
        // Calculate SLA penalties if performance is below threshold
        uint256 penalty = 0;
        if (actualPerformance < futuresContract.slaLevel) {
            uint256 breach = futuresContract.slaLevel - actualPerformance;
            penalty = (futuresContract.collateralAmount * breach * slaPenaltyRate) / 10000;
            
            emit SLABreach(contractId, futuresContract.slaLevel, actualPerformance, penalty);
        }
        
        // Transfer payment minus penalties
        uint256 paymentAmount = (futuresContract.quantity * futuresContract.price) - penalty;
        settlementToken.transfer(futuresContract.seller, paymentAmount);
        
        // Return remaining collateral to buyer
        uint256 remainingCollateral = futuresContract.collateralAmount - paymentAmount;
        if (remainingCollateral > 0) {
            settlementToken.transfer(futuresContract.buyer, remainingCollateral);
        }
        
        futuresContract.status = ContractStatus.SETTLED;
        futuresContract.physicallySettled = true;
        
        emit ContractSettled(contractId, true, actualPerformance);
    }
    
    /**
     * @notice Create a quality derivative on a futures contract
     */
    function createQualityDerivative(
        uint256 futuresContractId,
        string memory derivativeType,
        uint256 strikeValue,
        uint256 premium
    ) external nonReentrant whenNotPaused returns (uint256) {
        FuturesContract storage futuresContract = futuresContracts[futuresContractId];
        require(futuresContract.status == ContractStatus.ACTIVE, "Contract not active");
        
        // Transfer premium
        require(
            settlementToken.transferFrom(msg.sender, address(this), premium),
            "Premium transfer failed"
        );
        
        uint256 derivativeId = nextDerivativeId++;
        
        qualityDerivatives[derivativeId] = QualityDerivative({
            derivativeId: derivativeId,
            futuresContractId: futuresContractId,
            derivativeType: derivativeType,
            strikeValue: strikeValue,
            premium: premium,
            buyer: msg.sender,
            isExercised: false,
            payout: 0
        });
        
        emit QualityDerivativeCreated(derivativeId, futuresContractId, derivativeType, strikeValue);
        
        return derivativeId;
    }
    
    /**
     * @notice Exercise a quality derivative after contract settlement
     */
    function exerciseQualityDerivative(uint256 derivativeId) external nonReentrant {
        QualityDerivative storage derivative = qualityDerivatives[derivativeId];
        require(!derivative.isExercised, "Already exercised");
        require(derivative.buyer == msg.sender, "Not derivative owner");
        
        FuturesContract storage futuresContract = futuresContracts[derivative.futuresContractId];
        require(futuresContract.status == ContractStatus.SETTLED, "Contract not settled");
        
        // Calculate payout based on actual performance vs strike
        uint256 payout = _calculateDerivativePayout(derivative, futuresContract);
        
        if (payout > 0) {
            derivative.payout = payout;
            derivative.isExercised = true;
            settlementToken.transfer(msg.sender, payout);
        }
    }
    
    /**
     * @notice Emergency pause
     */
    function pause() external onlyRole(OPERATOR_ROLE) {
        _pause();
    }
    
    function unpause() external onlyRole(OPERATOR_ROLE) {
        _unpause();
    }
    
    // Internal functions
    
    function _findClearingPrice(DayAheadMarket storage market) internal view returns (uint256) {
        // Simplified clearing price calculation
        // In production, use proper auction algorithms
        return (market.bidPrices[address(0)] + market.offerPrices[address(0)]) / 2;
    }
    
    function _matchBidsAndOffers(DayAheadMarket storage market, uint256 clearingPrice) internal {
        // Simplified matching logic
        // Real implementation would properly match bids and offers
    }
    
    function _calculateDerivativePayout(
        QualityDerivative storage derivative,
        FuturesContract storage futuresContract
    ) internal pure returns (uint256) {
        // Calculate payout based on derivative type and performance
        if (futuresContract.actualPerformance < derivative.strikeValue) {
            return derivative.premium * 2; // Simplified payout
        }
        return 0;
    }
    
    function _authorizeUpgrade(address newImplementation) internal override onlyRole(DEFAULT_ADMIN_ROLE) {}
} 