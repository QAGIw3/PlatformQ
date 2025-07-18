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
}

interface IInsurancePool {
    function coverLoss(uint256 amount) external;
    function addFunds(uint256 amount) external;
}

interface IOracle {
    function getPrice(address asset) external view returns (uint256);
    function getMultiplePrices(address[] calldata assets) external view returns (uint256[] memory);
}

/**
 * @title DerivativesExchange
 * @notice Main contract for perpetual swaps and derivatives trading
 * @dev Implements multi-tier collateral, partial liquidations, and advanced risk management
 */
contract DerivativesExchange is 
    Initializable,
    AccessControlUpgradeable,
    PausableUpgradeable,
    ReentrancyGuardUpgradeable,
    UUPSUpgradeable
{
    // Roles
    bytes32 public constant ADMIN_ROLE = keccak256("ADMIN_ROLE");
    bytes32 public constant KEEPER_ROLE = keccak256("KEEPER_ROLE");
    bytes32 public constant LIQUIDATOR_ROLE = keccak256("LIQUIDATOR_ROLE");
    bytes32 public constant ORACLE_ROLE = keccak256("ORACLE_ROLE");

    // Market structure
    struct Market {
        address underlying;
        bool isActive;
        uint256 maxLeverage;
        uint256 maintenanceMargin; // basis points (e.g., 50 = 0.5%)
        uint256 initialMargin; // basis points
        uint256 liquidationFee; // basis points
        uint256 makerFee; // can be negative for rebates
        uint256 takerFee;
        uint256 fundingRateLimit; // max funding rate per hour
        uint256 maxPositionSize;
        uint256 openInterestLong;
        uint256 openInterestShort;
    }

    // Position structure
    struct Position {
        address trader;
        uint256 marketId;
        bool isLong;
        uint256 size;
        uint256 collateral;
        uint256 entryPrice;
        uint256 entryFundingAccumulator;
        uint256 lastUpdateTime;
        uint256 realizedPnl;
    }

    // Collateral tier structure
    struct CollateralTier {
        address token;
        uint256 weight; // basis points (10000 = 100%)
        uint256 cap; // max amount in USD
        bool isActive;
        uint8 decimals;
    }

    // Order structure
    struct Order {
        address trader;
        uint256 marketId;
        bool isLong;
        bool isMarket;
        uint256 size;
        uint256 price; // 0 for market orders
        uint256 leverage;
        bool reduceOnly;
        uint256 expiry;
        uint256 nonce;
    }

    // State variables
    mapping(uint256 => Market) public markets;
    mapping(bytes32 => Position) public positions;
    mapping(address => mapping(address => uint256)) public collateralBalances;
    mapping(address => CollateralTier) public collateralTiers;
    mapping(uint256 => uint256) public fundingAccumulators;
    mapping(uint256 => uint256) public lastFundingTime;
    mapping(address => uint256) public userNonces;
    mapping(address => uint256) public reputationScores;
    
    uint256 public nextMarketId;
    uint256 public totalInsuranceFund;
    uint256 public minLiquidationReward;
    uint256 public maxLiquidationReward;
    uint256 public partialLiquidationRatio; // Max % of position to liquidate at once
    
    address public oracle;
    address public insurancePool;
    address public treasury;
    address public platformToken;
    
    // Circuit breaker
    mapping(uint256 => uint256) public marketPriceBreakers;
    uint256 public circuitBreakerThreshold; // % price move to trigger
    
    // Events
    event MarketCreated(uint256 indexed marketId, address underlying);
    event PositionOpened(bytes32 indexed positionId, address indexed trader, uint256 marketId, bool isLong, uint256 size);
    event PositionClosed(bytes32 indexed positionId, address indexed trader, int256 pnl);
    event PositionLiquidated(bytes32 indexed positionId, address indexed liquidator, uint256 liquidatedSize, uint256 remainingSize);
    event CollateralDeposited(address indexed user, address token, uint256 amount);
    event CollateralWithdrawn(address indexed user, address token, uint256 amount);
    event FundingPaid(uint256 indexed marketId, int256 fundingRate, uint256 timestamp);
    event CircuitBreakerTriggered(uint256 indexed marketId, uint256 price);

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    function initialize(
        address _oracle,
        address _insurancePool,
        address _treasury,
        address _platformToken
    ) public initializer {
        __AccessControl_init();
        __Pausable_init();
        __ReentrancyGuard_init();
        __UUPSUpgradeable_init();

        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(ADMIN_ROLE, msg.sender);

        oracle = _oracle;
        insurancePool = _insurancePool;
        treasury = _treasury;
        platformToken = _platformToken;

        minLiquidationReward = 100; // 1%
        maxLiquidationReward = 500; // 5%
        partialLiquidationRatio = 5000; // 50%
        circuitBreakerThreshold = 1000; // 10%
    }

    // Market Management
    function createMarket(
        address underlying,
        uint256 maxLeverage,
        uint256 maintenanceMargin,
        uint256 initialMargin,
        uint256 makerFee,
        uint256 takerFee,
        uint256 maxPositionSize
    ) external onlyRole(ADMIN_ROLE) returns (uint256) {
        uint256 marketId = nextMarketId++;
        
        markets[marketId] = Market({
            underlying: underlying,
            isActive: true,
            maxLeverage: maxLeverage,
            maintenanceMargin: maintenanceMargin,
            initialMargin: initialMargin,
            liquidationFee: 100, // 1% default
            makerFee: makerFee,
            takerFee: takerFee,
            fundingRateLimit: 10, // 0.1% per hour max
            maxPositionSize: maxPositionSize,
            openInterestLong: 0,
            openInterestShort: 0
        });

        emit MarketCreated(marketId, underlying);
        return marketId;
    }

    // Collateral Management
    function addCollateralTier(
        address token,
        uint256 weight,
        uint256 cap
    ) external onlyRole(ADMIN_ROLE) {
        require(weight <= 10000, "Invalid weight");
        
        collateralTiers[token] = CollateralTier({
            token: token,
            weight: weight,
            cap: cap,
            isActive: true,
            decimals: IERC20(token).decimals()
        });
    }

    function depositCollateral(address token, uint256 amount) external nonReentrant whenNotPaused {
        require(collateralTiers[token].isActive, "Invalid collateral");
        
        IERC20(token).transferFrom(msg.sender, address(this), amount);
        collateralBalances[msg.sender][token] += amount;
        
        emit CollateralDeposited(msg.sender, token, amount);
    }

    function withdrawCollateral(address token, uint256 amount) external nonReentrant whenNotPaused {
        require(collateralBalances[msg.sender][token] >= amount, "Insufficient balance");
        require(calculateFreeCollateral(msg.sender) >= amount, "Would make account under-collateralized");
        
        collateralBalances[msg.sender][token] -= amount;
        IERC20(token).transfer(msg.sender, amount);
        
        emit CollateralWithdrawn(msg.sender, token, amount);
    }

    // Trading Functions
    function openPosition(
        uint256 marketId,
        bool isLong,
        uint256 size,
        uint256 leverage,
        uint256 maxSlippage
    ) external nonReentrant whenNotPaused returns (bytes32) {
        Market storage market = markets[marketId];
        require(market.isActive, "Market inactive");
        require(leverage <= market.maxLeverage, "Leverage too high");
        require(size <= market.maxPositionSize, "Position too large");
        
        uint256 price = IOracle(oracle).getPrice(market.underlying);
        require(!_isCircuitBreakerActive(marketId, price), "Circuit breaker active");
        
        uint256 notionalValue = size * price / 1e18;
        uint256 requiredCollateral = notionalValue * market.initialMargin / 10000 / leverage;
        
        require(calculateFreeCollateral(msg.sender) >= requiredCollateral, "Insufficient collateral");
        
        bytes32 positionId = keccak256(abi.encodePacked(msg.sender, marketId, block.timestamp));
        
        positions[positionId] = Position({
            trader: msg.sender,
            marketId: marketId,
            isLong: isLong,
            size: size,
            collateral: requiredCollateral,
            entryPrice: price,
            entryFundingAccumulator: fundingAccumulators[marketId],
            lastUpdateTime: block.timestamp,
            realizedPnl: 0
        });
        
        // Update open interest
        if (isLong) {
            market.openInterestLong += size;
        } else {
            market.openInterestShort += size;
        }
        
        // Lock collateral
        _lockCollateral(msg.sender, requiredCollateral);
        
        emit PositionOpened(positionId, msg.sender, marketId, isLong, size);
        return positionId;
    }

    function closePosition(bytes32 positionId) external nonReentrant whenNotPaused {
        Position storage position = positions[positionId];
        require(position.trader == msg.sender, "Not position owner");
        
        Market storage market = markets[position.marketId];
        uint256 exitPrice = IOracle(oracle).getPrice(market.underlying);
        
        // Calculate PnL
        int256 pnl = calculatePositionPnl(positionId, exitPrice);
        
        // Calculate funding
        int256 fundingPayment = calculateFundingPayment(positionId);
        
        // Total PnL including funding
        int256 totalPnl = pnl - fundingPayment;
        
        // Update open interest
        if (position.isLong) {
            market.openInterestLong -= position.size;
        } else {
            market.openInterestShort -= position.size;
        }
        
        // Settle position
        _settlePosition(positionId, totalPnl);
        
        emit PositionClosed(positionId, msg.sender, totalPnl);
    }

    // Liquidation Functions
    function liquidatePosition(bytes32 positionId) external nonReentrant whenNotPaused {
        Position storage position = positions[positionId];
        require(position.size > 0, "Position does not exist");
        
        uint256 price = IOracle(oracle).getPrice(markets[position.marketId].underlying);
        uint256 healthFactor = calculateHealthFactor(positionId, price);
        
        require(healthFactor < 1e18, "Position healthy");
        
        // Partial liquidation if position is large
        uint256 liquidationSize = position.size;
        if (liquidationSize > 1000e18) { // If position > $1000
            liquidationSize = position.size * partialLiquidationRatio / 10000;
        }
        
        // Calculate liquidation bonus
        uint256 liquidationBonus = calculateLiquidationBonus(position.collateral);
        
        // Execute liquidation
        _executeLiquidation(positionId, liquidationSize, msg.sender, liquidationBonus);
        
        emit PositionLiquidated(positionId, msg.sender, liquidationSize, position.size);
    }

    // Funding Rate Calculation
    function updateFunding(uint256 marketId) external onlyRole(KEEPER_ROLE) {
        Market storage market = markets[marketId];
        require(block.timestamp >= lastFundingTime[marketId] + 1 hours, "Too soon");
        
        uint256 price = IOracle(oracle).getPrice(market.underlying);
        
        // Calculate funding rate based on open interest imbalance
        int256 fundingRate = calculateFundingRate(marketId, price);
        
        // Apply funding rate limits
        if (fundingRate > int256(market.fundingRateLimit)) {
            fundingRate = int256(market.fundingRateLimit);
        } else if (fundingRate < -int256(market.fundingRateLimit)) {
            fundingRate = -int256(market.fundingRateLimit);
        }
        
        // Update accumulator
        fundingAccumulators[marketId] += uint256(fundingRate);
        lastFundingTime[marketId] = block.timestamp;
        
        emit FundingPaid(marketId, fundingRate, block.timestamp);
    }

    // View Functions
    function calculateHealthFactor(bytes32 positionId, uint256 currentPrice) public view returns (uint256) {
        Position memory position = positions[positionId];
        Market memory market = markets[position.marketId];
        
        int256 unrealizedPnl = calculatePositionPnl(positionId, currentPrice);
        int256 equity = int256(position.collateral) + unrealizedPnl;
        
        if (equity <= 0) return 0;
        
        uint256 maintenanceMarginRequired = position.size * currentPrice * market.maintenanceMargin / 1e18 / 10000;
        
        return uint256(equity) * 1e18 / maintenanceMarginRequired;
    }

    function calculatePositionPnl(bytes32 positionId, uint256 currentPrice) public view returns (int256) {
        Position memory position = positions[positionId];
        
        if (position.isLong) {
            return int256((currentPrice - position.entryPrice) * position.size / 1e18);
        } else {
            return int256((position.entryPrice - currentPrice) * position.size / 1e18);
        }
    }

    function calculateFundingPayment(bytes32 positionId) public view returns (int256) {
        Position memory position = positions[positionId];
        
        uint256 fundingDiff = fundingAccumulators[position.marketId] - position.entryFundingAccumulator;
        int256 fundingPayment = int256(fundingDiff * position.size / 1e18);
        
        return position.isLong ? fundingPayment : -fundingPayment;
    }

    function calculateFreeCollateral(address user) public view returns (uint256) {
        uint256 totalCollateralValue = 0;
        
        // Calculate total collateral value with weights
        address[] memory tokens = getCollateralTokens();
        for (uint256 i = 0; i < tokens.length; i++) {
            if (collateralBalances[user][tokens[i]] > 0) {
                uint256 tokenPrice = IOracle(oracle).getPrice(tokens[i]);
                uint256 value = collateralBalances[user][tokens[i]] * tokenPrice / 10**collateralTiers[tokens[i]].decimals;
                uint256 weightedValue = value * collateralTiers[tokens[i]].weight / 10000;
                totalCollateralValue += weightedValue;
            }
        }
        
        // Add reputation-based credit if applicable
        uint256 reputationCredit = calculateReputationCredit(user);
        totalCollateralValue += reputationCredit;
        
        // Subtract used collateral from all positions
        uint256 usedCollateral = calculateUsedCollateral(user);
        
        if (totalCollateralValue > usedCollateral) {
            return totalCollateralValue - usedCollateral;
        }
        return 0;
    }

    function calculateReputationCredit(address user) public view returns (uint256) {
        uint256 score = reputationScores[user];
        
        // Reputation tiers with credit limits
        if (score >= 1000) {
            return 1000000e18; // $1M credit for top tier
        } else if (score >= 500) {
            return 100000e18; // $100k credit
        } else if (score >= 100) {
            return 10000e18; // $10k credit
        }
        return 0;
    }

    function calculateLiquidationBonus(uint256 collateral) public view returns (uint256) {
        uint256 bonus = collateral * minLiquidationReward / 10000;
        uint256 maxBonus = collateral * maxLiquidationReward / 10000;
        
        // Scale bonus based on position size
        if (collateral > 100000e18) {
            bonus = maxBonus;
        } else {
            bonus = bonus + (maxBonus - bonus) * collateral / 100000e18;
        }
        
        return bonus;
    }

    function calculateFundingRate(uint256 marketId, uint256 price) public view returns (int256) {
        Market memory market = markets[marketId];
        
        // Calculate imbalance
        int256 imbalance = int256(market.openInterestLong) - int256(market.openInterestShort);
        int256 totalOI = int256(market.openInterestLong + market.openInterestShort);
        
        if (totalOI == 0) return 0;
        
        // Funding rate = imbalance / total OI * base rate
        int256 fundingRate = imbalance * 1e18 / totalOI * 10 / 10000; // 0.1% base rate
        
        return fundingRate;
    }

    function getCollateralTokens() public pure returns (address[] memory) {
        // This should be implemented to return active collateral tokens
        // For now, returning empty array
        return new address[](0);
    }

    function calculateUsedCollateral(address user) public view returns (uint256) {
        // This should sum up collateral used in all user's positions
        // Implementation depends on position tracking mechanism
        return 0;
    }

    // Internal Functions
    function _lockCollateral(address user, uint256 amount) internal {
        // Lock collateral from user's balances
        // Implementation depends on multi-collateral logic
    }

    function _settlePosition(bytes32 positionId, int256 totalPnl) internal {
        Position storage position = positions[positionId];
        
        if (totalPnl > 0) {
            // Profit - release collateral + profit
            _releaseCollateral(position.trader, position.collateral + uint256(totalPnl));
        } else if (totalPnl < 0) {
            uint256 loss = uint256(-totalPnl);
            if (loss < position.collateral) {
                // Partial loss - release remaining collateral
                _releaseCollateral(position.trader, position.collateral - loss);
            } else {
                // Total loss - insurance fund covers excess
                uint256 insuranceLoss = loss - position.collateral;
                IInsurancePool(insurancePool).coverLoss(insuranceLoss);
            }
        } else {
            // Break even - release collateral
            _releaseCollateral(position.trader, position.collateral);
        }
        
        delete positions[positionId];
    }

    function _releaseCollateral(address user, uint256 amount) internal {
        // Release collateral back to user
        // Implementation depends on multi-collateral logic
    }

    function _executeLiquidation(
        bytes32 positionId,
        uint256 liquidationSize,
        address liquidator,
        uint256 bonus
    ) internal {
        Position storage position = positions[positionId];
        
        // Reduce position size
        position.size -= liquidationSize;
        
        // Calculate collateral to liquidate
        uint256 collateralToLiquidate = position.collateral * liquidationSize / (position.size + liquidationSize);
        position.collateral -= collateralToLiquidate;
        
        // Pay liquidator
        _releaseCollateral(liquidator, collateralToLiquidate + bonus);
        
        // If position fully liquidated, delete it
        if (position.size == 0) {
            delete positions[positionId];
        }
    }

    function _isCircuitBreakerActive(uint256 marketId, uint256 currentPrice) internal view returns (bool) {
        uint256 lastPrice = marketPriceBreakers[marketId];
        if (lastPrice == 0) return false;
        
        uint256 priceChange = currentPrice > lastPrice ? 
            (currentPrice - lastPrice) * 10000 / lastPrice :
            (lastPrice - currentPrice) * 10000 / lastPrice;
            
        return priceChange > circuitBreakerThreshold;
    }

    // Admin Functions
    function pause() external onlyRole(ADMIN_ROLE) {
        _pause();
    }

    function unpause() external onlyRole(ADMIN_ROLE) {
        _unpause();
    }

    function _authorizeUpgrade(address newImplementation) internal override onlyRole(ADMIN_ROLE) {}
} 