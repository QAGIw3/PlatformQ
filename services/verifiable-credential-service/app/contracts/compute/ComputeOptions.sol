// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts-upgradeable/access/AccessControlUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/security/PausableUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/security/ReentrancyGuardUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";
import "@chainlink/contracts/src/v0.8/interfaces/AggregatorV3Interface.sol";

interface IComputeFutures {
    function createFuturesContract(
        address seller,
        uint8 resourceType,
        uint256 quantity,
        uint256 price,
        uint256 deliveryTime,
        uint256 duration,
        string memory resourceSpec,
        uint256 slaLevel
    ) external returns (uint256);
}

interface IERC20 {
    function transfer(address to, uint256 amount) external returns (bool);
    function transferFrom(address from, address to, uint256 amount) external returns (bool);
    function balanceOf(address account) external view returns (uint256);
}

/**
 * @title ComputeOptions
 * @notice Options trading for compute resources with automatic exercise and AMM liquidity
 * @dev Implements call/put/burst/throttle options with various exercise styles
 */
contract ComputeOptions is 
    Initializable,
    AccessControlUpgradeable,
    PausableUpgradeable,
    ReentrancyGuardUpgradeable,
    UUPSUpgradeable 
{
    bytes32 public constant OPERATOR_ROLE = keccak256("OPERATOR_ROLE");
    bytes32 public constant MARKET_MAKER_ROLE = keccak256("MARKET_MAKER_ROLE");
    
    enum OptionType {
        CALL,
        PUT,
        BURST,
        THROTTLE
    }
    
    enum ExerciseStyle {
        EUROPEAN,
        AMERICAN,
        ASIAN,
        BERMUDAN
    }
    
    enum OptionStatus {
        ACTIVE,
        EXERCISED,
        EXPIRED,
        CANCELLED
    }
    
    struct ComputeOption {
        uint256 optionId;
        address writer;
        address holder;
        OptionType optionType;
        ExerciseStyle exerciseStyle;
        uint256 strikePrice;
        uint256 quantity; // Amount of compute resources
        uint256 expiry;
        uint256 premium;
        uint8 resourceType; // 0=GPU, 1=CPU, 2=MEMORY, etc
        OptionStatus status;
        bool isLong;
        uint256 collateral;
        uint256[] exerciseDates; // For Bermudan style
        uint256 averagePrice; // For Asian style
        uint256 burstMultiplier; // For burst options
    }
    
    struct Greeks {
        int256 delta;
        int256 gamma;
        int256 vega;
        int256 theta;
        int256 rho;
    }
    
    struct VolatilityPoint {
        uint256 strike;
        uint256 expiry;
        uint256 impliedVol;
        uint256 timestamp;
    }
    
    // State variables
    IERC20 public settlementToken;
    IComputeFutures public computeFutures;
    AggregatorV3Interface public priceOracle;
    
    uint256 public nextOptionId;
    mapping(uint256 => ComputeOption) public options;
    mapping(address => uint256[]) public userOptions;
    mapping(uint256 => Greeks) public optionGreeks;
    
    // AMM pools for each resource type
    mapping(uint8 => mapping(uint256 => uint256)) public liquidityPools; // resourceType -> expiry -> liquidity
    mapping(uint8 => VolatilityPoint[]) public volatilitySurface; // resourceType -> vol points
    
    // Risk parameters
    uint256 public maxOptionSize = 1000; // Max units per option
    uint256 public minPremium = 1e15; // Minimum premium in wei
    uint256 public collateralRatio = 150; // 150% collateral for writers
    uint256 public exerciseFee = 1e16; // Exercise fee
    
    // Market parameters
    uint256 public baseVolatility = 3000; // 30% in basis points
    uint256 public riskFreeRate = 500; // 5% in basis points
    uint256 public constant VOLATILITY_PRECISION = 10000;
    uint256 public constant PRICE_PRECISION = 1e18;
    
    // Events
    event OptionCreated(
        uint256 indexed optionId,
        address indexed writer,
        OptionType optionType,
        uint256 strikePrice,
        uint256 expiry,
        uint256 premium
    );
    
    event OptionPurchased(
        uint256 indexed optionId,
        address indexed buyer,
        uint256 premium
    );
    
    event OptionExercised(
        uint256 indexed optionId,
        address indexed exerciser,
        uint256 settlementAmount
    );
    
    event OptionExpired(uint256 indexed optionId);
    
    event LiquidityAdded(
        uint8 indexed resourceType,
        uint256 indexed expiry,
        uint256 amount
    );
    
    event VolatilityUpdated(
        uint8 indexed resourceType,
        uint256 strike,
        uint256 expiry,
        uint256 impliedVol
    );
    
    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }
    
    function initialize(
        address _settlementToken,
        address _computeFutures,
        address _priceOracle
    ) public initializer {
        __AccessControl_init();
        __Pausable_init();
        __ReentrancyGuard_init();
        __UUPSUpgradeable_init();
        
        settlementToken = IERC20(_settlementToken);
        computeFutures = IComputeFutures(_computeFutures);
        priceOracle = AggregatorV3Interface(_priceOracle);
        
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(OPERATOR_ROLE, msg.sender);
    }
    
    /**
     * @notice Write a new option
     */
    function writeOption(
        OptionType optionType,
        ExerciseStyle exerciseStyle,
        uint256 strikePrice,
        uint256 quantity,
        uint256 expiry,
        uint8 resourceType,
        uint256 premium
    ) external nonReentrant whenNotPaused returns (uint256) {
        require(quantity <= maxOptionSize, "Size exceeds maximum");
        require(expiry > block.timestamp, "Expiry must be in future");
        require(premium >= minPremium, "Premium too low");
        
        // Calculate required collateral
        uint256 collateral = _calculateCollateral(optionType, strikePrice, quantity);
        
        // Transfer collateral from writer
        require(
            settlementToken.transferFrom(msg.sender, address(this), collateral),
            "Collateral transfer failed"
        );
        
        uint256 optionId = nextOptionId++;
        
        options[optionId] = ComputeOption({
            optionId: optionId,
            writer: msg.sender,
            holder: address(0),
            optionType: optionType,
            exerciseStyle: exerciseStyle,
            strikePrice: strikePrice,
            quantity: quantity,
            expiry: expiry,
            premium: premium,
            resourceType: resourceType,
            status: OptionStatus.ACTIVE,
            isLong: false,
            collateral: collateral,
            exerciseDates: new uint256[](0),
            averagePrice: 0,
            burstMultiplier: optionType == OptionType.BURST ? 3 : 1
        });
        
        userOptions[msg.sender].push(optionId);
        
        // Calculate initial Greeks
        _updateGreeks(optionId);
        
        emit OptionCreated(optionId, msg.sender, optionType, strikePrice, expiry, premium);
        
        return optionId;
    }
    
    /**
     * @notice Buy an option from a writer
     */
    function buyOption(uint256 optionId) external nonReentrant whenNotPaused {
        ComputeOption storage option = options[optionId];
        require(option.status == OptionStatus.ACTIVE, "Option not active");
        require(option.holder == address(0), "Option already sold");
        require(block.timestamp < option.expiry, "Option expired");
        
        // Transfer premium to writer
        require(
            settlementToken.transferFrom(msg.sender, option.writer, option.premium),
            "Premium transfer failed"
        );
        
        option.holder = msg.sender;
        option.isLong = true;
        userOptions[msg.sender].push(optionId);
        
        emit OptionPurchased(optionId, msg.sender, option.premium);
    }
    
    /**
     * @notice Exercise an option
     */
    function exerciseOption(uint256 optionId) external nonReentrant {
        ComputeOption storage option = options[optionId];
        require(option.holder == msg.sender, "Not option holder");
        require(option.status == OptionStatus.ACTIVE, "Option not active");
        require(block.timestamp < option.expiry, "Option expired");
        
        // Check exercise style constraints
        require(_canExercise(option), "Cannot exercise at this time");
        
        // Get current spot price
        uint256 spotPrice = _getSpotPrice(option.resourceType);
        
        // Calculate payoff
        uint256 payoff = _calculatePayoff(option, spotPrice);
        require(payoff > exerciseFee, "Payoff less than exercise fee");
        
        // For call options on compute, create a futures contract
        if (option.optionType == OptionType.CALL && payoff > 0) {
            // Create futures contract for physical delivery
            computeFutures.createFuturesContract(
                option.writer,
                option.resourceType,
                option.quantity,
                option.strikePrice,
                block.timestamp + 1 hours, // Quick delivery
                1 days, // Duration
                "",
                95 // 95% SLA
            );
        }
        
        // Transfer payoff minus fee
        uint256 netPayoff = payoff - exerciseFee;
        settlementToken.transfer(msg.sender, netPayoff);
        
        // Return remaining collateral to writer
        uint256 remainingCollateral = option.collateral > payoff ? option.collateral - payoff : 0;
        if (remainingCollateral > 0) {
            settlementToken.transfer(option.writer, remainingCollateral);
        }
        
        option.status = OptionStatus.EXERCISED;
        
        emit OptionExercised(optionId, msg.sender, netPayoff);
    }
    
    /**
     * @notice Add liquidity to AMM pool
     */
    function addLiquidity(
        uint8 resourceType,
        uint256 expiry,
        uint256 amount
    ) external nonReentrant onlyRole(MARKET_MAKER_ROLE) {
        require(
            settlementToken.transferFrom(msg.sender, address(this), amount),
            "Transfer failed"
        );
        
        liquidityPools[resourceType][expiry] += amount;
        
        emit LiquidityAdded(resourceType, expiry, amount);
    }
    
    /**
     * @notice Update implied volatility for a point on the surface
     */
    function updateVolatility(
        uint8 resourceType,
        uint256 strike,
        uint256 expiry,
        uint256 impliedVol
    ) external onlyRole(OPERATOR_ROLE) {
        volatilitySurface[resourceType].push(VolatilityPoint({
            strike: strike,
            expiry: expiry,
            impliedVol: impliedVol,
            timestamp: block.timestamp
        }));
        
        emit VolatilityUpdated(resourceType, strike, expiry, impliedVol);
    }
    
    /**
     * @notice Calculate option Greeks
     */
    function calculateGreeks(uint256 optionId) external view returns (Greeks memory) {
        ComputeOption storage option = options[optionId];
        uint256 spotPrice = _getSpotPrice(option.resourceType);
        uint256 timeToExpiry = option.expiry > block.timestamp ? 
            option.expiry - block.timestamp : 0;
        uint256 volatility = _getImpliedVolatility(option);
        
        return _blackScholesGreeks(
            spotPrice,
            option.strikePrice,
            timeToExpiry,
            volatility,
            riskFreeRate,
            option.optionType == OptionType.CALL
        );
    }
    
    /**
     * @notice Get option price using Black-Scholes
     */
    function getOptionPrice(
        OptionType optionType,
        uint256 strikePrice,
        uint256 expiry,
        uint8 resourceType
    ) external view returns (uint256) {
        uint256 spotPrice = _getSpotPrice(resourceType);
        uint256 timeToExpiry = expiry > block.timestamp ? expiry - block.timestamp : 0;
        uint256 volatility = baseVolatility; // Simplified
        
        return _blackScholesPrice(
            spotPrice,
            strikePrice,
            timeToExpiry,
            volatility,
            riskFreeRate,
            optionType == OptionType.CALL
        );
    }
    
    /**
     * @notice Expire options that have passed expiry
     */
    function expireOptions(uint256[] calldata optionIds) external {
        for (uint256 i = 0; i < optionIds.length; i++) {
            ComputeOption storage option = options[optionIds[i]];
            
            if (option.status == OptionStatus.ACTIVE && block.timestamp >= option.expiry) {
                option.status = OptionStatus.EXPIRED;
                
                // Return collateral to writer if not exercised
                if (option.holder == address(0) || option.holder != address(0)) {
                    settlementToken.transfer(option.writer, option.collateral);
                }
                
                emit OptionExpired(optionIds[i]);
            }
        }
    }
    
    // Internal functions
    
    function _calculateCollateral(
        OptionType optionType,
        uint256 strikePrice,
        uint256 quantity
    ) internal view returns (uint256) {
        uint256 notional = strikePrice * quantity;
        return (notional * collateralRatio) / 100;
    }
    
    function _canExercise(ComputeOption storage option) internal view returns (bool) {
        if (option.exerciseStyle == ExerciseStyle.EUROPEAN) {
            // Can only exercise at expiry
            return block.timestamp >= option.expiry - 1 hours;
        } else if (option.exerciseStyle == ExerciseStyle.AMERICAN) {
            // Can exercise anytime
            return true;
        } else if (option.exerciseStyle == ExerciseStyle.BERMUDAN) {
            // Can exercise on specific dates
            for (uint256 i = 0; i < option.exerciseDates.length; i++) {
                if (block.timestamp >= option.exerciseDates[i] - 1 hours &&
                    block.timestamp <= option.exerciseDates[i] + 1 hours) {
                    return true;
                }
            }
            return false;
        }
        // Asian style handled differently
        return true;
    }
    
    function _calculatePayoff(
        ComputeOption storage option,
        uint256 spotPrice
    ) internal view returns (uint256) {
        if (option.optionType == OptionType.CALL) {
            return spotPrice > option.strikePrice ? 
                (spotPrice - option.strikePrice) * option.quantity : 0;
        } else if (option.optionType == OptionType.PUT) {
            return option.strikePrice > spotPrice ? 
                (option.strikePrice - spotPrice) * option.quantity : 0;
        } else if (option.optionType == OptionType.BURST) {
            // Burst options pay based on surge multiplier
            uint256 surgePrice = spotPrice * option.burstMultiplier;
            return surgePrice > option.strikePrice ? 
                (surgePrice - option.strikePrice) * option.quantity : 0;
        }
        return 0;
    }
    
    function _getSpotPrice(uint8 resourceType) internal view returns (uint256) {
        // In production, this would query actual spot prices
        (, int256 price,,,) = priceOracle.latestRoundData();
        return uint256(price);
    }
    
    function _getImpliedVolatility(ComputeOption storage option) internal view returns (uint256) {
        // Simplified - in production, interpolate from volatility surface
        return baseVolatility;
    }
    
    function _updateGreeks(uint256 optionId) internal {
        Greeks memory greeks = this.calculateGreeks(optionId);
        optionGreeks[optionId] = greeks;
    }
    
    function _blackScholesPrice(
        uint256 S,
        uint256 K,
        uint256 T,
        uint256 sigma,
        uint256 r,
        bool isCall
    ) internal pure returns (uint256) {
        // Simplified Black-Scholes implementation
        // In production, use proper mathematical libraries
        if (T == 0) return isCall ? (S > K ? S - K : 0) : (K > S ? K - S : 0);
        
        // Placeholder calculation
        uint256 intrinsicValue = isCall ? (S > K ? S - K : 0) : (K > S ? K - S : 0);
        uint256 timeValue = (sigma * T * S) / (365 days * VOLATILITY_PRECISION);
        
        return intrinsicValue + timeValue;
    }
    
    function _blackScholesGreeks(
        uint256 S,
        uint256 K,
        uint256 T,
        uint256 sigma,
        uint256 r,
        bool isCall
    ) internal pure returns (Greeks memory) {
        // Simplified Greeks calculation
        // In production, use proper mathematical libraries
        
        Greeks memory greeks;
        
        // Delta: rate of change of option price with respect to underlying price
        if (isCall) {
            greeks.delta = S > K ? int256(PRICE_PRECISION) : int256(PRICE_PRECISION / 2);
        } else {
            greeks.delta = S < K ? -int256(PRICE_PRECISION) : -int256(PRICE_PRECISION / 2);
        }
        
        // Simplified other Greeks
        greeks.gamma = int256(PRICE_PRECISION / 100);
        greeks.vega = int256((sigma * T) / 365 days);
        greeks.theta = -int256(PRICE_PRECISION / 365);
        greeks.rho = int256(r * T / 365 days);
        
        return greeks;
    }
    
    function _authorizeUpgrade(address newImplementation) internal override onlyRole(DEFAULT_ADMIN_ROLE) {}
} 