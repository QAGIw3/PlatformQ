// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/security/Pausable.sol";
import "@openzeppelin/contracts/utils/math/SignedMath.sol";
import "../ResourceToken.sol";
import "../ResourceAMM.sol";

/**
 * @title ResourcePerpetuals
 * @notice Perpetual futures contracts for infrastructure resources
 * @dev Implements funding rate mechanism and liquidations
 */
contract ResourcePerpetuals is ReentrancyGuard, AccessControl, Pausable {
    using SignedMath for int256;
    
    // Roles
    bytes32 public constant KEEPER_ROLE = keccak256("KEEPER_ROLE");
    bytes32 public constant ORACLE_ROLE = keccak256("ORACLE_ROLE");
    bytes32 public constant LIQUIDATOR_ROLE = keccak256("LIQUIDATOR_ROLE");
    
    // Constants
    uint256 public constant PRECISION = 1e18;
    uint256 public constant BASIS_POINTS = 10000;
    uint256 public constant MIN_MARGIN = 500; // 5% minimum margin
    uint256 public constant LIQUIDATION_MARGIN = 250; // 2.5% liquidation threshold
    uint256 public constant MAX_LEVERAGE = 20; // 20x max leverage
    uint256 public constant FUNDING_INTERVAL = 8 hours;
    
    // Position struct
    struct Position {
        uint256 size;                 // Position size in resource units
        int256 entryPrice;           // Average entry price
        uint256 margin;              // Margin deposited
        int256 fundingIndex;         // Funding index at position open
        uint256 lastUpdateTime;      // Last position update
        bool isLong;                 // Long or short position
    }
    
    // Market struct
    struct Market {
        uint256 resourceTokenId;      // Resource token ID
        uint256 openInterest;        // Total open interest
        uint256 longOpenInterest;    // Long open interest
        uint256 shortOpenInterest;   // Short open interest
        int256 fundingRate;          // Current funding rate (per funding interval)
        int256 cumulativeFunding;    // Cumulative funding index
        uint256 lastFundingTime;     // Last funding update
        uint256 maxOpenInterest;     // Maximum allowed open interest
        bool isActive;               // Whether market is active
    }
    
    // State variables
    ResourceToken public immutable resourceToken;
    ResourceAMM public immutable resourceAMM;
    IERC20 public immutable marginToken;
    
    mapping(uint256 => Market) public markets; // resourceTokenId => Market
    mapping(address => mapping(uint256 => Position)) public positions; // user => resourceTokenId => Position
    
    // Oracle prices
    mapping(uint256 => uint256) public indexPrices; // resourceTokenId => index price
    mapping(uint256 => uint256) public markPrices; // resourceTokenId => mark price
    
    // Fees and parameters
    uint256 public tradingFee = 30; // 0.3%
    uint256 public liquidationFee = 100; // 1%
    uint256 public insuranceFundRatio = 50; // 0.5% to insurance fund
    address public feeRecipient;
    uint256 public insuranceFund;
    
    // Events
    event MarketCreated(uint256 indexed resourceTokenId, uint256 maxOpenInterest);
    event PositionOpened(
        address indexed trader,
        uint256 indexed resourceTokenId,
        bool isLong,
        uint256 size,
        uint256 margin,
        int256 entryPrice
    );
    event PositionModified(
        address indexed trader,
        uint256 indexed resourceTokenId,
        int256 sizeDelta,
        uint256 marginDelta,
        int256 newPrice
    );
    event PositionClosed(
        address indexed trader,
        uint256 indexed resourceTokenId,
        int256 pnl,
        uint256 fee
    );
    event PositionLiquidated(
        address indexed trader,
        uint256 indexed resourceTokenId,
        address indexed liquidator,
        uint256 size,
        uint256 margin,
        uint256 liquidationFee
    );
    event FundingUpdated(
        uint256 indexed resourceTokenId,
        int256 fundingRate,
        int256 cumulativeFunding
    );
    event PriceUpdated(
        uint256 indexed resourceTokenId,
        uint256 indexPrice,
        uint256 markPrice
    );
    
    /**
     * @dev Constructor
     * @param _resourceToken ResourceToken contract address
     * @param _resourceAMM ResourceAMM contract address
     * @param _marginToken Margin token address (e.g., USDC)
     */
    constructor(
        address _resourceToken,
        address _resourceAMM,
        address _marginToken
    ) {
        resourceToken = ResourceToken(_resourceToken);
        resourceAMM = ResourceAMM(_resourceAMM);
        marginToken = IERC20(_marginToken);
        
        feeRecipient = msg.sender;
        
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(KEEPER_ROLE, msg.sender);
        _grantRole(ORACLE_ROLE, msg.sender);
        _grantRole(LIQUIDATOR_ROLE, msg.sender);
    }
    
    /**
     * @notice Create a new perpetual market
     * @param resourceTokenId Resource token ID
     * @param maxOpenInterest Maximum open interest allowed
     */
    function createMarket(
        uint256 resourceTokenId,
        uint256 maxOpenInterest
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(!markets[resourceTokenId].isActive, "Market exists");
        require(maxOpenInterest > 0, "Invalid max OI");
        
        markets[resourceTokenId] = Market({
            resourceTokenId: resourceTokenId,
            openInterest: 0,
            longOpenInterest: 0,
            shortOpenInterest: 0,
            fundingRate: 0,
            cumulativeFunding: 0,
            lastFundingTime: block.timestamp,
            maxOpenInterest: maxOpenInterest,
            isActive: true
        });
        
        emit MarketCreated(resourceTokenId, maxOpenInterest);
    }
    
    /**
     * @notice Open a new position
     * @param resourceTokenId Resource token ID
     * @param size Position size
     * @param margin Margin amount
     * @param isLong Long or short position
     */
    function openPosition(
        uint256 resourceTokenId,
        uint256 size,
        uint256 margin,
        bool isLong
    ) external nonReentrant whenNotPaused {
        Market storage market = markets[resourceTokenId];
        require(market.isActive, "Market not active");
        require(size > 0, "Invalid size");
        require(margin > 0, "Invalid margin");
        
        // Check leverage
        uint256 price = getMarkPrice(resourceTokenId);
        uint256 notional = size * price / PRECISION;
        uint256 leverage = notional * PRECISION / margin;
        require(leverage <= MAX_LEVERAGE * PRECISION, "Leverage too high");
        require(leverage >= PRECISION, "Leverage too low");
        
        // Check minimum margin
        require(margin * BASIS_POINTS / notional >= MIN_MARGIN, "Below min margin");
        
        // Check open interest limits
        if (isLong) {
            require(market.longOpenInterest + size <= market.maxOpenInterest, "Max OI exceeded");
        } else {
            require(market.shortOpenInterest + size <= market.maxOpenInterest, "Max OI exceeded");
        }
        
        // Transfer margin
        uint256 fee = notional * tradingFee / BASIS_POINTS;
        marginToken.transferFrom(msg.sender, address(this), margin + fee);
        
        // Distribute fee
        _distributeFee(fee);
        
        // Update position
        Position storage position = positions[msg.sender][resourceTokenId];
        if (position.size > 0) {
            // Add to existing position
            _modifyPosition(msg.sender, resourceTokenId, int256(size), margin, isLong);
        } else {
            // New position
            position.size = size;
            position.entryPrice = int256(price);
            position.margin = margin;
            position.fundingIndex = market.cumulativeFunding;
            position.lastUpdateTime = block.timestamp;
            position.isLong = isLong;
            
            // Update market
            market.openInterest += size;
            if (isLong) {
                market.longOpenInterest += size;
            } else {
                market.shortOpenInterest += size;
            }
            
            emit PositionOpened(msg.sender, resourceTokenId, isLong, size, margin, int256(price));
        }
    }
    
    /**
     * @notice Close a position
     * @param resourceTokenId Resource token ID
     * @param size Size to close (0 for full close)
     */
    function closePosition(
        uint256 resourceTokenId,
        uint256 size
    ) external nonReentrant {
        Position storage position = positions[msg.sender][resourceTokenId];
        require(position.size > 0, "No position");
        
        uint256 closeSize = size == 0 ? position.size : size;
        require(closeSize <= position.size, "Size too large");
        
        // Update funding
        _updateFunding(resourceTokenId);
        
        // Calculate PnL
        uint256 price = getMarkPrice(resourceTokenId);
        int256 pnl = _calculatePnL(position, int256(price), closeSize);
        
        // Calculate funding payment
        int256 fundingPayment = _calculateFundingPayment(
            position,
            markets[resourceTokenId].cumulativeFunding,
            closeSize
        );
        
        // Calculate fee
        uint256 notional = closeSize * price / PRECISION;
        uint256 fee = notional * tradingFee / BASIS_POINTS;
        
        // Calculate total payout
        int256 totalPnL = pnl - fundingPayment - int256(fee);
        uint256 marginToReturn = position.margin * closeSize / position.size;
        
        // Update position
        if (closeSize == position.size) {
            // Full close
            delete positions[msg.sender][resourceTokenId];
        } else {
            // Partial close
            position.size -= closeSize;
            position.margin -= marginToReturn;
        }
        
        // Update market
        Market storage market = markets[resourceTokenId];
        market.openInterest -= closeSize;
        if (position.isLong) {
            market.longOpenInterest -= closeSize;
        } else {
            market.shortOpenInterest -= closeSize;
        }
        
        // Process payout
        if (totalPnL > 0) {
            marginToken.transfer(msg.sender, marginToReturn + uint256(totalPnL));
        } else if (int256(marginToReturn) + totalPnL > 0) {
            marginToken.transfer(msg.sender, uint256(int256(marginToReturn) + totalPnL));
        }
        // else: loss exceeds margin, nothing to return
        
        // Distribute fee
        _distributeFee(fee);
        
        emit PositionClosed(msg.sender, resourceTokenId, totalPnL, fee);
    }
    
    /**
     * @notice Add margin to a position
     * @param resourceTokenId Resource token ID
     * @param amount Margin to add
     */
    function addMargin(
        uint256 resourceTokenId,
        uint256 amount
    ) external nonReentrant {
        Position storage position = positions[msg.sender][resourceTokenId];
        require(position.size > 0, "No position");
        require(amount > 0, "Invalid amount");
        
        marginToken.transferFrom(msg.sender, address(this), amount);
        position.margin += amount;
        
        emit PositionModified(msg.sender, resourceTokenId, 0, amount, position.entryPrice);
    }
    
    /**
     * @notice Remove margin from a position
     * @param resourceTokenId Resource token ID
     * @param amount Margin to remove
     */
    function removeMargin(
        uint256 resourceTokenId,
        uint256 amount
    ) external nonReentrant {
        Position storage position = positions[msg.sender][resourceTokenId];
        require(position.size > 0, "No position");
        require(amount > 0 && amount < position.margin, "Invalid amount");
        
        // Check margin requirements after removal
        uint256 price = getMarkPrice(resourceTokenId);
        uint256 notional = position.size * price / PRECISION;
        uint256 newMargin = position.margin - amount;
        
        require(newMargin * BASIS_POINTS / notional >= MIN_MARGIN, "Below min margin");
        
        position.margin = newMargin;
        marginToken.transfer(msg.sender, amount);
        
        emit PositionModified(msg.sender, resourceTokenId, 0, -int256(amount), position.entryPrice);
    }
    
    /**
     * @notice Liquidate an undercollateralized position
     * @param trader Trader address
     * @param resourceTokenId Resource token ID
     */
    function liquidatePosition(
        address trader,
        uint256 resourceTokenId
    ) external nonReentrant onlyRole(LIQUIDATOR_ROLE) {
        Position storage position = positions[trader][resourceTokenId];
        require(position.size > 0, "No position");
        
        // Update funding
        _updateFunding(resourceTokenId);
        
        // Check if liquidatable
        uint256 price = getMarkPrice(resourceTokenId);
        require(_isLiquidatable(position, price, resourceTokenId), "Not liquidatable");
        
        // Calculate liquidation fee
        uint256 notional = position.size * price / PRECISION;
        uint256 liquidationFeeAmount = notional * liquidationFee / BASIS_POINTS;
        
        // Update market
        Market storage market = markets[resourceTokenId];
        market.openInterest -= position.size;
        if (position.isLong) {
            market.longOpenInterest -= position.size;
        } else {
            market.shortOpenInterest -= position.size;
        }
        
        // Pay liquidator
        uint256 liquidatorReward = liquidationFeeAmount / 2;
        marginToken.transfer(msg.sender, liquidatorReward);
        
        // Add to insurance fund
        insuranceFund += position.margin - liquidatorReward;
        
        emit PositionLiquidated(
            trader,
            resourceTokenId,
            msg.sender,
            position.size,
            position.margin,
            liquidationFeeAmount
        );
        
        // Delete position
        delete positions[trader][resourceTokenId];
    }
    
    /**
     * @notice Update funding rate for a market
     * @param resourceTokenId Resource token ID
     */
    function updateFunding(uint256 resourceTokenId) external onlyRole(KEEPER_ROLE) {
        _updateFunding(resourceTokenId);
    }
    
    /**
     * @notice Update oracle prices
     * @param resourceTokenId Resource token ID
     * @param indexPrice Index price
     * @param markPrice Mark price
     */
    function updatePrices(
        uint256 resourceTokenId,
        uint256 indexPrice,
        uint256 markPrice
    ) external onlyRole(ORACLE_ROLE) {
        require(indexPrice > 0 && markPrice > 0, "Invalid prices");
        
        indexPrices[resourceTokenId] = indexPrice;
        markPrices[resourceTokenId] = markPrice;
        
        emit PriceUpdated(resourceTokenId, indexPrice, markPrice);
    }
    
    /**
     * @notice Get mark price for a resource
     * @param resourceTokenId Resource token ID
     * @return Mark price
     */
    function getMarkPrice(uint256 resourceTokenId) public view returns (uint256) {
        uint256 price = markPrices[resourceTokenId];
        if (price == 0) {
            // Fallback to AMM price
            (uint256 reserve0, uint256 reserve1, ) = resourceAMM.getReserves(resourceTokenId);
            price = reserve1 * PRECISION / reserve0;
        }
        return price;
    }
    
    /**
     * @notice Get position info
     * @param trader Trader address
     * @param resourceTokenId Resource token ID
     * @return position Position details
     * @return unrealizedPnL Unrealized PnL
     * @return marginRatio Current margin ratio
     */
    function getPositionInfo(
        address trader,
        uint256 resourceTokenId
    ) external view returns (
        Position memory position,
        int256 unrealizedPnL,
        uint256 marginRatio
    ) {
        position = positions[trader][resourceTokenId];
        if (position.size == 0) {
            return (position, 0, 0);
        }
        
        uint256 price = getMarkPrice(resourceTokenId);
        unrealizedPnL = _calculatePnL(position, int256(price), position.size);
        
        // Calculate funding payment
        Market memory market = markets[resourceTokenId];
        int256 fundingPayment = _calculateFundingPayment(
            position,
            market.cumulativeFunding,
            position.size
        );
        
        unrealizedPnL -= fundingPayment;
        
        // Calculate margin ratio
        uint256 notional = position.size * price / PRECISION;
        int256 equity = int256(position.margin) + unrealizedPnL;
        
        if (equity <= 0) {
            marginRatio = 0;
        } else {
            marginRatio = uint256(equity) * BASIS_POINTS / notional;
        }
    }
    
    // Internal functions
    
    function _updateFunding(uint256 resourceTokenId) internal {
        Market storage market = markets[resourceTokenId];
        
        if (block.timestamp < market.lastFundingTime + FUNDING_INTERVAL) {
            return;
        }
        
        uint256 indexPrice = indexPrices[resourceTokenId];
        uint256 markPrice = getMarkPrice(resourceTokenId);
        
        if (indexPrice == 0 || markPrice == 0) {
            return;
        }
        
        // Calculate funding rate
        // Positive rate: longs pay shorts
        // Negative rate: shorts pay longs
        int256 priceDiff = int256(markPrice) - int256(indexPrice);
        int256 fundingRate = priceDiff * int256(FUNDING_INTERVAL) / (int256(indexPrice) * 24 hours);
        
        // Apply funding rate limits (-0.75% to 0.75% per interval)
        int256 maxRate = 75; // 0.75%
        if (fundingRate > maxRate) {
            fundingRate = maxRate;
        } else if (fundingRate < -maxRate) {
            fundingRate = -maxRate;
        }
        
        market.fundingRate = fundingRate;
        market.cumulativeFunding += fundingRate;
        market.lastFundingTime = block.timestamp;
        
        emit FundingUpdated(resourceTokenId, fundingRate, market.cumulativeFunding);
    }
    
    function _calculatePnL(
        Position memory position,
        int256 currentPrice,
        uint256 size
    ) internal pure returns (int256) {
        int256 priceDiff = currentPrice - position.entryPrice;
        
        if (position.isLong) {
            return priceDiff * int256(size) / int256(PRECISION);
        } else {
            return -priceDiff * int256(size) / int256(PRECISION);
        }
    }
    
    function _calculateFundingPayment(
        Position memory position,
        int256 currentFundingIndex,
        uint256 size
    ) internal pure returns (int256) {
        int256 fundingDiff = currentFundingIndex - position.fundingIndex;
        
        if (position.isLong) {
            return fundingDiff * int256(size) / int256(BASIS_POINTS);
        } else {
            return -fundingDiff * int256(size) / int256(BASIS_POINTS);
        }
    }
    
    function _isLiquidatable(
        Position memory position,
        uint256 currentPrice,
        uint256 resourceTokenId
    ) internal view returns (bool) {
        int256 unrealizedPnL = _calculatePnL(position, int256(currentPrice), position.size);
        
        // Calculate funding payment
        Market memory market = markets[resourceTokenId];
        int256 fundingPayment = _calculateFundingPayment(
            position,
            market.cumulativeFunding,
            position.size
        );
        
        int256 equity = int256(position.margin) + unrealizedPnL - fundingPayment;
        
        if (equity <= 0) {
            return true;
        }
        
        uint256 notional = position.size * currentPrice / PRECISION;
        uint256 marginRatio = uint256(equity) * BASIS_POINTS / notional;
        
        return marginRatio < LIQUIDATION_MARGIN;
    }
    
    function _modifyPosition(
        address trader,
        uint256 resourceTokenId,
        int256 sizeDelta,
        uint256 marginDelta,
        bool isLong
    ) internal {
        Position storage position = positions[trader][resourceTokenId];
        Market storage market = markets[resourceTokenId];
        
        require(position.isLong == isLong || position.size == 0, "Wrong direction");
        
        // Update funding
        _updateFunding(resourceTokenId);
        
        // Calculate new average entry price
        uint256 price = getMarkPrice(resourceTokenId);
        uint256 newSize = position.size + uint256(sizeDelta);
        int256 newEntryPrice = (position.entryPrice * int256(position.size) + int256(price) * sizeDelta) / int256(newSize);
        
        // Update position
        position.size = newSize;
        position.entryPrice = newEntryPrice;
        position.margin += marginDelta;
        position.fundingIndex = market.cumulativeFunding;
        position.lastUpdateTime = block.timestamp;
        
        // Update market
        market.openInterest += uint256(sizeDelta);
        if (isLong) {
            market.longOpenInterest += uint256(sizeDelta);
        } else {
            market.shortOpenInterest += uint256(sizeDelta);
        }
        
        emit PositionModified(trader, resourceTokenId, sizeDelta, marginDelta, newEntryPrice);
    }
    
    function _distributeFee(uint256 fee) internal {
        uint256 insuranceFee = fee * insuranceFundRatio / BASIS_POINTS;
        insuranceFund += insuranceFee;
        
        uint256 protocolFee = fee - insuranceFee;
        marginToken.transfer(feeRecipient, protocolFee);
    }
    
    // Admin functions
    
    function setFees(
        uint256 _tradingFee,
        uint256 _liquidationFee,
        uint256 _insuranceFundRatio
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(_tradingFee <= 100, "Trading fee too high"); // Max 1%
        require(_liquidationFee <= 500, "Liquidation fee too high"); // Max 5%
        require(_insuranceFundRatio <= 1000, "Insurance ratio too high"); // Max 10%
        
        tradingFee = _tradingFee;
        liquidationFee = _liquidationFee;
        insuranceFundRatio = _insuranceFundRatio;
    }
    
    function setFeeRecipient(address _feeRecipient) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(_feeRecipient != address(0), "Invalid recipient");
        feeRecipient = _feeRecipient;
    }
    
    function setMaxOpenInterest(
        uint256 resourceTokenId,
        uint256 maxOpenInterest
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        markets[resourceTokenId].maxOpenInterest = maxOpenInterest;
    }
    
    function withdrawInsuranceFund(
        address recipient,
        uint256 amount
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(amount <= insuranceFund, "Insufficient fund");
        insuranceFund -= amount;
        marginToken.transfer(recipient, amount);
    }
    
    function pause() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _pause();
    }
    
    function unpause() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _unpause();
    }
} 