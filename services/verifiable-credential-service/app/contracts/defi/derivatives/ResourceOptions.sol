// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC1155/IERC1155.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/token/ERC721/ERC721.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/utils/Counters.sol";
import "../ResourceToken.sol";
import "../ResourceAMM.sol";

/**
 * @title ResourceOptions
 * @notice European and American style options for infrastructure resources
 * @dev Options are represented as ERC721 NFTs
 */
contract ResourceOptions is ERC721, ReentrancyGuard, AccessControl {
    using Counters for Counters.Counter;
    
    // Roles
    bytes32 public constant MARKET_MAKER_ROLE = keccak256("MARKET_MAKER_ROLE");
    bytes32 public constant ORACLE_ROLE = keccak256("ORACLE_ROLE");
    
    // Option types
    enum OptionType { CALL, PUT }
    enum OptionStyle { EUROPEAN, AMERICAN }
    
    // Option struct
    struct Option {
        uint256 resourceTokenId;      // Resource token ID
        uint256 strikePrice;          // Strike price in settlement token
        uint256 expiry;               // Expiration timestamp
        uint256 amount;               // Amount of resource tokens
        OptionType optionType;        // Call or Put
        OptionStyle style;            // European or American
        address writer;               // Option writer
        address holder;               // Current holder
        bool exercised;               // Whether exercised
        bool expired;                 // Whether expired
        uint256 premium;              // Premium paid
        uint256 collateral;           // Collateral locked
    }
    
    // State variables
    ResourceToken public immutable resourceToken;
    ResourceAMM public immutable resourceAMM;
    IERC20 public immutable settlementToken;
    
    Counters.Counter private _optionIds;
    mapping(uint256 => Option) public options;
    
    // Market data
    mapping(uint256 => uint256) public impliedVolatility; // resourceTokenId => IV
    mapping(uint256 => uint256) public spotPrices; // resourceTokenId => spot price
    uint256 public constant MIN_DURATION = 1 days;
    uint256 public constant MAX_DURATION = 365 days;
    
    // Collateral requirements
    uint256 public constant CALL_COLLATERAL_RATIO = 10000; // 100% for calls
    uint256 public constant PUT_COLLATERAL_RATIO = 10000; // 100% for puts
    uint256 public constant BASIS_POINTS = 10000;
    
    // Fees
    uint256 public exerciseFee = 10; // 0.1%
    uint256 public settlementFee = 20; // 0.2%
    address public feeRecipient;
    
    // Events
    event OptionWritten(
        uint256 indexed optionId,
        address indexed writer,
        uint256 resourceTokenId,
        uint256 strikePrice,
        uint256 expiry,
        OptionType optionType,
        uint256 amount,
        uint256 premium
    );
    
    event OptionPurchased(
        uint256 indexed optionId,
        address indexed buyer,
        uint256 premium
    );
    
    event OptionExercised(
        uint256 indexed optionId,
        address indexed holder,
        uint256 payout
    );
    
    event OptionExpired(uint256 indexed optionId);
    
    event SpotPriceUpdated(uint256 indexed resourceTokenId, uint256 price);
    event ImpliedVolatilityUpdated(uint256 indexed resourceTokenId, uint256 iv);
    
    /**
     * @dev Constructor
     * @param _resourceToken ResourceToken contract address
     * @param _resourceAMM ResourceAMM contract address
     * @param _settlementToken Settlement token address (e.g., USDC)
     */
    constructor(
        address _resourceToken,
        address _resourceAMM,
        address _settlementToken
    ) ERC721("Resource Options", "rOPT") {
        resourceToken = ResourceToken(_resourceToken);
        resourceAMM = ResourceAMM(_resourceAMM);
        settlementToken = IERC20(_settlementToken);
        
        feeRecipient = msg.sender;
        
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(MARKET_MAKER_ROLE, msg.sender);
        _grantRole(ORACLE_ROLE, msg.sender);
    }
    
    /**
     * @notice Write a new option
     * @param resourceTokenId Resource token ID
     * @param strikePrice Strike price in settlement token
     * @param expiry Expiration timestamp
     * @param optionType Call or Put
     * @param style European or American
     * @param amount Amount of resource tokens
     * @return optionId The ID of the written option
     */
    function writeOption(
        uint256 resourceTokenId,
        uint256 strikePrice,
        uint256 expiry,
        OptionType optionType,
        OptionStyle style,
        uint256 amount
    ) external nonReentrant returns (uint256) {
        require(expiry > block.timestamp + MIN_DURATION, "Expiry too soon");
        require(expiry <= block.timestamp + MAX_DURATION, "Expiry too far");
        require(amount > 0, "Invalid amount");
        require(strikePrice > 0, "Invalid strike price");
        
        // Calculate required collateral
        uint256 collateral = _calculateCollateral(
            resourceTokenId,
            strikePrice,
            optionType,
            amount
        );
        
        // Lock collateral
        if (optionType == OptionType.CALL) {
            // For calls, lock the resource tokens
            resourceToken.safeTransferFrom(msg.sender, address(this), resourceTokenId, amount, "");
        } else {
            // For puts, lock settlement tokens equal to strike * amount
            uint256 putCollateral = strikePrice * amount / 1e18;
            settlementToken.transferFrom(msg.sender, address(this), putCollateral);
        }
        
        // Create option
        _optionIds.increment();
        uint256 optionId = _optionIds.current();
        
        options[optionId] = Option({
            resourceTokenId: resourceTokenId,
            strikePrice: strikePrice,
            expiry: expiry,
            amount: amount,
            optionType: optionType,
            style: style,
            writer: msg.sender,
            holder: msg.sender,
            exercised: false,
            expired: false,
            premium: 0,
            collateral: collateral
        });
        
        // Mint NFT to writer
        _mint(msg.sender, optionId);
        
        emit OptionWritten(
            optionId,
            msg.sender,
            resourceTokenId,
            strikePrice,
            expiry,
            optionType,
            amount,
            0
        );
        
        return optionId;
    }
    
    /**
     * @notice Calculate option premium using Black-Scholes
     * @param optionId Option ID
     * @return premium The option premium
     */
    function calculatePremium(uint256 optionId) public view returns (uint256) {
        Option memory option = options[optionId];
        require(option.expiry > block.timestamp, "Option expired");
        
        uint256 spot = spotPrices[option.resourceTokenId];
        require(spot > 0, "No spot price");
        
        uint256 timeToExpiry = option.expiry - block.timestamp;
        uint256 iv = impliedVolatility[option.resourceTokenId];
        require(iv > 0, "No implied volatility");
        
        // Simplified Black-Scholes approximation
        // In production, use proper implementation with normal distribution
        uint256 intrinsicValue = 0;
        
        if (option.optionType == OptionType.CALL) {
            if (spot > option.strikePrice) {
                intrinsicValue = spot - option.strikePrice;
            }
        } else {
            if (option.strikePrice > spot) {
                intrinsicValue = option.strikePrice - spot;
            }
        }
        
        // Time value = base_premium * sqrt(time_to_expiry) * iv
        uint256 timeValue = (spot * iv * sqrt(timeToExpiry)) / (100 * 365 days);
        
        uint256 premium = (intrinsicValue + timeValue) * option.amount / 1e18;
        
        // Apply minimum premium
        uint256 minPremium = (spot * option.amount * 10) / BASIS_POINTS; // 0.1% minimum
        if (premium < minPremium) {
            premium = minPremium;
        }
        
        return premium;
    }
    
    /**
     * @notice Buy an option from the writer
     * @param optionId Option ID
     */
    function buyOption(uint256 optionId) external nonReentrant {
        Option storage option = options[optionId];
        require(option.holder == option.writer, "Option already sold");
        require(option.expiry > block.timestamp, "Option expired");
        require(!option.exercised, "Option exercised");
        
        uint256 premium = calculatePremium(optionId);
        option.premium = premium;
        
        // Transfer premium to writer
        settlementToken.transferFrom(msg.sender, option.writer, premium);
        
        // Transfer option NFT to buyer
        _transfer(option.writer, msg.sender, optionId);
        option.holder = msg.sender;
        
        emit OptionPurchased(optionId, msg.sender, premium);
    }
    
    /**
     * @notice Exercise an option
     * @param optionId Option ID
     */
    function exerciseOption(uint256 optionId) external nonReentrant {
        Option storage option = options[optionId];
        require(ownerOf(optionId) == msg.sender, "Not option holder");
        require(!option.exercised, "Already exercised");
        require(!option.expired, "Option expired");
        require(option.expiry > block.timestamp, "Option expired");
        
        // Check exercise style
        if (option.style == OptionStyle.EUROPEAN) {
            require(
                block.timestamp >= option.expiry - 1 hours,
                "European option not yet exercisable"
            );
        }
        
        uint256 payout = 0;
        uint256 fee = 0;
        
        if (option.optionType == OptionType.CALL) {
            // Call option: pay strike price, receive resource tokens
            uint256 cost = option.strikePrice * option.amount / 1e18;
            fee = cost * exerciseFee / BASIS_POINTS;
            
            settlementToken.transferFrom(msg.sender, option.writer, cost - fee);
            settlementToken.transferFrom(msg.sender, feeRecipient, fee);
            
            resourceToken.safeTransferFrom(
                address(this),
                msg.sender,
                option.resourceTokenId,
                option.amount,
                ""
            );
            
            payout = option.amount;
        } else {
            // Put option: sell resource tokens at strike price
            resourceToken.safeTransferFrom(
                msg.sender,
                option.writer,
                option.resourceTokenId,
                option.amount,
                ""
            );
            
            uint256 proceeds = option.strikePrice * option.amount / 1e18;
            fee = proceeds * exerciseFee / BASIS_POINTS;
            
            settlementToken.transfer(msg.sender, proceeds - fee);
            settlementToken.transfer(feeRecipient, fee);
            
            payout = proceeds - fee;
        }
        
        option.exercised = true;
        
        emit OptionExercised(optionId, msg.sender, payout);
    }
    
    /**
     * @notice Expire an option that has passed expiry
     * @param optionId Option ID
     */
    function expireOption(uint256 optionId) external nonReentrant {
        Option storage option = options[optionId];
        require(block.timestamp >= option.expiry, "Not expired");
        require(!option.exercised, "Already exercised");
        require(!option.expired, "Already marked expired");
        
        option.expired = true;
        
        // Return collateral to writer
        if (option.optionType == OptionType.CALL) {
            resourceToken.safeTransferFrom(
                address(this),
                option.writer,
                option.resourceTokenId,
                option.amount,
                ""
            );
        } else {
            uint256 putCollateral = option.strikePrice * option.amount / 1e18;
            settlementToken.transfer(option.writer, putCollateral);
        }
        
        emit OptionExpired(optionId);
    }
    
    /**
     * @notice Update spot price for a resource
     * @param resourceTokenId Resource token ID
     * @param price New spot price
     */
    function updateSpotPrice(
        uint256 resourceTokenId,
        uint256 price
    ) external onlyRole(ORACLE_ROLE) {
        require(price > 0, "Invalid price");
        spotPrices[resourceTokenId] = price;
        emit SpotPriceUpdated(resourceTokenId, price);
    }
    
    /**
     * @notice Update implied volatility
     * @param resourceTokenId Resource token ID
     * @param iv New implied volatility (basis points)
     */
    function updateImpliedVolatility(
        uint256 resourceTokenId,
        uint256 iv
    ) external onlyRole(ORACLE_ROLE) {
        require(iv > 0 && iv <= 50000, "Invalid IV"); // Max 500%
        impliedVolatility[resourceTokenId] = iv;
        emit ImpliedVolatilityUpdated(resourceTokenId, iv);
    }
    
    /**
     * @notice Create and sell option in one transaction
     * @param resourceTokenId Resource token ID
     * @param strikePrice Strike price
     * @param expiry Expiration timestamp
     * @param optionType Call or Put
     * @param style European or American
     * @param amount Amount of resource tokens
     * @param buyer Address of the buyer
     * @param premium Premium amount
     */
    function writeAndSellOption(
        uint256 resourceTokenId,
        uint256 strikePrice,
        uint256 expiry,
        OptionType optionType,
        OptionStyle style,
        uint256 amount,
        address buyer,
        uint256 premium
    ) external nonReentrant onlyRole(MARKET_MAKER_ROLE) returns (uint256) {
        // Write option
        uint256 optionId = writeOption(
            resourceTokenId,
            strikePrice,
            expiry,
            optionType,
            style,
            amount
        );
        
        // Update premium
        options[optionId].premium = premium;
        
        // Transfer premium from buyer to writer
        settlementToken.transferFrom(buyer, msg.sender, premium);
        
        // Transfer option to buyer
        _transfer(msg.sender, buyer, optionId);
        options[optionId].holder = buyer;
        
        emit OptionPurchased(optionId, buyer, premium);
        
        return optionId;
    }
    
    /**
     * @notice Get option details
     * @param optionId Option ID
     * @return Option details
     */
    function getOption(uint256 optionId) external view returns (Option memory) {
        return options[optionId];
    }
    
    /**
     * @notice Check if option is in the money
     * @param optionId Option ID
     * @return Whether option is ITM
     */
    function isInTheMoney(uint256 optionId) external view returns (bool) {
        Option memory option = options[optionId];
        uint256 spot = spotPrices[option.resourceTokenId];
        
        if (option.optionType == OptionType.CALL) {
            return spot > option.strikePrice;
        } else {
            return option.strikePrice > spot;
        }
    }
    
    /**
     * @notice Calculate option Greeks (simplified)
     * @param optionId Option ID
     * @return delta Option delta
     * @return gamma Option gamma
     * @return theta Option theta (per day)
     * @return vega Option vega
     */
    function getGreeks(uint256 optionId) external view returns (
        int256 delta,
        uint256 gamma,
        int256 theta,
        uint256 vega
    ) {
        Option memory option = options[optionId];
        uint256 spot = spotPrices[option.resourceTokenId];
        uint256 timeToExpiry = option.expiry > block.timestamp ? 
            option.expiry - block.timestamp : 0;
        
        // Simplified Greeks calculation
        // In production, use proper Black-Scholes Greeks
        
        if (option.optionType == OptionType.CALL) {
            // Delta: 0 to 1 for calls
            if (spot > option.strikePrice) {
                delta = int256(7000); // 0.7
            } else {
                delta = int256(3000); // 0.3
            }
        } else {
            // Delta: -1 to 0 for puts
            if (option.strikePrice > spot) {
                delta = -int256(7000); // -0.7
            } else {
                delta = -int256(3000); // -0.3
            }
        }
        
        // Gamma: highest at the money
        uint256 moneyness = spot > option.strikePrice ? 
            spot - option.strikePrice : option.strikePrice - spot;
        gamma = 1000 - (moneyness * 1000 / spot); // Simplified
        
        // Theta: time decay (negative)
        theta = -int256(option.premium * 1 days / timeToExpiry);
        
        // Vega: sensitivity to IV
        vega = option.amount * sqrt(timeToExpiry) / 1000;
    }
    
    // Internal functions
    
    function _calculateCollateral(
        uint256 resourceTokenId,
        uint256 strikePrice,
        OptionType optionType,
        uint256 amount
    ) internal view returns (uint256) {
        if (optionType == OptionType.CALL) {
            // For calls, collateral is the resource tokens
            return amount;
        } else {
            // For puts, collateral is strike price * amount
            return strikePrice * amount / 1e18;
        }
    }
    
    function sqrt(uint256 x) internal pure returns (uint256) {
        if (x == 0) return 0;
        uint256 z = (x + 1) / 2;
        uint256 y = x;
        while (z < y) {
            y = z;
            z = (x / z + z) / 2;
        }
        return y;
    }
    
    // Admin functions
    
    function setFees(
        uint256 _exerciseFee,
        uint256 _settlementFee
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(_exerciseFee <= 1000, "Exercise fee too high"); // Max 10%
        require(_settlementFee <= 1000, "Settlement fee too high"); // Max 10%
        exerciseFee = _exerciseFee;
        settlementFee = _settlementFee;
    }
    
    function setFeeRecipient(address _feeRecipient) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(_feeRecipient != address(0), "Invalid recipient");
        feeRecipient = _feeRecipient;
    }
    
    // ERC1155 Receiver
    function onERC1155Received(
        address,
        address,
        uint256,
        uint256,
        bytes memory
    ) public virtual returns (bytes4) {
        return this.onERC1155Received.selector;
    }
} 