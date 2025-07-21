// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/token/ERC721/IERC721.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/access/AccessControl.sol";
import "../ResourceToken.sol";
import "./ResourceOptions.sol";

/**
 * @title OptionsAMM
 * @notice Automated Market Maker for Resource Options
 * @dev Provides liquidity and automated pricing for options
 */
contract OptionsAMM is ReentrancyGuard, AccessControl {
    
    // Roles
    bytes32 public constant LP_ROLE = keccak256("LP_ROLE");
    bytes32 public constant KEEPER_ROLE = keccak256("KEEPER_ROLE");
    
    // Constants
    uint256 public constant PRECISION = 1e18;
    uint256 public constant BASIS_POINTS = 10000;
    uint256 public constant MIN_LIQUIDITY = 1000; // Minimum liquidity to prevent manipulation
    
    // Pool struct
    struct Pool {
        uint256 resourceTokenId;          // Resource token ID
        uint256 totalLiquidity;          // Total liquidity in pool
        uint256 resourceReserve;         // Resource token reserves
        uint256 stablecoinReserve;      // Stablecoin reserves
        uint256 utilization;             // Current utilization rate
        uint256 baseIV;                  // Base implied volatility
        bool isActive;                   // Pool status
    }
    
    // Liquidity provider struct
    struct LiquidityProvider {
        uint256 liquidity;               // LP tokens
        uint256 depositTime;             // Deposit timestamp
        uint256 resourceDeposited;       // Resources deposited
        uint256 stablecoinDeposited;    // Stablecoins deposited
    }
    
    // State variables
    ResourceToken public immutable resourceToken;
    ResourceOptions public immutable optionsContract;
    IERC20 public immutable stablecoin;
    
    mapping(uint256 => Pool) public pools; // resourceTokenId => Pool
    mapping(address => mapping(uint256 => LiquidityProvider)) public liquidityProviders;
    
    // Pricing parameters
    mapping(uint256 => uint256) public volatilitySmile; // moneyness => IV adjustment
    uint256 public timeDecayFactor = 100; // Time decay adjustment
    uint256 public skewFactor = 50; // Skew adjustment for calls vs puts
    
    // Fees
    uint256 public lpFee = 200; // 2% fee to LPs
    uint256 public protocolFee = 50; // 0.5% protocol fee
    address public feeRecipient;
    
    // Written options tracking
    mapping(uint256 => uint256[]) public poolOptions; // poolId => optionIds
    mapping(uint256 => uint256) public optionPools; // optionId => poolId
    
    // Events
    event PoolCreated(uint256 indexed resourceTokenId, uint256 initialLiquidity);
    event LiquidityAdded(
        address indexed provider,
        uint256 indexed resourceTokenId,
        uint256 resourceAmount,
        uint256 stablecoinAmount,
        uint256 liquidity
    );
    event LiquidityRemoved(
        address indexed provider,
        uint256 indexed resourceTokenId,
        uint256 liquidity,
        uint256 resourceAmount,
        uint256 stablecoinAmount
    );
    event OptionSold(
        uint256 indexed poolId,
        uint256 indexed optionId,
        address buyer,
        uint256 premium
    );
    event OptionBought(
        uint256 indexed poolId,
        uint256 indexed optionId,
        address seller,
        uint256 payout
    );
    event VolatilityUpdated(uint256 indexed resourceTokenId, uint256 newIV);
    
    /**
     * @dev Constructor
     * @param _resourceToken ResourceToken contract
     * @param _optionsContract ResourceOptions contract
     * @param _stablecoin Stablecoin contract
     */
    constructor(
        address _resourceToken,
        address _optionsContract,
        address _stablecoin
    ) {
        resourceToken = ResourceToken(_resourceToken);
        optionsContract = ResourceOptions(_optionsContract);
        stablecoin = IERC20(_stablecoin);
        
        feeRecipient = msg.sender;
        
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(LP_ROLE, msg.sender);
        _grantRole(KEEPER_ROLE, msg.sender);
    }
    
    /**
     * @notice Create a new liquidity pool
     * @param resourceTokenId Resource token ID
     * @param resourceAmount Initial resource amount
     * @param stablecoinAmount Initial stablecoin amount
     * @param baseIV Base implied volatility
     */
    function createPool(
        uint256 resourceTokenId,
        uint256 resourceAmount,
        uint256 stablecoinAmount,
        uint256 baseIV
    ) external nonReentrant {
        require(!pools[resourceTokenId].isActive, "Pool exists");
        require(resourceAmount >= MIN_LIQUIDITY, "Insufficient resources");
        require(stablecoinAmount >= MIN_LIQUIDITY, "Insufficient stablecoin");
        require(baseIV > 0 && baseIV <= 50000, "Invalid IV"); // Max 500%
        
        // Transfer tokens
        resourceToken.safeTransferFrom(msg.sender, address(this), resourceTokenId, resourceAmount, "");
        stablecoin.transferFrom(msg.sender, address(this), stablecoinAmount);
        
        // Calculate initial liquidity
        uint256 liquidity = sqrt(resourceAmount * stablecoinAmount);
        
        // Create pool
        pools[resourceTokenId] = Pool({
            resourceTokenId: resourceTokenId,
            totalLiquidity: liquidity,
            resourceReserve: resourceAmount,
            stablecoinReserve: stablecoinAmount,
            utilization: 0,
            baseIV: baseIV,
            isActive: true
        });
        
        // Record LP
        liquidityProviders[msg.sender][resourceTokenId] = LiquidityProvider({
            liquidity: liquidity,
            depositTime: block.timestamp,
            resourceDeposited: resourceAmount,
            stablecoinDeposited: stablecoinAmount
        });
        
        emit PoolCreated(resourceTokenId, liquidity);
        emit LiquidityAdded(msg.sender, resourceTokenId, resourceAmount, stablecoinAmount, liquidity);
    }
    
    /**
     * @notice Add liquidity to a pool
     * @param resourceTokenId Resource token ID
     * @param resourceAmount Resource amount to add
     * @param stablecoinAmount Stablecoin amount to add
     * @param minLiquidity Minimum liquidity to receive
     */
    function addLiquidity(
        uint256 resourceTokenId,
        uint256 resourceAmount,
        uint256 stablecoinAmount,
        uint256 minLiquidity
    ) external nonReentrant onlyRole(LP_ROLE) {
        Pool storage pool = pools[resourceTokenId];
        require(pool.isActive, "Pool not active");
        
        // Calculate liquidity based on current ratio
        uint256 resourceLiquidity = resourceAmount * pool.totalLiquidity / pool.resourceReserve;
        uint256 stablecoinLiquidity = stablecoinAmount * pool.totalLiquidity / pool.stablecoinReserve;
        
        uint256 liquidity = resourceLiquidity < stablecoinLiquidity ? resourceLiquidity : stablecoinLiquidity;
        require(liquidity >= minLiquidity, "Insufficient liquidity");
        
        // Calculate actual amounts needed
        uint256 actualResourceAmount = liquidity * pool.resourceReserve / pool.totalLiquidity;
        uint256 actualStablecoinAmount = liquidity * pool.stablecoinReserve / pool.totalLiquidity;
        
        // Transfer tokens
        resourceToken.safeTransferFrom(msg.sender, address(this), resourceTokenId, actualResourceAmount, "");
        stablecoin.transferFrom(msg.sender, address(this), actualStablecoinAmount);
        
        // Update pool
        pool.resourceReserve += actualResourceAmount;
        pool.stablecoinReserve += actualStablecoinAmount;
        pool.totalLiquidity += liquidity;
        
        // Update LP position
        LiquidityProvider storage lp = liquidityProviders[msg.sender][resourceTokenId];
        lp.liquidity += liquidity;
        lp.resourceDeposited += actualResourceAmount;
        lp.stablecoinDeposited += actualStablecoinAmount;
        
        emit LiquidityAdded(msg.sender, resourceTokenId, actualResourceAmount, actualStablecoinAmount, liquidity);
    }
    
    /**
     * @notice Remove liquidity from a pool
     * @param resourceTokenId Resource token ID
     * @param liquidity Amount of liquidity to remove
     * @param minResourceAmount Minimum resources to receive
     * @param minStablecoinAmount Minimum stablecoins to receive
     */
    function removeLiquidity(
        uint256 resourceTokenId,
        uint256 liquidity,
        uint256 minResourceAmount,
        uint256 minStablecoinAmount
    ) external nonReentrant {
        Pool storage pool = pools[resourceTokenId];
        LiquidityProvider storage lp = liquidityProviders[msg.sender][resourceTokenId];
        
        require(liquidity > 0 && liquidity <= lp.liquidity, "Invalid liquidity");
        
        // Calculate amounts to return
        uint256 resourceAmount = liquidity * pool.resourceReserve / pool.totalLiquidity;
        uint256 stablecoinAmount = liquidity * pool.stablecoinReserve / pool.totalLiquidity;
        
        require(resourceAmount >= minResourceAmount, "Insufficient resources");
        require(stablecoinAmount >= minStablecoinAmount, "Insufficient stablecoins");
        
        // Update pool
        pool.resourceReserve -= resourceAmount;
        pool.stablecoinReserve -= stablecoinAmount;
        pool.totalLiquidity -= liquidity;
        
        // Update LP position
        lp.liquidity -= liquidity;
        
        // Transfer tokens
        resourceToken.safeTransferFrom(address(this), msg.sender, resourceTokenId, resourceAmount, "");
        stablecoin.transfer(msg.sender, stablecoinAmount);
        
        emit LiquidityRemoved(msg.sender, resourceTokenId, liquidity, resourceAmount, stablecoinAmount);
    }
    
    /**
     * @notice Sell an option to a buyer
     * @param resourceTokenId Resource token ID
     * @param strikePrice Strike price
     * @param expiry Expiration timestamp
     * @param optionType Call (0) or Put (1)
     * @param amount Amount of resources
     * @param buyer Buyer address
     * @return optionId Created option ID
     * @return premium Option premium
     */
    function sellOption(
        uint256 resourceTokenId,
        uint256 strikePrice,
        uint256 expiry,
        ResourceOptions.OptionType optionType,
        uint256 amount,
        address buyer
    ) external nonReentrant onlyRole(KEEPER_ROLE) returns (uint256 optionId, uint256 premium) {
        Pool storage pool = pools[resourceTokenId];
        require(pool.isActive, "Pool not active");
        
        // Calculate premium
        premium = calculatePremium(
            resourceTokenId,
            strikePrice,
            expiry,
            optionType,
            amount
        );
        
        // Check pool has sufficient collateral
        if (optionType == ResourceOptions.OptionType.CALL) {
            require(pool.resourceReserve >= amount, "Insufficient resources");
            pool.resourceReserve -= amount; // Lock resources
        } else {
            uint256 putCollateral = strikePrice * amount / PRECISION;
            require(pool.stablecoinReserve >= putCollateral, "Insufficient collateral");
            pool.stablecoinReserve -= putCollateral; // Lock stablecoins
        }
        
        // Approve options contract
        resourceToken.setApprovalForAll(address(optionsContract), true);
        stablecoin.approve(address(optionsContract), type(uint256).max);
        
        // Write and sell option
        optionId = optionsContract.writeAndSellOption(
            resourceTokenId,
            strikePrice,
            expiry,
            optionType,
            ResourceOptions.OptionStyle.EUROPEAN,
            amount,
            buyer,
            premium
        );
        
        // Track option
        poolOptions[resourceTokenId].push(optionId);
        optionPools[optionId] = resourceTokenId;
        
        // Update utilization
        _updateUtilization(resourceTokenId);
        
        // Collect premium
        stablecoin.transferFrom(buyer, address(this), premium);
        
        // Distribute fees
        uint256 lpShare = premium * lpFee / BASIS_POINTS;
        uint256 protocolShare = premium * protocolFee / BASIS_POINTS;
        
        pool.stablecoinReserve += premium - protocolShare;
        stablecoin.transfer(feeRecipient, protocolShare);
        
        emit OptionSold(resourceTokenId, optionId, buyer, premium);
        
        return (optionId, premium);
    }
    
    /**
     * @notice Calculate option premium
     * @param resourceTokenId Resource token ID
     * @param strikePrice Strike price
     * @param expiry Expiration timestamp
     * @param optionType Call or Put
     * @param amount Amount of resources
     * @return premium Option premium
     */
    function calculatePremium(
        uint256 resourceTokenId,
        uint256 strikePrice,
        uint256 expiry,
        ResourceOptions.OptionType optionType,
        uint256 amount
    ) public view returns (uint256) {
        Pool memory pool = pools[resourceTokenId];
        require(pool.isActive, "Pool not active");
        
        // Get spot price
        uint256 spotPrice = optionsContract.spotPrices(resourceTokenId);
        if (spotPrice == 0) {
            // Use pool ratio as spot price
            spotPrice = pool.stablecoinReserve * PRECISION / pool.resourceReserve;
        }
        
        // Calculate time to expiry
        uint256 timeToExpiry = expiry > block.timestamp ? expiry - block.timestamp : 0;
        
        // Adjust IV based on utilization
        uint256 utilizationAdjustment = pool.utilization * pool.baseIV / BASIS_POINTS / 2;
        uint256 adjustedIV = pool.baseIV + utilizationAdjustment;
        
        // Apply volatility smile
        uint256 moneyness = spotPrice > strikePrice ? 
            (spotPrice - strikePrice) * BASIS_POINTS / spotPrice :
            (strikePrice - spotPrice) * BASIS_POINTS / strikePrice;
        
        uint256 smileAdjustment = volatilitySmile[moneyness / 100]; // Group by percentage
        adjustedIV = adjustedIV * (BASIS_POINTS + smileAdjustment) / BASIS_POINTS;
        
        // Calculate intrinsic value
        uint256 intrinsicValue = 0;
        if (optionType == ResourceOptions.OptionType.CALL && spotPrice > strikePrice) {
            intrinsicValue = spotPrice - strikePrice;
        } else if (optionType == ResourceOptions.OptionType.PUT && strikePrice > spotPrice) {
            intrinsicValue = strikePrice - spotPrice;
        }
        
        // Calculate time value (simplified Black-Scholes approximation)
        uint256 timeValue = spotPrice * adjustedIV * sqrt(timeToExpiry) / (100 * sqrt(365 days));
        
        // Apply time decay factor
        timeValue = timeValue * (BASIS_POINTS - timeDecayFactor * (365 days - timeToExpiry) / 365 days) / BASIS_POINTS;
        
        // Apply skew for puts vs calls
        if (optionType == ResourceOptions.OptionType.PUT) {
            timeValue = timeValue * (BASIS_POINTS + skewFactor) / BASIS_POINTS;
        }
        
        // Total premium
        uint256 premium = (intrinsicValue + timeValue) * amount / PRECISION;
        
        // Minimum premium
        uint256 minPremium = spotPrice * amount * 10 / BASIS_POINTS; // 0.1% minimum
        
        return premium > minPremium ? premium : minPremium;
    }
    
    /**
     * @notice Handle option exercise/expiry
     * @param optionId Option ID
     */
    function settleOption(uint256 optionId) external nonReentrant {
        uint256 poolId = optionPools[optionId];
        require(poolId > 0, "Option not from pool");
        
        Pool storage pool = pools[poolId];
        ResourceOptions.Option memory option = optionsContract.getOption(optionId);
        
        // Check if expired or exercised
        if (option.expired || option.exercised) {
            // Return collateral to pool if expired
            if (option.expired && !option.exercised) {
                if (option.optionType == ResourceOptions.OptionType.CALL) {
                    pool.resourceReserve += option.amount;
                } else {
                    pool.stablecoinReserve += option.collateral;
                }
            }
            
            // Update utilization
            _updateUtilization(poolId);
            
            // Remove from tracking
            _removeOption(poolId, optionId);
        }
    }
    
    /**
     * @notice Update base IV for a pool
     * @param resourceTokenId Resource token ID
     * @param newIV New implied volatility
     */
    function updateVolatility(
        uint256 resourceTokenId,
        uint256 newIV
    ) external onlyRole(KEEPER_ROLE) {
        require(newIV > 0 && newIV <= 50000, "Invalid IV");
        pools[resourceTokenId].baseIV = newIV;
        emit VolatilityUpdated(resourceTokenId, newIV);
    }
    
    /**
     * @notice Update volatility smile
     * @param moneyness Moneyness level (0-100)
     * @param adjustment IV adjustment in basis points
     */
    function updateVolatilitySmile(
        uint256 moneyness,
        uint256 adjustment
    ) external onlyRole(KEEPER_ROLE) {
        require(moneyness <= 100, "Invalid moneyness");
        require(adjustment <= 5000, "Adjustment too high"); // Max 50% adjustment
        volatilitySmile[moneyness] = adjustment;
    }
    
    /**
     * @notice Get pool info
     * @param resourceTokenId Resource token ID
     * @return pool Pool details
     * @return activeOptions Number of active options
     */
    function getPoolInfo(uint256 resourceTokenId) external view returns (
        Pool memory pool,
        uint256 activeOptions
    ) {
        pool = pools[resourceTokenId];
        activeOptions = poolOptions[resourceTokenId].length;
    }
    
    // Internal functions
    
    function _updateUtilization(uint256 resourceTokenId) internal {
        Pool storage pool = pools[resourceTokenId];
        
        uint256 totalValue = pool.resourceReserve + pool.stablecoinReserve;
        uint256 activeValue = 0;
        
        // Calculate value locked in options
        uint256[] memory optionIds = poolOptions[resourceTokenId];
        for (uint256 i = 0; i < optionIds.length; i++) {
            ResourceOptions.Option memory option = optionsContract.getOption(optionIds[i]);
            if (!option.expired && !option.exercised) {
                if (option.optionType == ResourceOptions.OptionType.CALL) {
                    activeValue += option.amount;
                } else {
                    activeValue += option.collateral;
                }
            }
        }
        
        pool.utilization = totalValue > 0 ? activeValue * BASIS_POINTS / totalValue : 0;
    }
    
    function _removeOption(uint256 poolId, uint256 optionId) internal {
        uint256[] storage options = poolOptions[poolId];
        for (uint256 i = 0; i < options.length; i++) {
            if (options[i] == optionId) {
                options[i] = options[options.length - 1];
                options.pop();
                break;
            }
        }
        delete optionPools[optionId];
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
        uint256 _lpFee,
        uint256 _protocolFee
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(_lpFee <= 500, "LP fee too high"); // Max 5%
        require(_protocolFee <= 100, "Protocol fee too high"); // Max 1%
        lpFee = _lpFee;
        protocolFee = _protocolFee;
    }
    
    function setFeeRecipient(address _feeRecipient) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(_feeRecipient != address(0), "Invalid recipient");
        feeRecipient = _feeRecipient;
    }
    
    function setPricingParameters(
        uint256 _timeDecayFactor,
        uint256 _skewFactor
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(_timeDecayFactor <= 200, "Time decay too high");
        require(_skewFactor <= 200, "Skew too high");
        timeDecayFactor = _timeDecayFactor;
        skewFactor = _skewFactor;
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