// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts-upgradeable/token/ERC20/ERC20Upgradeable.sol";
import "@openzeppelin/contracts-upgradeable/access/AccessControlUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/security/PausableUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/security/ReentrancyGuardUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";
import "@chainlink/contracts/src/v0.8/interfaces/AggregatorV3Interface.sol";

interface IERC20Extended {
    function mint(address to, uint256 amount) external;
    function burn(uint256 amount) external;
    function burnFrom(address from, uint256 amount) external;
    function totalSupply() external view returns (uint256);
    function balanceOf(address account) external view returns (uint256);
    function transfer(address to, uint256 amount) external returns (bool);
    function transferFrom(address from, address to, uint256 amount) external returns (bool);
}

/**
 * @title ComputeStablecoin
 * @notice Factory and manager for compute-backed stablecoins with multiple peg mechanisms
 * @dev Implements algorithmic, collateralized, hybrid, and rebase mechanisms
 */
contract ComputeStablecoin is 
    Initializable,
    AccessControlUpgradeable,
    PausableUpgradeable,
    ReentrancyGuardUpgradeable,
    UUPSUpgradeable 
{
    bytes32 public constant OPERATOR_ROLE = keccak256("OPERATOR_ROLE");
    bytes32 public constant MINTER_ROLE = keccak256("MINTER_ROLE");
    bytes32 public constant ORACLE_ROLE = keccak256("ORACLE_ROLE");
    
    enum StablecoinType {
        CFLOPS,     // Pegged to TFLOPS
        CGPUH,      // Pegged to GPU hours
        CSTORAGE,   // Pegged to storage GB
        CBANDWIDTH, // Pegged to bandwidth Mbps
        CBASKET     // Basket of compute resources
    }
    
    enum StabilizationMode {
        ALGORITHMIC,    // Pure algorithmic with no collateral
        COLLATERALIZED, // Fully collateralized
        HYBRID,         // Partial collateral + algorithmic
        REBASE          // Supply adjusts to maintain peg
    }
    
    struct Stablecoin {
        address tokenAddress;
        StablecoinType coinType;
        StabilizationMode mode;
        uint256 targetPrice; // Target price in USD with 18 decimals
        uint256 collateralRatio; // 0-10000 (basis points)
        uint256 refreshCooldown; // Minimum time between refreshes
        uint256 lastRefreshTime;
        uint256 priceDeviationThreshold; // Basis points
        uint256 expansionRate; // Rate of supply expansion (basis points)
        uint256 contractionRate; // Rate of supply contraction (basis points)
        bool isActive;
        mapping(address => uint256) collateralBalances;
        uint256 totalCollateral;
    }
    
    struct CollateralAsset {
        address assetAddress;
        address priceFeed;
        uint256 collateralFactor; // 0-10000 basis points (e.g., 8000 = 80%)
        bool isAccepted;
        uint256 totalDeposited;
    }
    
    struct BasketWeight {
        StablecoinType coinType;
        uint256 weight; // Basis points
    }
    
    // State variables
    mapping(StablecoinType => Stablecoin) public stablecoins;
    mapping(address => CollateralAsset) public collateralAssets;
    address[] public acceptedCollaterals;
    
    mapping(StablecoinType => AggregatorV3Interface) public priceOracles;
    mapping(address => mapping(StablecoinType => uint256)) public userCollateral;
    mapping(address => mapping(StablecoinType => uint256)) public userDebt;
    
    // Basket configuration
    BasketWeight[] public basketWeights;
    uint256 public constant BASKET_PRECISION = 10000;
    
    // System parameters
    uint256 public mintFee = 30; // 0.3% in basis points
    uint256 public redeemFee = 30; // 0.3% in basis points
    uint256 public liquidationPenalty = 1000; // 10% in basis points
    uint256 public minCollateralRatio = 11000; // 110% in basis points
    
    // Emergency parameters
    uint256 public emergencyCollateralRatio = 15000; // 150% during emergency
    bool public emergencyMode = false;
    
    // Events
    event StablecoinDeployed(
        StablecoinType indexed coinType,
        address indexed tokenAddress,
        StabilizationMode mode
    );
    
    event Minted(
        address indexed user,
        StablecoinType indexed coinType,
        uint256 amount,
        uint256 collateralAmount
    );
    
    event Redeemed(
        address indexed user,
        StablecoinType indexed coinType,
        uint256 amount,
        uint256 collateralReturned
    );
    
    event Refreshed(
        StablecoinType indexed coinType,
        uint256 oldSupply,
        uint256 newSupply,
        uint256 price
    );
    
    event CollateralAdded(
        address indexed asset,
        address indexed priceFeed,
        uint256 collateralFactor
    );
    
    event Liquidated(
        address indexed user,
        StablecoinType indexed coinType,
        uint256 debtAmount,
        uint256 collateralSeized
    );
    
    event EmergencyModeToggled(bool enabled);
    
    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }
    
    function initialize() public initializer {
        __AccessControl_init();
        __Pausable_init();
        __ReentrancyGuard_init();
        __UUPSUpgradeable_init();
        
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(OPERATOR_ROLE, msg.sender);
        
        // Initialize basket weights
        basketWeights.push(BasketWeight(StablecoinType.CFLOPS, 4000)); // 40%
        basketWeights.push(BasketWeight(StablecoinType.CGPUH, 3000)); // 30%
        basketWeights.push(BasketWeight(StablecoinType.CSTORAGE, 2000)); // 20%
        basketWeights.push(BasketWeight(StablecoinType.CBANDWIDTH, 1000)); // 10%
    }
    
    /**
     * @notice Deploy a new compute stablecoin
     */
    function deployStablecoin(
        StablecoinType coinType,
        StabilizationMode mode,
        string memory name,
        string memory symbol,
        uint256 targetPrice,
        address priceFeed
    ) external onlyRole(OPERATOR_ROLE) returns (address) {
        require(!stablecoins[coinType].isActive, "Stablecoin already exists");
        
        // Deploy ERC20 token contract
        ComputeStablecoinToken token = new ComputeStablecoinToken(name, symbol);
        address tokenAddress = address(token);
        
        // Initialize stablecoin
        Stablecoin storage coin = stablecoins[coinType];
        coin.tokenAddress = tokenAddress;
        coin.coinType = coinType;
        coin.mode = mode;
        coin.targetPrice = targetPrice;
        coin.collateralRatio = mode == StabilizationMode.COLLATERALIZED ? 10000 : 
                               mode == StabilizationMode.HYBRID ? 5000 : 0;
        coin.refreshCooldown = 1 hours;
        coin.priceDeviationThreshold = 200; // 2%
        coin.expansionRate = 1000; // 10% expansion
        coin.contractionRate = 1000; // 10% contraction
        coin.isActive = true;
        
        // Set price oracle
        priceOracles[coinType] = AggregatorV3Interface(priceFeed);
        
        emit StablecoinDeployed(coinType, tokenAddress, mode);
        
        return tokenAddress;
    }
    
    /**
     * @notice Mint stablecoins
     */
    function mint(
        StablecoinType coinType,
        uint256 amount,
        address collateralAsset,
        uint256 collateralAmount
    ) external nonReentrant whenNotPaused {
        Stablecoin storage coin = stablecoins[coinType];
        require(coin.isActive, "Stablecoin not active");
        
        uint256 price = getPrice(coinType);
        uint256 value = (amount * price) / 1e18;
        
        // Calculate required collateral
        uint256 requiredCollateral = 0;
        if (coin.mode != StabilizationMode.ALGORITHMIC) {
            uint256 effectiveRatio = emergencyMode ? emergencyCollateralRatio : coin.collateralRatio;
            requiredCollateral = (value * effectiveRatio) / 10000;
            
            // Verify collateral asset
            CollateralAsset storage collateral = collateralAssets[collateralAsset];
            require(collateral.isAccepted, "Collateral not accepted");
            
            // Calculate collateral value
            uint256 collateralPrice = getCollateralPrice(collateralAsset);
            uint256 collateralValue = (collateralAmount * collateralPrice * collateral.collateralFactor) / (1e18 * 10000);
            require(collateralValue >= requiredCollateral, "Insufficient collateral");
            
            // Transfer collateral
            IERC20Extended(collateralAsset).transferFrom(msg.sender, address(this), collateralAmount);
            
            // Update accounting
            coin.collateralBalances[collateralAsset] += collateralAmount;
            coin.totalCollateral += collateralValue;
            collateral.totalDeposited += collateralAmount;
            userCollateral[msg.sender][coinType] += collateralAmount;
        }
        
        // Apply mint fee
        uint256 fee = (amount * mintFee) / 10000;
        uint256 netAmount = amount - fee;
        
        // Mint tokens
        ComputeStablecoinToken(coin.tokenAddress).mint(msg.sender, netAmount);
        if (fee > 0) {
            ComputeStablecoinToken(coin.tokenAddress).mint(address(this), fee);
        }
        
        // Update user debt
        userDebt[msg.sender][coinType] += amount;
        
        emit Minted(msg.sender, coinType, netAmount, collateralAmount);
    }
    
    /**
     * @notice Redeem stablecoins for collateral
     */
    function redeem(
        StablecoinType coinType,
        uint256 amount,
        address collateralAsset
    ) external nonReentrant whenNotPaused {
        Stablecoin storage coin = stablecoins[coinType];
        require(coin.isActive, "Stablecoin not active");
        require(coin.mode != StabilizationMode.ALGORITHMIC, "Cannot redeem algorithmic stablecoins");
        
        // Burn tokens
        ComputeStablecoinToken(coin.tokenAddress).burnFrom(msg.sender, amount);
        
        // Calculate collateral to return
        uint256 price = getPrice(coinType);
        uint256 value = (amount * price) / 1e18;
        
        // Apply redeem fee
        uint256 fee = (value * redeemFee) / 10000;
        uint256 netValue = value - fee;
        
        // Calculate collateral amount
        CollateralAsset storage collateral = collateralAssets[collateralAsset];
        require(collateral.isAccepted, "Collateral not accepted");
        
        uint256 collateralPrice = getCollateralPrice(collateralAsset);
        uint256 collateralToReturn = (netValue * 1e18) / collateralPrice;
        
        require(coin.collateralBalances[collateralAsset] >= collateralToReturn, "Insufficient collateral in pool");
        
        // Update accounting
        coin.collateralBalances[collateralAsset] -= collateralToReturn;
        coin.totalCollateral -= netValue;
        collateral.totalDeposited -= collateralToReturn;
        
        // Update user debt
        if (userDebt[msg.sender][coinType] >= amount) {
            userDebt[msg.sender][coinType] -= amount;
        } else {
            userDebt[msg.sender][coinType] = 0;
        }
        
        // Transfer collateral
        IERC20Extended(collateralAsset).transfer(msg.sender, collateralToReturn);
        
        emit Redeemed(msg.sender, coinType, amount, collateralToReturn);
    }
    
    /**
     * @notice Refresh stablecoin supply based on peg deviation
     */
    function refreshStablecoin(StablecoinType coinType) external {
        Stablecoin storage coin = stablecoins[coinType];
        require(coin.isActive, "Stablecoin not active");
        require(block.timestamp >= coin.lastRefreshTime + coin.refreshCooldown, "Cooldown period");
        
        uint256 currentPrice = getPrice(coinType);
        uint256 targetPrice = coin.targetPrice;
        
        // Calculate price deviation
        uint256 deviation = currentPrice > targetPrice ? 
            ((currentPrice - targetPrice) * 10000) / targetPrice :
            ((targetPrice - currentPrice) * 10000) / targetPrice;
            
        require(deviation >= coin.priceDeviationThreshold, "Deviation below threshold");
        
        ComputeStablecoinToken token = ComputeStablecoinToken(coin.tokenAddress);
        uint256 currentSupply = token.totalSupply();
        uint256 newSupply = currentSupply;
        
        if (coin.mode == StabilizationMode.ALGORITHMIC || coin.mode == StabilizationMode.HYBRID) {
            if (currentPrice > targetPrice) {
                // Price too high, expand supply
                uint256 expansion = (currentSupply * coin.expansionRate) / 10000;
                token.mint(address(this), expansion);
                newSupply = currentSupply + expansion;
            } else {
                // Price too low, contract supply
                uint256 contraction = (currentSupply * coin.contractionRate) / 10000;
                uint256 burnAmount = contraction > token.balanceOf(address(this)) ? 
                    token.balanceOf(address(this)) : contraction;
                if (burnAmount > 0) {
                    token.burn(burnAmount);
                    newSupply = currentSupply - burnAmount;
                }
            }
        } else if (coin.mode == StabilizationMode.REBASE) {
            // Rebase: adjust all balances proportionally
            // This would require a special rebase token implementation
            revert("Rebase not implemented in this version");
        }
        
        coin.lastRefreshTime = block.timestamp;
        
        emit Refreshed(coinType, currentSupply, newSupply, currentPrice);
    }
    
    /**
     * @notice Liquidate undercollateralized positions
     */
    function liquidate(
        address user,
        StablecoinType coinType,
        address collateralAsset
    ) external nonReentrant whenNotPaused {
        Stablecoin storage coin = stablecoins[coinType];
        require(coin.mode != StabilizationMode.ALGORITHMIC, "Cannot liquidate algorithmic positions");
        
        uint256 debt = userDebt[user][coinType];
        require(debt > 0, "No debt to liquidate");
        
        uint256 collateralAmount = userCollateral[user][coinType];
        require(collateralAmount > 0, "No collateral to liquidate");
        
        // Check if position is undercollateralized
        uint256 debtValue = (debt * getPrice(coinType)) / 1e18;
        uint256 collateralValue = (collateralAmount * getCollateralPrice(collateralAsset)) / 1e18;
        uint256 collateralRatio = (collateralValue * 10000) / debtValue;
        
        require(collateralRatio < minCollateralRatio, "Position not liquidatable");
        
        // Calculate liquidation amounts
        uint256 liquidationValue = (debtValue * (10000 + liquidationPenalty)) / 10000;
        uint256 collateralToSeize = (liquidationValue * 1e18) / getCollateralPrice(collateralAsset);
        
        if (collateralToSeize > collateralAmount) {
            collateralToSeize = collateralAmount;
        }
        
        // Update accounting
        userDebt[user][coinType] = 0;
        userCollateral[user][coinType] = 0;
        coin.collateralBalances[collateralAsset] -= collateralToSeize;
        
        // Burn debt tokens from liquidator
        ComputeStablecoinToken(coin.tokenAddress).burnFrom(msg.sender, debt);
        
        // Transfer collateral to liquidator
        IERC20Extended(collateralAsset).transfer(msg.sender, collateralToSeize);
        
        emit Liquidated(user, coinType, debt, collateralToSeize);
    }
    
    /**
     * @notice Add accepted collateral asset
     */
    function addCollateralAsset(
        address asset,
        address priceFeed,
        uint256 collateralFactor
    ) external onlyRole(OPERATOR_ROLE) {
        require(!collateralAssets[asset].isAccepted, "Already accepted");
        require(collateralFactor <= 10000, "Invalid factor");
        
        collateralAssets[asset] = CollateralAsset({
            assetAddress: asset,
            priceFeed: priceFeed,
            collateralFactor: collateralFactor,
            isAccepted: true,
            totalDeposited: 0
        });
        
        acceptedCollaterals.push(asset);
        
        emit CollateralAdded(asset, priceFeed, collateralFactor);
    }
    
    /**
     * @notice Toggle emergency mode
     */
    function toggleEmergencyMode() external onlyRole(OPERATOR_ROLE) {
        emergencyMode = !emergencyMode;
        emit EmergencyModeToggled(emergencyMode);
    }
    
    /**
     * @notice Get current price of a stablecoin type
     */
    function getPrice(StablecoinType coinType) public view returns (uint256) {
        if (coinType == StablecoinType.CBASKET) {
            // Calculate weighted average price for basket
            uint256 weightedPrice = 0;
            for (uint256 i = 0; i < basketWeights.length; i++) {
                BasketWeight memory weight = basketWeights[i];
                uint256 componentPrice = getPrice(weight.coinType);
                weightedPrice += (componentPrice * weight.weight) / BASKET_PRECISION;
            }
            return weightedPrice;
        } else {
            // Get price from oracle
            AggregatorV3Interface oracle = priceOracles[coinType];
            require(address(oracle) != address(0), "Oracle not set");
            
            (, int256 price,,,) = oracle.latestRoundData();
            return uint256(price) * 1e10; // Convert to 18 decimals
        }
    }
    
    /**
     * @notice Get collateral asset price
     */
    function getCollateralPrice(address asset) public view returns (uint256) {
        CollateralAsset storage collateral = collateralAssets[asset];
        require(collateral.isAccepted, "Collateral not accepted");
        
        AggregatorV3Interface oracle = AggregatorV3Interface(collateral.priceFeed);
        (, int256 price,,,) = oracle.latestRoundData();
        return uint256(price) * 1e10; // Convert to 18 decimals
    }
    
    /**
     * @notice Get user health factor
     */
    function getUserHealthFactor(address user, StablecoinType coinType) external view returns (uint256) {
        uint256 debt = userDebt[user][coinType];
        if (debt == 0) return type(uint256).max;
        
        uint256 collateral = userCollateral[user][coinType];
        if (collateral == 0) return 0;
        
        uint256 debtValue = (debt * getPrice(coinType)) / 1e18;
        uint256 collateralValue = (collateral * getCollateralPrice(acceptedCollaterals[0])) / 1e18; // Simplified
        
        return (collateralValue * 10000) / debtValue;
    }
    
    function _authorizeUpgrade(address newImplementation) internal override onlyRole(DEFAULT_ADMIN_ROLE) {}
}

/**
 * @title ComputeStablecoinToken
 * @notice ERC20 token implementation for compute stablecoins
 */
contract ComputeStablecoinToken is ERC20Upgradeable, AccessControlUpgradeable {
    bytes32 public constant MINTER_ROLE = keccak256("MINTER_ROLE");
    bytes32 public constant BURNER_ROLE = keccak256("BURNER_ROLE");
    
    constructor(string memory name, string memory symbol) {
        initialize(name, symbol);
    }
    
    function initialize(string memory name, string memory symbol) public initializer {
        __ERC20_init(name, symbol);
        __AccessControl_init();
        
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(MINTER_ROLE, msg.sender);
        _grantRole(BURNER_ROLE, msg.sender);
    }
    
    function mint(address to, uint256 amount) external onlyRole(MINTER_ROLE) {
        _mint(to, amount);
    }
    
    function burn(uint256 amount) external onlyRole(BURNER_ROLE) {
        _burn(msg.sender, amount);
    }
    
    function burnFrom(address from, uint256 amount) external onlyRole(BURNER_ROLE) {
        _spendAllowance(from, msg.sender, amount);
        _burn(from, amount);
    }
} 