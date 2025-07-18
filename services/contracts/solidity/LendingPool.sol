// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/security/Pausable.sol";
import "@openzeppelin/contracts/access/AccessControl.sol";
import "@chainlink/contracts/src/v0.8/interfaces/AggregatorV3Interface.sol";

/**
 * @title LendingPool
 * @dev Decentralized lending pool with collateralized borrowing
 * Features: Over-collateralization, liquidation, interest rates
 */
contract LendingPool is ReentrancyGuard, Pausable, AccessControl {
    using SafeERC20 for IERC20;

    // Roles
    bytes32 public constant LIQUIDATOR_ROLE = keccak256("LIQUIDATOR_ROLE");
    bytes32 public constant ORACLE_ROLE = keccak256("ORACLE_ROLE");
    bytes32 public constant RISK_MANAGER_ROLE = keccak256("RISK_MANAGER_ROLE");

    // Asset configuration
    struct AssetConfig {
        bool isActive;
        uint256 ltv; // Loan-to-value ratio (basis points, e.g., 8000 = 80%)
        uint256 liquidationThreshold; // Basis points
        uint256 liquidationBonus; // Basis points
        uint256 reserveFactor; // Basis points
        address priceOracle;
        uint256 baseInterestRate; // Annual rate in basis points
        uint256 rateSlope1; // Rate increase per utilization
        uint256 rateSlope2; // Rate increase after optimal utilization
        uint256 optimalUtilization; // Basis points
    }

    // User account data
    struct UserAccount {
        mapping(address => uint256) deposits;
        mapping(address => uint256) borrows;
        mapping(address => uint256) borrowIndex;
        uint256 lastUpdateTimestamp;
    }

    // Asset data
    struct AssetData {
        uint256 totalDeposits;
        uint256 totalBorrows;
        uint256 borrowIndex;
        uint256 lastUpdateTimestamp;
        uint256 reserveBalance;
    }

    // Constants
    uint256 public constant PRECISION = 10000; // Basis points
    uint256 public constant SECONDS_PER_YEAR = 365 days;
    uint256 public constant HEALTH_FACTOR_LIQUIDATION_THRESHOLD = 1e18;

    // State variables
    mapping(address => AssetConfig) public assetConfigs;
    mapping(address => AssetData) public assetData;
    mapping(address => UserAccount) private userAccounts;
    address[] public supportedAssets;

    // Interest rate model parameters
    uint256 public constant MIN_INTEREST_RATE = 100; // 1%
    uint256 public constant MAX_INTEREST_RATE = 5000; // 50%

    // Events
    event AssetAdded(address indexed asset, address priceOracle);
    event Deposited(address indexed user, address indexed asset, uint256 amount);
    event Withdrawn(address indexed user, address indexed asset, uint256 amount);
    event Borrowed(address indexed user, address indexed asset, uint256 amount);
    event Repaid(address indexed user, address indexed asset, uint256 amount);
    event Liquidated(
        address indexed liquidator,
        address indexed borrower,
        address indexed collateralAsset,
        address debtAsset,
        uint256 debtAmount,
        uint256 collateralAmount
    );
    event ReserveWithdrawn(address indexed asset, uint256 amount, address to);
    event InterestAccrued(address indexed asset, uint256 borrowIndex);

    constructor() {
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(LIQUIDATOR_ROLE, msg.sender);
        _grantRole(ORACLE_ROLE, msg.sender);
        _grantRole(RISK_MANAGER_ROLE, msg.sender);
    }

    /**
     * @dev Add a new asset to the lending pool
     */
    function addAsset(
        address asset,
        uint256 ltv,
        uint256 liquidationThreshold,
        uint256 liquidationBonus,
        uint256 reserveFactor,
        address priceOracle,
        uint256 baseInterestRate,
        uint256 rateSlope1,
        uint256 rateSlope2,
        uint256 optimalUtilization
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(asset != address(0), "Invalid asset");
        require(!assetConfigs[asset].isActive, "Asset already added");
        require(priceOracle != address(0), "Invalid oracle");
        require(ltv < liquidationThreshold, "Invalid LTV");
        require(liquidationThreshold < PRECISION, "Invalid threshold");

        assetConfigs[asset] = AssetConfig({
            isActive: true,
            ltv: ltv,
            liquidationThreshold: liquidationThreshold,
            liquidationBonus: liquidationBonus,
            reserveFactor: reserveFactor,
            priceOracle: priceOracle,
            baseInterestRate: baseInterestRate,
            rateSlope1: rateSlope1,
            rateSlope2: rateSlope2,
            optimalUtilization: optimalUtilization
        });

        assetData[asset].borrowIndex = 1e18; // Initialize to 1
        assetData[asset].lastUpdateTimestamp = block.timestamp;

        supportedAssets.push(asset);

        emit AssetAdded(asset, priceOracle);
    }

    /**
     * @dev Deposit assets into the pool
     */
    function deposit(address asset, uint256 amount) 
        external 
        nonReentrant 
        whenNotPaused 
    {
        require(assetConfigs[asset].isActive, "Asset not supported");
        require(amount > 0, "Invalid amount");

        // Update interest rates
        _updateInterest(asset);

        // Transfer tokens from user
        IERC20(asset).safeTransferFrom(msg.sender, address(this), amount);

        // Update balances
        userAccounts[msg.sender].deposits[asset] += amount;
        assetData[asset].totalDeposits += amount;

        emit Deposited(msg.sender, asset, amount);
    }

    /**
     * @dev Withdraw assets from the pool
     */
    function withdraw(address asset, uint256 amount) 
        external 
        nonReentrant 
        whenNotPaused 
    {
        require(amount > 0, "Invalid amount");
        require(userAccounts[msg.sender].deposits[asset] >= amount, "Insufficient balance");

        // Update interest rates
        _updateInterest(asset);

        // Check health factor after withdrawal
        userAccounts[msg.sender].deposits[asset] -= amount;
        require(_calculateHealthFactor(msg.sender) >= HEALTH_FACTOR_LIQUIDATION_THRESHOLD, "Withdrawal would cause undercollateralization");

        // Update totals
        assetData[asset].totalDeposits -= amount;

        // Transfer tokens to user
        IERC20(asset).safeTransfer(msg.sender, amount);

        emit Withdrawn(msg.sender, asset, amount);
    }

    /**
     * @dev Borrow assets from the pool
     */
    function borrow(address asset, uint256 amount) 
        external 
        nonReentrant 
        whenNotPaused 
    {
        require(assetConfigs[asset].isActive, "Asset not supported");
        require(amount > 0, "Invalid amount");

        // Update interest rates
        _updateInterest(asset);

        // Check available liquidity
        uint256 availableLiquidity = _getAvailableLiquidity(asset);
        require(amount <= availableLiquidity, "Insufficient liquidity");

        // Update user's borrow with accumulated interest
        _updateUserBorrowBalance(msg.sender, asset);

        // Add new borrow amount
        userAccounts[msg.sender].borrows[asset] += amount;
        assetData[asset].totalBorrows += amount;

        // Check health factor
        require(_calculateHealthFactor(msg.sender) >= HEALTH_FACTOR_LIQUIDATION_THRESHOLD, "Insufficient collateral");

        // Transfer tokens to user
        IERC20(asset).safeTransfer(msg.sender, amount);

        emit Borrowed(msg.sender, asset, amount);
    }

    /**
     * @dev Repay borrowed assets
     */
    function repay(address asset, uint256 amount) 
        external 
        nonReentrant 
        whenNotPaused 
    {
        require(amount > 0, "Invalid amount");

        // Update interest rates
        _updateInterest(asset);

        // Update user's borrow with accumulated interest
        _updateUserBorrowBalance(msg.sender, asset);

        uint256 userBorrow = userAccounts[msg.sender].borrows[asset];
        uint256 repayAmount = amount > userBorrow ? userBorrow : amount;

        // Transfer tokens from user
        IERC20(asset).safeTransferFrom(msg.sender, address(this), repayAmount);

        // Update balances
        userAccounts[msg.sender].borrows[asset] -= repayAmount;
        assetData[asset].totalBorrows -= repayAmount;

        emit Repaid(msg.sender, asset, repayAmount);
    }

    /**
     * @dev Liquidate an undercollateralized position
     */
    function liquidate(
        address borrower,
        address collateralAsset,
        address debtAsset,
        uint256 debtAmount
    ) external nonReentrant whenNotPaused {
        require(borrower != msg.sender, "Cannot liquidate yourself");
        require(_calculateHealthFactor(borrower) < HEALTH_FACTOR_LIQUIDATION_THRESHOLD, "Position is healthy");

        // Update interest rates
        _updateInterest(collateralAsset);
        _updateInterest(debtAsset);

        // Update borrower's balances
        _updateUserBorrowBalance(borrower, debtAsset);

        uint256 userDebt = userAccounts[borrower].borrows[debtAsset];
        uint256 actualDebtAmount = debtAmount > userDebt ? userDebt : debtAmount;

        // Calculate collateral to seize
        uint256 collateralAmount = _calculateCollateralToSeize(
            collateralAsset,
            debtAsset,
            actualDebtAmount
        );

        require(userAccounts[borrower].deposits[collateralAsset] >= collateralAmount, "Insufficient collateral");

        // Transfer debt payment from liquidator
        IERC20(debtAsset).safeTransferFrom(msg.sender, address(this), actualDebtAmount);

        // Update debt
        userAccounts[borrower].borrows[debtAsset] -= actualDebtAmount;
        assetData[debtAsset].totalBorrows -= actualDebtAmount;

        // Transfer collateral to liquidator
        userAccounts[borrower].deposits[collateralAsset] -= collateralAmount;
        assetData[collateralAsset].totalDeposits -= collateralAmount;
        IERC20(collateralAsset).safeTransfer(msg.sender, collateralAmount);

        emit Liquidated(msg.sender, borrower, collateralAsset, debtAsset, actualDebtAmount, collateralAmount);
    }

    /**
     * @dev Calculate health factor for a user
     */
    function calculateHealthFactor(address user) external view returns (uint256) {
        return _calculateHealthFactor(user);
    }

    /**
     * @dev Get user account data
     */
    function getUserAccountData(address user) 
        external 
        view 
        returns (
            uint256 totalCollateralUSD,
            uint256 totalDebtUSD,
            uint256 availableBorrowsUSD,
            uint256 ltv,
            uint256 healthFactor
        ) 
    {
        (totalCollateralUSD, totalDebtUSD, ltv) = _getUserAccountDataUSD(user);
        
        if (ltv > 0) {
            availableBorrowsUSD = (totalCollateralUSD * ltv / PRECISION) - totalDebtUSD;
        }
        
        healthFactor = _calculateHealthFactor(user);
    }

    /**
     * @dev Calculate current borrow rate
     */
    function getCurrentBorrowRate(address asset) external view returns (uint256) {
        return _calculateBorrowRate(asset);
    }

    /**
     * @dev Calculate current supply rate
     */
    function getCurrentSupplyRate(address asset) external view returns (uint256) {
        uint256 borrowRate = _calculateBorrowRate(asset);
        uint256 utilizationRate = _calculateUtilizationRate(asset);
        AssetConfig memory config = assetConfigs[asset];
        
        return borrowRate * utilizationRate * (PRECISION - config.reserveFactor) / PRECISION / PRECISION;
    }

    // Internal functions

    function _updateInterest(address asset) internal {
        AssetData storage data = assetData[asset];
        
        if (block.timestamp > data.lastUpdateTimestamp) {
            uint256 timeDelta = block.timestamp - data.lastUpdateTimestamp;
            uint256 borrowRate = _calculateBorrowRate(asset);
            
            if (data.totalBorrows > 0 && borrowRate > 0) {
                uint256 interestAccumulated = data.totalBorrows * borrowRate * timeDelta / SECONDS_PER_YEAR / PRECISION;
                uint256 reserveAmount = interestAccumulated * assetConfigs[asset].reserveFactor / PRECISION;
                
                data.totalBorrows += interestAccumulated;
                data.reserveBalance += reserveAmount;
                
                // Update borrow index
                data.borrowIndex = data.borrowIndex * (PRECISION + (borrowRate * timeDelta / SECONDS_PER_YEAR)) / PRECISION;
                
                emit InterestAccrued(asset, data.borrowIndex);
            }
            
            data.lastUpdateTimestamp = block.timestamp;
        }
    }

    function _updateUserBorrowBalance(address user, address asset) internal {
        UserAccount storage account = userAccounts[user];
        AssetData storage data = assetData[asset];
        
        if (account.borrows[asset] > 0) {
            account.borrows[asset] = account.borrows[asset] * data.borrowIndex / account.borrowIndex[asset];
        }
        
        account.borrowIndex[asset] = data.borrowIndex;
        account.lastUpdateTimestamp = block.timestamp;
    }

    function _calculateBorrowRate(address asset) internal view returns (uint256) {
        AssetConfig memory config = assetConfigs[asset];
        uint256 utilizationRate = _calculateUtilizationRate(asset);
        
        if (utilizationRate <= config.optimalUtilization) {
            return config.baseInterestRate + (utilizationRate * config.rateSlope1 / config.optimalUtilization);
        } else {
            uint256 excessUtilization = utilizationRate - config.optimalUtilization;
            return config.baseInterestRate + config.rateSlope1 + 
                   (excessUtilization * config.rateSlope2 / (PRECISION - config.optimalUtilization));
        }
    }

    function _calculateUtilizationRate(address asset) internal view returns (uint256) {
        AssetData memory data = assetData[asset];
        
        if (data.totalDeposits == 0) {
            return 0;
        }
        
        return data.totalBorrows * PRECISION / data.totalDeposits;
    }

    function _getAvailableLiquidity(address asset) internal view returns (uint256) {
        AssetData memory data = assetData[asset];
        return data.totalDeposits - data.totalBorrows;
    }

    function _calculateHealthFactor(address user) internal view returns (uint256) {
        (uint256 totalCollateralUSD, uint256 totalDebtUSD, ) = _getUserAccountDataUSD(user);
        
        if (totalDebtUSD == 0) {
            return type(uint256).max;
        }
        
        // Get weighted liquidation threshold
        uint256 liquidationThreshold = _getWeightedLiquidationThreshold(user);
        
        return totalCollateralUSD * liquidationThreshold * 1e18 / totalDebtUSD / PRECISION;
    }

    function _getUserAccountDataUSD(address user) 
        internal 
        view 
        returns (uint256 totalCollateralUSD, uint256 totalDebtUSD, uint256 avgLtv) 
    {
        uint256 totalLtvWeighted;
        
        for (uint256 i = 0; i < supportedAssets.length; i++) {
            address asset = supportedAssets[i];
            AssetConfig memory config = assetConfigs[asset];
            
            uint256 userDeposit = userAccounts[user].deposits[asset];
            uint256 userBorrow = userAccounts[user].borrows[asset];
            
            if (userDeposit > 0 || userBorrow > 0) {
                uint256 assetPriceUSD = _getAssetPrice(asset);
                
                if (userDeposit > 0) {
                    uint256 depositValueUSD = userDeposit * assetPriceUSD / 1e18;
                    totalCollateralUSD += depositValueUSD;
                    totalLtvWeighted += depositValueUSD * config.ltv;
                }
                
                if (userBorrow > 0) {
                    // Apply accumulated interest
                    uint256 borrowWithInterest = userBorrow * assetData[asset].borrowIndex / 
                                                userAccounts[user].borrowIndex[asset];
                    totalDebtUSD += borrowWithInterest * assetPriceUSD / 1e18;
                }
            }
        }
        
        if (totalCollateralUSD > 0) {
            avgLtv = totalLtvWeighted / totalCollateralUSD;
        }
    }

    function _getWeightedLiquidationThreshold(address user) internal view returns (uint256) {
        uint256 totalCollateralUSD;
        uint256 totalThresholdWeighted;
        
        for (uint256 i = 0; i < supportedAssets.length; i++) {
            address asset = supportedAssets[i];
            uint256 userDeposit = userAccounts[user].deposits[asset];
            
            if (userDeposit > 0) {
                AssetConfig memory config = assetConfigs[asset];
                uint256 assetPriceUSD = _getAssetPrice(asset);
                uint256 depositValueUSD = userDeposit * assetPriceUSD / 1e18;
                
                totalCollateralUSD += depositValueUSD;
                totalThresholdWeighted += depositValueUSD * config.liquidationThreshold;
            }
        }
        
        if (totalCollateralUSD > 0) {
            return totalThresholdWeighted / totalCollateralUSD;
        }
        
        return 0;
    }

    function _calculateCollateralToSeize(
        address collateralAsset,
        address debtAsset,
        uint256 debtAmount
    ) internal view returns (uint256) {
        uint256 debtPriceUSD = _getAssetPrice(debtAsset);
        uint256 collateralPriceUSD = _getAssetPrice(collateralAsset);
        uint256 liquidationBonus = assetConfigs[collateralAsset].liquidationBonus;
        
        // Calculate collateral amount with liquidation bonus
        return debtAmount * debtPriceUSD * (PRECISION + liquidationBonus) / collateralPriceUSD / PRECISION;
    }

    function _getAssetPrice(address asset) internal view returns (uint256) {
        address oracle = assetConfigs[asset].priceOracle;
        AggregatorV3Interface priceFeed = AggregatorV3Interface(oracle);
        
        (, int256 price, , , ) = priceFeed.latestRoundData();
        require(price > 0, "Invalid price");
        
        // Normalize to 18 decimals
        uint8 decimals = priceFeed.decimals();
        return uint256(price) * 10**(18 - decimals);
    }

    // Admin functions

    function withdrawReserve(address asset, uint256 amount, address to) 
        external 
        onlyRole(DEFAULT_ADMIN_ROLE) 
    {
        require(amount <= assetData[asset].reserveBalance, "Insufficient reserve");
        
        assetData[asset].reserveBalance -= amount;
        IERC20(asset).safeTransfer(to, amount);
        
        emit ReserveWithdrawn(asset, amount, to);
    }

    function updateAssetConfig(
        address asset,
        uint256 ltv,
        uint256 liquidationThreshold,
        uint256 liquidationBonus,
        uint256 reserveFactor
    ) external onlyRole(RISK_MANAGER_ROLE) {
        require(assetConfigs[asset].isActive, "Asset not active");
        require(ltv < liquidationThreshold, "Invalid LTV");
        
        AssetConfig storage config = assetConfigs[asset];
        config.ltv = ltv;
        config.liquidationThreshold = liquidationThreshold;
        config.liquidationBonus = liquidationBonus;
        config.reserveFactor = reserveFactor;
    }

    function pause() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _pause();
    }

    function unpause() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _unpause();
    }
} 