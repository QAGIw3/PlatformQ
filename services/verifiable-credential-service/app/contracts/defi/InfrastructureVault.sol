// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/token/ERC1155/IERC1155.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/security/Pausable.sol";
import "@openzeppelin/contracts/utils/math/Math.sol";
import "./ResourceToken.sol";
import "./ResourceAMM.sol";
import "./InfrastructureLending.sol";
import "./FlashResourceProvider.sol";
import "./ResourceStaking.sol";

/**
 * @title InfrastructureVault
 * @notice Automated yield optimization vault for infrastructure resources
 * @dev Implements multiple strategies similar to Yearn vaults
 */
contract InfrastructureVault is ERC20, ReentrancyGuard, AccessControl, Pausable {
    using Math for uint256;
    
    // Roles
    bytes32 public constant STRATEGIST_ROLE = keccak256("STRATEGIST_ROLE");
    bytes32 public constant KEEPER_ROLE = keccak256("KEEPER_ROLE");
    bytes32 public constant GUARDIAN_ROLE = keccak256("GUARDIAN_ROLE");
    
    // Constants
    uint256 public constant MAX_BPS = 10000;
    uint256 public constant SECS_PER_YEAR = 31556952; // 365.2425 days
    uint256 public constant MAX_STRATEGIES = 20;
    uint256 public constant DEGRADATION_COEFFICIENT = 10**18;
    
    // Vault configuration
    uint256 public immutable resourceTokenId; // Resource token ID this vault manages
    ResourceToken public immutable resourceToken;
    ResourceAMM public immutable resourceAMM;
    InfrastructureLending public immutable lendingContract;
    FlashResourceProvider public immutable flashProvider;
    ResourceStaking public immutable stakingContract;
    
    // Vault state
    uint256 public totalDebt; // Total assets allocated to strategies
    uint256 public lastReport; // Last time strategies reported
    uint256 public lockedProfit; // Profit locked from harvests
    uint256 public lockedProfitDegradation; // Rate of locked profit release
    
    // Fee configuration
    uint256 public managementFee = 200; // 2% annual management fee
    uint256 public performanceFee = 1000; // 10% performance fee
    address public rewards; // Fee recipient
    
    // Strategy struct
    struct StrategyParams {
        uint256 performanceFee;      // Strategist's fee
        uint256 activation;          // Activation block.timestamp
        uint256 debtRatio;          // Maximum borrow amount (in BPS of total assets)
        uint256 minDebtPerHarvest;  // Lower limit on increase of debt since last harvest
        uint256 maxDebtPerHarvest;  // Upper limit on increase of debt since last harvest
        uint256 lastReport;         // Last report block.timestamp
        uint256 totalDebt;          // Total outstanding debt
        uint256 totalGain;          // Total returns from strategy
        uint256 totalLoss;          // Total losses from strategy
    }
    
    // Strategy interface
    interface IStrategy {
        function want() external view returns (address);
        function vault() external view returns (address);
        function estimatedTotalAssets() external view returns (uint256);
        function withdraw(uint256 _amount) external returns (uint256);
        function migrate(address _newStrategy) external;
        function harvest() external;
        function name() external view returns (string memory);
    }
    
    // State variables
    mapping(address => StrategyParams) public strategies;
    address[] public withdrawalQueue;
    bool public emergencyShutdown;
    
    // Events
    event StrategyAdded(address indexed strategy, uint256 debtRatio, uint256 minDebtPerHarvest, uint256 maxDebtPerHarvest);
    event StrategyReported(
        address indexed strategy,
        uint256 gain,
        uint256 loss,
        uint256 debtPaid,
        uint256 totalGain,
        uint256 totalLoss,
        uint256 totalDebt,
        uint256 debtAdded,
        uint256 debtRatio
    );
    event UpdatedStrategyParams(address indexed strategy, uint256 debtRatio, uint256 minDebtPerHarvest, uint256 maxDebtPerHarvest);
    event UpdatedRewards(address rewards);
    event UpdatedManagementFee(uint256 managementFee);
    event UpdatedPerformanceFee(uint256 performanceFee);
    event StrategyMigrated(address indexed oldVersion, address indexed newVersion);
    event EmergencyShutdown(bool active);
    
    /**
     * @dev Constructor
     * @param _resourceToken Address of ResourceToken contract
     * @param _resourceTokenId Resource token ID this vault manages
     * @param _resourceAMM Address of ResourceAMM contract
     * @param _lendingContract Address of InfrastructureLending contract
     * @param _flashProvider Address of FlashResourceProvider contract
     * @param _stakingContract Address of ResourceStaking contract
     * @param _name Vault token name
     * @param _symbol Vault token symbol
     */
    constructor(
        address _resourceToken,
        uint256 _resourceTokenId,
        address _resourceAMM,
        address _lendingContract,
        address _flashProvider,
        address _stakingContract,
        string memory _name,
        string memory _symbol
    ) ERC20(_name, _symbol) {
        resourceToken = ResourceToken(_resourceToken);
        resourceTokenId = _resourceTokenId;
        resourceAMM = ResourceAMM(_resourceAMM);
        lendingContract = InfrastructureLending(_lendingContract);
        flashProvider = FlashResourceProvider(_flashProvider);
        stakingContract = ResourceStaking(_stakingContract);
        
        lastReport = block.timestamp;
        lockedProfitDegradation = (DEGRADATION_COEFFICIENT * 46) / 10**6; // 6 hours
        rewards = msg.sender;
        
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(STRATEGIST_ROLE, msg.sender);
        _grantRole(KEEPER_ROLE, msg.sender);
        _grantRole(GUARDIAN_ROLE, msg.sender);
    }
    
    /**
     * @notice Deposit resource tokens and receive vault shares
     * @param _amount Amount of resource tokens to deposit
     * @param _recipient Recipient of vault shares
     * @return shares Amount of shares minted
     */
    function deposit(uint256 _amount, address _recipient) external nonReentrant returns (uint256 shares) {
        require(!emergencyShutdown, "Emergency shutdown");
        require(_amount > 0, "Zero amount");
        
        // Calculate shares
        shares = _issueSharesForAmount(_recipient, _amount);
        
        // Transfer resource tokens
        resourceToken.safeTransferFrom(msg.sender, address(this), resourceTokenId, _amount, "");
        
        return shares;
    }
    
    /**
     * @notice Withdraw resource tokens by burning vault shares
     * @param _maxShares Maximum shares to burn
     * @param _recipient Recipient of resource tokens
     * @param _maxLoss Maximum acceptable loss in basis points
     * @return amount Amount of resource tokens withdrawn
     */
    function withdraw(
        uint256 _maxShares,
        address _recipient,
        uint256 _maxLoss
    ) external nonReentrant returns (uint256 amount) {
        require(_maxShares > 0, "Zero shares");
        require(_maxLoss <= MAX_BPS, "Loss too high");
        
        uint256 shares = Math.min(_maxShares, balanceOf(msg.sender));
        amount = _shareValue(shares);
        
        // Check available balance
        uint256 vaultBalance = resourceToken.balanceOf(address(this), resourceTokenId);
        
        if (amount > vaultBalance) {
            uint256 amountNeeded = amount - vaultBalance;
            
            // Withdraw from strategies
            amountNeeded = _withdrawFromStrategies(amountNeeded);
            amount = vaultBalance + amountNeeded;
            
            // Check loss
            uint256 loss = _shareValue(shares) - amount;
            require(loss <= _maxLoss * _shareValue(shares) / MAX_BPS, "Loss limit exceeded");
        }
        
        // Burn shares and transfer tokens
        _burn(msg.sender, shares);
        resourceToken.safeTransferFrom(address(this), _recipient, resourceTokenId, amount, "");
        
        return amount;
    }
    
    /**
     * @notice Add a new strategy to the vault
     * @param _strategy Strategy address
     * @param _debtRatio Target allocation in basis points
     * @param _minDebtPerHarvest Minimum change in debt per harvest
     * @param _maxDebtPerHarvest Maximum change in debt per harvest
     */
    function addStrategy(
        address _strategy,
        uint256 _debtRatio,
        uint256 _minDebtPerHarvest,
        uint256 _maxDebtPerHarvest
    ) external onlyRole(STRATEGIST_ROLE) {
        require(_strategy != address(0), "Invalid strategy");
        require(strategies[_strategy].activation == 0, "Strategy already exists");
        require(withdrawalQueue.length < MAX_STRATEGIES, "Too many strategies");
        require(_debtRatio <= MAX_BPS, "Invalid debt ratio");
        require(_minDebtPerHarvest <= _maxDebtPerHarvest, "Invalid debt limits");
        
        // Verify strategy interface
        require(IStrategy(_strategy).vault() == address(this), "Strategy vault mismatch");
        require(IStrategy(_strategy).want() == address(resourceToken), "Strategy want mismatch");
        
        strategies[_strategy] = StrategyParams({
            performanceFee: performanceFee,
            activation: block.timestamp,
            debtRatio: _debtRatio,
            minDebtPerHarvest: _minDebtPerHarvest,
            maxDebtPerHarvest: _maxDebtPerHarvest,
            lastReport: block.timestamp,
            totalDebt: 0,
            totalGain: 0,
            totalLoss: 0
        });
        
        // Add to withdrawal queue
        withdrawalQueue.push(_strategy);
        
        emit StrategyAdded(_strategy, _debtRatio, _minDebtPerHarvest, _maxDebtPerHarvest);
    }
    
    /**
     * @notice Update strategy parameters
     * @param _strategy Strategy address
     * @param _debtRatio New debt ratio
     * @param _minDebtPerHarvest New minimum debt per harvest
     * @param _maxDebtPerHarvest New maximum debt per harvest
     */
    function updateStrategyParams(
        address _strategy,
        uint256 _debtRatio,
        uint256 _minDebtPerHarvest,
        uint256 _maxDebtPerHarvest
    ) external onlyRole(STRATEGIST_ROLE) {
        require(strategies[_strategy].activation > 0, "Strategy does not exist");
        require(_debtRatio <= MAX_BPS, "Invalid debt ratio");
        require(_minDebtPerHarvest <= _maxDebtPerHarvest, "Invalid debt limits");
        
        strategies[_strategy].debtRatio = _debtRatio;
        strategies[_strategy].minDebtPerHarvest = _minDebtPerHarvest;
        strategies[_strategy].maxDebtPerHarvest = _maxDebtPerHarvest;
        
        emit UpdatedStrategyParams(_strategy, _debtRatio, _minDebtPerHarvest, _maxDebtPerHarvest);
    }
    
    /**
     * @notice Report strategy performance and adjust debt
     * @param _strategy Strategy address
     * @return debt Amount of debt issued or repaid
     */
    function report(address _strategy) external returns (uint256 debt) {
        require(msg.sender == _strategy || hasRole(KEEPER_ROLE, msg.sender), "Unauthorized");
        StrategyParams storage strategy = strategies[_strategy];
        require(strategy.activation > 0, "Strategy not active");
        
        // Get strategy's total assets
        uint256 totalAssets = IStrategy(_strategy).estimatedTotalAssets();
        uint256 gain = 0;
        uint256 loss = 0;
        
        // Calculate gain/loss
        if (totalAssets > strategy.totalDebt) {
            gain = totalAssets - strategy.totalDebt;
        } else {
            loss = strategy.totalDebt - totalAssets;
        }
        
        // Update locked profit
        if (gain > 0) {
            lockedProfit = _calculateLockedProfit() + gain;
        }
        
        // Update strategy accounting
        strategy.totalGain += gain;
        strategy.totalLoss += loss;
        
        // Adjust debt based on strategy performance and vault needs
        debt = _debtOutstanding(_strategy);
        uint256 debtPayment = 0;
        
        if (emergencyShutdown) {
            // Recall all debt
            debt = strategy.totalDebt;
        } else {
            // Calculate target debt
            uint256 targetDebt = _totalAssets() * strategy.debtRatio / MAX_BPS;
            
            if (targetDebt > strategy.totalDebt) {
                // Issue more debt
                debt = Math.min(
                    targetDebt - strategy.totalDebt,
                    strategy.maxDebtPerHarvest
                );
            } else if (targetDebt < strategy.totalDebt) {
                // Reduce debt
                debtPayment = Math.min(
                    strategy.totalDebt - targetDebt,
                    strategy.maxDebtPerHarvest
                );
            }
        }
        
        // Update state
        strategy.lastReport = block.timestamp;
        lastReport = block.timestamp;
        
        // Handle debt changes
        if (debt > 0) {
            strategy.totalDebt += debt;
            totalDebt += debt;
            
            // Transfer tokens to strategy
            resourceToken.safeTransferFrom(address(this), _strategy, resourceTokenId, debt, "");
        } else if (debtPayment > 0) {
            strategy.totalDebt -= debtPayment;
            totalDebt -= debtPayment;
            
            // Withdraw from strategy
            uint256 withdrawn = IStrategy(_strategy).withdraw(debtPayment);
            if (withdrawn < debtPayment) {
                loss += debtPayment - withdrawn;
                strategy.totalLoss += debtPayment - withdrawn;
            }
        }
        
        // Take fees
        uint256 totalFees = _assessFees(_strategy, gain);
        
        emit StrategyReported(
            _strategy,
            gain,
            loss,
            debtPayment,
            strategy.totalGain,
            strategy.totalLoss,
            strategy.totalDebt,
            debt,
            strategy.debtRatio
        );
        
        return debt;
    }
    
    /**
     * @notice Harvest all strategies
     */
    function harvestAll() external onlyRole(KEEPER_ROLE) {
        for (uint256 i = 0; i < withdrawalQueue.length; i++) {
            if (strategies[withdrawalQueue[i]].activation > 0) {
                IStrategy(withdrawalQueue[i]).harvest();
            }
        }
    }
    
    /**
     * @notice Migrate a strategy to a new version
     * @param _oldStrategy Current strategy address
     * @param _newStrategy New strategy address
     */
    function migrateStrategy(
        address _oldStrategy,
        address _newStrategy
    ) external onlyRole(STRATEGIST_ROLE) {
        StrategyParams memory oldParams = strategies[_oldStrategy];
        require(oldParams.activation > 0, "Old strategy not active");
        require(strategies[_newStrategy].activation == 0, "New strategy already active");
        
        // Copy parameters
        strategies[_newStrategy] = StrategyParams({
            performanceFee: oldParams.performanceFee,
            activation: block.timestamp,
            debtRatio: oldParams.debtRatio,
            minDebtPerHarvest: oldParams.minDebtPerHarvest,
            maxDebtPerHarvest: oldParams.maxDebtPerHarvest,
            lastReport: block.timestamp,
            totalDebt: oldParams.totalDebt,
            totalGain: 0,
            totalLoss: 0
        });
        
        // Replace in withdrawal queue
        for (uint256 i = 0; i < withdrawalQueue.length; i++) {
            if (withdrawalQueue[i] == _oldStrategy) {
                withdrawalQueue[i] = _newStrategy;
                break;
            }
        }
        
        // Migrate funds
        IStrategy(_oldStrategy).migrate(_newStrategy);
        
        // Remove old strategy
        delete strategies[_oldStrategy];
        
        emit StrategyMigrated(_oldStrategy, _newStrategy);
    }
    
    /**
     * @notice Set emergency shutdown mode
     * @param _active Whether to activate emergency shutdown
     */
    function setEmergencyShutdown(bool _active) external onlyRole(GUARDIAN_ROLE) {
        emergencyShutdown = _active;
        emit EmergencyShutdown(_active);
    }
    
    /**
     * @notice Update management fee
     * @param _managementFee New management fee in basis points
     */
    function setManagementFee(uint256 _managementFee) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(_managementFee <= MAX_BPS, "Fee too high");
        managementFee = _managementFee;
        emit UpdatedManagementFee(_managementFee);
    }
    
    /**
     * @notice Update performance fee
     * @param _performanceFee New performance fee in basis points
     */
    function setPerformanceFee(uint256 _performanceFee) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(_performanceFee <= MAX_BPS / 2, "Fee too high"); // Max 50%
        performanceFee = _performanceFee;
        emit UpdatedPerformanceFee(_performanceFee);
    }
    
    /**
     * @notice Update rewards recipient
     * @param _rewards New rewards recipient
     */
    function setRewards(address _rewards) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(_rewards != address(0), "Invalid rewards");
        rewards = _rewards;
        emit UpdatedRewards(_rewards);
    }
    
    // View functions
    
    /**
     * @notice Get total assets under management
     * @return Total assets in vault and strategies
     */
    function totalAssets() external view returns (uint256) {
        return _totalAssets();
    }
    
    /**
     * @notice Get price per share
     * @return Price per vault share
     */
    function pricePerShare() external view returns (uint256) {
        return _shareValue(10 ** decimals());
    }
    
    /**
     * @notice Get available deposit limit
     * @return Maximum deposit amount
     */
    function availableDepositLimit() external view returns (uint256) {
        if (emergencyShutdown) return 0;
        
        // Could implement deposit limits here
        return type(uint256).max;
    }
    
    /**
     * @notice Get expected share amount for deposit
     * @param _amount Amount to deposit
     * @return Expected shares
     */
    function expectedSharesForAmount(uint256 _amount) external view returns (uint256) {
        uint256 totalSupply = totalSupply();
        if (totalSupply == 0) {
            return _amount;
        }
        return _amount * totalSupply / _totalAssets();
    }
    
    // Internal functions
    
    function _totalAssets() internal view returns (uint256) {
        return resourceToken.balanceOf(address(this), resourceTokenId) + totalDebt;
    }
    
    function _shareValue(uint256 _shares) internal view returns (uint256) {
        uint256 totalSupply = totalSupply();
        if (totalSupply == 0) {
            return _shares;
        }
        return _shares * _totalAssets() / totalSupply;
    }
    
    function _issueSharesForAmount(address _recipient, uint256 _amount) internal returns (uint256) {
        uint256 totalSupply = totalSupply();
        uint256 shares;
        
        if (totalSupply == 0) {
            shares = _amount;
        } else {
            shares = _amount * totalSupply / _totalAssets();
        }
        
        _mint(_recipient, shares);
        return shares;
    }
    
    function _withdrawFromStrategies(uint256 _amount) internal returns (uint256) {
        uint256 amountWithdrawn = 0;
        
        for (uint256 i = 0; i < withdrawalQueue.length && amountWithdrawn < _amount; i++) {
            address strategy = withdrawalQueue[i];
            StrategyParams storage params = strategies[strategy];
            
            if (params.activation == 0 || params.totalDebt == 0) {
                continue;
            }
            
            uint256 toWithdraw = Math.min(_amount - amountWithdrawn, params.totalDebt);
            uint256 withdrawn = IStrategy(strategy).withdraw(toWithdraw);
            
            params.totalDebt -= withdrawn;
            totalDebt -= withdrawn;
            amountWithdrawn += withdrawn;
        }
        
        return amountWithdrawn;
    }
    
    function _debtOutstanding(address _strategy) internal view returns (uint256) {
        StrategyParams memory params = strategies[_strategy];
        uint256 targetDebt = _totalAssets() * params.debtRatio / MAX_BPS;
        
        if (targetDebt > params.totalDebt) {
            return Math.min(targetDebt - params.totalDebt, params.maxDebtPerHarvest);
        } else {
            return 0;
        }
    }
    
    function _calculateLockedProfit() internal view returns (uint256) {
        uint256 lockedFundsRatio = (block.timestamp - lastReport) * lockedProfitDegradation;
        
        if (lockedFundsRatio < DEGRADATION_COEFFICIENT) {
            return lockedProfit - (lockedFundsRatio * lockedProfit / DEGRADATION_COEFFICIENT);
        } else {
            return 0;
        }
    }
    
    function _assessFees(address _strategy, uint256 _gain) internal returns (uint256) {
        StrategyParams storage params = strategies[_strategy];
        
        if (_gain == 0) {
            return 0;
        }
        
        uint256 totalFees = 0;
        
        // Performance fee for strategist
        if (params.performanceFee > 0) {
            uint256 strategistFee = _gain * params.performanceFee / MAX_BPS;
            totalFees += strategistFee;
            // Mint shares to strategist
            _issueSharesForAmount(_strategy, strategistFee);
        }
        
        // Performance fee for vault
        uint256 vaultFee = _gain * performanceFee / MAX_BPS;
        totalFees += vaultFee;
        
        // Management fee
        uint256 duration = block.timestamp - params.lastReport;
        uint256 managementFeeAmount = params.totalDebt * duration * managementFee / MAX_BPS / SECS_PER_YEAR;
        totalFees += managementFeeAmount;
        
        // Mint shares to rewards
        if (totalFees > 0) {
            _issueSharesForAmount(rewards, totalFees);
        }
        
        return totalFees;
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