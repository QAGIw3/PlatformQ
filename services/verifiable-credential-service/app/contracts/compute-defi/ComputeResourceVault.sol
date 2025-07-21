// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import "@openzeppelin/contracts/token/ERC1155/IERC1155.sol";
import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/security/Pausable.sol";

/**
 * @title ComputeResourceVault
 * @notice Vault for compute resources with automated yield strategies
 */
contract ComputeResourceVault is ERC20, AccessControl, ReentrancyGuard, Pausable {
    bytes32 public constant MANAGER_ROLE = keccak256("MANAGER_ROLE");
    bytes32 public constant STRATEGIST_ROLE = keccak256("STRATEGIST_ROLE");
    
    // Resource token (ERC-1155)
    IERC1155 public immutable resourceToken;
    uint256 public immutable resourceTokenId;
    
    // Fee structure
    uint256 public managementFee = 200; // 2% annual
    uint256 public performanceFee = 1500; // 15% of profits
    uint256 public constant MAX_FEE = 5000; // 50% max
    uint256 public constant FEE_DENOMINATOR = 10000;
    
    // Vault state
    uint256 public totalAssets;
    uint256 public lastHarvest;
    uint256 public minDeposit = 100;
    
    // Strategy configuration
    address[] public strategies;
    mapping(address => bool) public approvedStrategies;
    mapping(address => uint256) public strategyAllocations;
    
    // User tracking
    mapping(address => uint256) public userShares;
    mapping(address => uint256) public lastDepositTime;
    mapping(address => uint256) public lockExpiry;
    
    // Events
    event Deposit(address indexed user, uint256 assets, uint256 shares);
    event Withdraw(address indexed user, uint256 assets, uint256 shares);
    event StrategyAdded(address indexed strategy);
    event StrategyRemoved(address indexed strategy);
    event Harvest(uint256 profit, uint256 performanceFees);
    event FeesUpdated(uint256 managementFee, uint256 performanceFee);
    
    constructor(
        string memory _name,
        string memory _symbol,
        address _resourceToken,
        uint256 _resourceTokenId
    ) ERC20(_name, _symbol) {
        resourceToken = IERC1155(_resourceToken);
        resourceTokenId = _resourceTokenId;
        
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(MANAGER_ROLE, msg.sender);
        _grantRole(STRATEGIST_ROLE, msg.sender);
        
        lastHarvest = block.timestamp;
    }
    
    /**
     * @notice Deposit compute resources into vault
     * @param resourceIds Array of resource token IDs
     * @param amounts Array of amounts to deposit
     * @param lockDuration Lock duration in seconds (0 for no lock)
     * @return shares Amount of vault shares minted
     */
    function deposit(
        uint256[] memory resourceIds,
        uint256[] memory amounts,
        uint256 lockDuration
    ) external nonReentrant whenNotPaused returns (uint256 shares) {
        require(resourceIds.length == amounts.length, "Array mismatch");
        require(resourceIds.length > 0, "Empty deposit");
        
        uint256 totalDeposit = 0;
        for (uint256 i = 0; i < resourceIds.length; i++) {
            require(resourceIds[i] == resourceTokenId, "Invalid resource");
            totalDeposit += amounts[i];
        }
        
        require(totalDeposit >= minDeposit, "Below minimum");
        
        // Transfer resources from user
        resourceToken.safeBatchTransferFrom(
            msg.sender,
            address(this),
            resourceIds,
            amounts,
            ""
        );
        
        // Calculate shares
        shares = _calculateShares(totalDeposit);
        
        // Apply lock bonus if applicable
        if (lockDuration > 0) {
            uint256 lockBonus = _calculateLockBonus(lockDuration);
            shares = shares * (FEE_DENOMINATOR + lockBonus) / FEE_DENOMINATOR;
            lockExpiry[msg.sender] = block.timestamp + lockDuration;
        }
        
        // Mint shares
        _mint(msg.sender, shares);
        userShares[msg.sender] += shares;
        lastDepositTime[msg.sender] = block.timestamp;
        totalAssets += totalDeposit;
        
        emit Deposit(msg.sender, totalDeposit, shares);
    }
    
    /**
     * @notice Withdraw compute resources from vault
     * @param shares Amount of shares to burn
     * @return assets Amount of resources withdrawn
     */
    function withdraw(
        uint256 shares
    ) external nonReentrant returns (uint256 assets) {
        require(shares > 0, "Zero shares");
        require(balanceOf(msg.sender) >= shares, "Insufficient shares");
        require(block.timestamp >= lockExpiry[msg.sender], "Still locked");
        
        // Calculate assets to withdraw
        assets = _calculateAssets(shares);
        
        // Burn shares
        _burn(msg.sender, shares);
        userShares[msg.sender] -= shares;
        totalAssets -= assets;
        
        // Transfer resources to user
        uint256[] memory ids = new uint256[](1);
        uint256[] memory amounts = new uint256[](1);
        ids[0] = resourceTokenId;
        amounts[0] = assets;
        
        resourceToken.safeBatchTransferFrom(
            address(this),
            msg.sender,
            ids,
            amounts,
            ""
        );
        
        emit Withdraw(msg.sender, assets, shares);
    }
    
    /**
     * @notice Harvest profits from strategies
     */
    function harvest() external onlyRole(STRATEGIST_ROLE) {
        uint256 currentAssets = _getTotalAssets();
        
        if (currentAssets > totalAssets) {
            uint256 profit = currentAssets - totalAssets;
            
            // Calculate performance fee
            uint256 perfFee = profit * performanceFee / FEE_DENOMINATOR;
            
            // Mint fee shares to treasury
            uint256 feeShares = _calculateShares(perfFee);
            _mint(getRoleMember(DEFAULT_ADMIN_ROLE, 0), feeShares);
            
            // Update total assets
            totalAssets = currentAssets - perfFee;
            
            emit Harvest(profit, perfFee);
        }
        
        lastHarvest = block.timestamp;
    }
    
    /**
     * @notice Add approved strategy
     * @param strategy Strategy address
     * @param allocation Initial allocation (basis points)
     */
    function addStrategy(
        address strategy,
        uint256 allocation
    ) external onlyRole(MANAGER_ROLE) {
        require(strategy != address(0), "Invalid strategy");
        require(!approvedStrategies[strategy], "Already approved");
        require(allocation <= FEE_DENOMINATOR, "Invalid allocation");
        
        strategies.push(strategy);
        approvedStrategies[strategy] = true;
        strategyAllocations[strategy] = allocation;
        
        emit StrategyAdded(strategy);
    }
    
    /**
     * @notice Remove strategy
     * @param strategy Strategy address
     */
    function removeStrategy(
        address strategy
    ) external onlyRole(MANAGER_ROLE) {
        require(approvedStrategies[strategy], "Not approved");
        
        approvedStrategies[strategy] = false;
        strategyAllocations[strategy] = 0;
        
        // Remove from array
        for (uint256 i = 0; i < strategies.length; i++) {
            if (strategies[i] == strategy) {
                strategies[i] = strategies[strategies.length - 1];
                strategies.pop();
                break;
            }
        }
        
        emit StrategyRemoved(strategy);
    }
    
    /**
     * @notice Update fees
     * @param _managementFee New management fee
     * @param _performanceFee New performance fee
     */
    function updateFees(
        uint256 _managementFee,
        uint256 _performanceFee
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(_managementFee <= MAX_FEE, "Management fee too high");
        require(_performanceFee <= MAX_FEE, "Performance fee too high");
        
        managementFee = _managementFee;
        performanceFee = _performanceFee;
        
        emit FeesUpdated(_managementFee, _performanceFee);
    }
    
    /**
     * @notice Get vault share price
     * @return price Share price in resource units
     */
    function sharePrice() external view returns (uint256 price) {
        uint256 supply = totalSupply();
        if (supply == 0) {
            return 1e18; // Initial price
        }
        return _getTotalAssets() * 1e18 / supply;
    }
    
    /**
     * @notice Pause vault operations
     */
    function pause() external onlyRole(MANAGER_ROLE) {
        _pause();
    }
    
    /**
     * @notice Unpause vault operations
     */
    function unpause() external onlyRole(MANAGER_ROLE) {
        _unpause();
    }
    
    // Internal functions
    
    function _calculateShares(uint256 assets) internal view returns (uint256) {
        uint256 supply = totalSupply();
        if (supply == 0) {
            return assets;
        }
        return assets * supply / _getTotalAssets();
    }
    
    function _calculateAssets(uint256 shares) internal view returns (uint256) {
        uint256 supply = totalSupply();
        if (supply == 0) {
            return shares;
        }
        return shares * _getTotalAssets() / supply;
    }
    
    function _getTotalAssets() internal view returns (uint256) {
        // In production, would aggregate from strategies
        return totalAssets;
    }
    
    function _calculateLockBonus(uint256 duration) internal pure returns (uint256) {
        // Max 50% bonus for 1 year lock
        uint256 maxDuration = 365 days;
        if (duration > maxDuration) {
            duration = maxDuration;
        }
        return duration * 5000 / maxDuration; // Linear scaling to 50%
    }
    
    // Required for ERC-1155 receiver
    function onERC1155Received(
        address,
        address,
        uint256,
        uint256,
        bytes calldata
    ) external pure returns (bytes4) {
        return this.onERC1155Received.selector;
    }
    
    function onERC1155BatchReceived(
        address,
        address,
        uint256[] calldata,
        uint256[] calldata,
        bytes calldata
    ) external pure returns (bytes4) {
        return this.onERC1155BatchReceived.selector;
    }
} 