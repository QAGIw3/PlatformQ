// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC1155/IERC1155.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "../InfrastructureVault.sol";
import "../InfrastructureLending.sol";
import "../ResourceStaking.sol";

/**
 * @title LendingOptimizerVault
 * @notice Strategy that optimizes yields across lending pools and staking
 * @dev Automatically moves resources between lending and staking for best APY
 */
contract LendingOptimizerVault is Ownable, ReentrancyGuard {
    
    // Constants
    uint256 public constant MAX_BPS = 10000;
    uint256 public constant REBALANCE_THRESHOLD = 100; // 1% APY difference triggers rebalance
    
    // Immutable addresses
    address public immutable vault;
    address public immutable want;
    uint256 public immutable wantTokenId;
    InfrastructureLending public immutable lendingContract;
    ResourceStaking public immutable stakingContract;
    
    // Strategy state
    struct AllocationTarget {
        uint256 lendingRatio;    // Basis points allocated to lending
        uint256 stakingPoolId;   // Staking pool ID if staking
        uint256 minAPY;          // Minimum acceptable APY
        bool autoRebalance;      // Whether to auto-rebalance
    }
    
    AllocationTarget public allocationTarget;
    
    // Current allocations
    uint256 public lendingBalance;
    uint256 public stakingBalance;
    uint256[] public activeStakeIds;
    
    // Performance tracking
    uint256 public lastRebalance;
    uint256 public totalRebalances;
    uint256 public performanceFee = 1000; // 10%
    address public strategist;
    
    // Events
    event Rebalanced(uint256 toLending, uint256 toStaking, uint256 apy);
    event AllocationUpdated(uint256 lendingRatio, uint256 stakingPoolId);
    event Harvested(uint256 profit, uint256 loss, uint256 debtPayment);
    
    modifier onlyVault() {
        require(msg.sender == vault, "Only vault");
        _;
    }
    
    modifier onlyAuthorized() {
        require(msg.sender == vault || msg.sender == strategist || msg.sender == owner(), "Not authorized");
        _;
    }
    
    /**
     * @dev Constructor
     */
    constructor(
        address _vault,
        address _want,
        uint256 _wantTokenId,
        address _lendingContract,
        address _stakingContract
    ) {
        vault = _vault;
        want = _want;
        wantTokenId = _wantTokenId;
        lendingContract = InfrastructureLending(_lendingContract);
        stakingContract = ResourceStaking(_stakingContract);
        strategist = msg.sender;
        
        // Default allocation: 50/50 lending/staking
        allocationTarget = AllocationTarget({
            lendingRatio: 5000,
            stakingPoolId: 1,
            minAPY: 500, // 5% minimum
            autoRebalance: true
        });
    }
    
    /**
     * @notice Get strategy name
     */
    function name() external pure returns (string memory) {
        return "LendingOptimizerVault";
    }
    
    /**
     * @notice Get total assets under management
     */
    function estimatedTotalAssets() external view returns (uint256) {
        return _totalAssets();
    }
    
    /**
     * @notice Update allocation targets
     */
    function updateAllocation(
        uint256 _lendingRatio,
        uint256 _stakingPoolId,
        uint256 _minAPY,
        bool _autoRebalance
    ) external onlyAuthorized {
        require(_lendingRatio <= MAX_BPS, "Invalid ratio");
        
        allocationTarget = AllocationTarget({
            lendingRatio: _lendingRatio,
            stakingPoolId: _stakingPoolId,
            minAPY: _minAPY,
            autoRebalance: _autoRebalance
        });
        
        emit AllocationUpdated(_lendingRatio, _stakingPoolId);
        
        // Trigger rebalance
        _rebalance();
    }
    
    /**
     * @notice Harvest yields and rebalance
     */
    function harvest() external onlyAuthorized {
        // Claim lending rewards
        _claimLendingRewards();
        
        // Claim staking rewards
        _claimStakingRewards();
        
        // Rebalance if needed
        if (allocationTarget.autoRebalance) {
            uint256 currentAPY = _calculateCurrentAPY();
            uint256 optimalAPY = _calculateOptimalAPY();
            
            if (_shouldRebalance(currentAPY, optimalAPY)) {
                _rebalance();
            }
        }
        
        // Calculate profit/loss
        uint256 totalAssets = _totalAssets();
        uint256 debt = InfrastructureVault(vault).strategies(address(this)).totalDebt;
        
        uint256 profit = 0;
        uint256 loss = 0;
        
        if (totalAssets > debt) {
            profit = totalAssets - debt;
            
            // Take performance fee
            if (performanceFee > 0) {
                uint256 feeAmount = profit * performanceFee / MAX_BPS;
                IERC1155(want).safeTransferFrom(
                    address(this),
                    strategist,
                    wantTokenId,
                    feeAmount,
                    ""
                );
                profit -= feeAmount;
            }
        } else if (totalAssets < debt) {
            loss = debt - totalAssets;
        }
        
        // Report to vault
        uint256 debtPayment = InfrastructureVault(vault).report(address(this));
        
        emit Harvested(profit, loss, debtPayment);
    }
    
    /**
     * @notice Withdraw requested amount
     */
    function withdraw(uint256 _amount) external onlyVault returns (uint256) {
        uint256 liquidBalance = IERC1155(want).balanceOf(address(this), wantTokenId);
        
        if (liquidBalance >= _amount) {
            IERC1155(want).safeTransferFrom(address(this), vault, wantTokenId, _amount, "");
            return _amount;
        }
        
        // Need to withdraw from positions
        uint256 needed = _amount - liquidBalance;
        
        // Withdraw from lending first (usually more liquid)
        if (lendingBalance > 0) {
            uint256 toWithdraw = needed > lendingBalance ? lendingBalance : needed;
            _withdrawFromLending(toWithdraw);
            needed -= toWithdraw;
        }
        
        // Withdraw from staking if still needed
        if (needed > 0 && stakingBalance > 0) {
            uint256 toWithdraw = needed > stakingBalance ? stakingBalance : needed;
            _withdrawFromStaking(toWithdraw);
        }
        
        // Transfer what we have
        uint256 finalBalance = IERC1155(want).balanceOf(address(this), wantTokenId);
        uint256 withdrawn = finalBalance > _amount ? _amount : finalBalance;
        
        IERC1155(want).safeTransferFrom(address(this), vault, wantTokenId, withdrawn, "");
        return withdrawn;
    }
    
    /**
     * @notice Migrate to new strategy
     */
    function migrate(address _newStrategy) external onlyVault {
        // Withdraw all positions
        if (lendingBalance > 0) {
            _withdrawFromLending(lendingBalance);
        }
        
        if (stakingBalance > 0) {
            _withdrawFromStaking(stakingBalance);
        }
        
        // Transfer all assets
        uint256 balance = IERC1155(want).balanceOf(address(this), wantTokenId);
        if (balance > 0) {
            IERC1155(want).safeTransferFrom(address(this), _newStrategy, wantTokenId, balance, "");
        }
    }
    
    // Internal functions
    
    function _totalAssets() internal view returns (uint256) {
        uint256 liquid = IERC1155(want).balanceOf(address(this), wantTokenId);
        
        // Add lending balance with accrued interest
        uint256 lendingValue = lendingBalance;
        if (lendingBalance > 0) {
            // Estimate accrued interest
            uint256 supplyAPY = lendingContract.getSupplyAPY(wantTokenId);
            uint256 timeDelta = block.timestamp - lastRebalance;
            uint256 interest = lendingBalance * supplyAPY * timeDelta / (365 days * MAX_BPS);
            lendingValue += interest;
        }
        
        // Add staking balance with pending rewards
        uint256 stakingValue = stakingBalance;
        for (uint256 i = 0; i < activeStakeIds.length; i++) {
            uint256 pendingReward = stakingContract.pendingReward(activeStakeIds[i]);
            stakingValue += pendingReward;
        }
        
        return liquid + lendingValue + stakingValue;
    }
    
    function _rebalance() internal {
        uint256 totalAssets = _totalAssets();
        
        // Calculate target allocations
        uint256 targetLending = totalAssets * allocationTarget.lendingRatio / MAX_BPS;
        uint256 targetStaking = totalAssets - targetLending;
        
        // Rebalance lending
        if (targetLending > lendingBalance) {
            uint256 toDeposit = targetLending - lendingBalance;
            _depositToLending(toDeposit);
        } else if (targetLending < lendingBalance) {
            uint256 toWithdraw = lendingBalance - targetLending;
            _withdrawFromLending(toWithdraw);
        }
        
        // Rebalance staking
        if (targetStaking > stakingBalance) {
            uint256 toStake = targetStaking - stakingBalance;
            _depositToStaking(toStake);
        } else if (targetStaking < stakingBalance) {
            uint256 toWithdraw = stakingBalance - targetStaking;
            _withdrawFromStaking(toWithdraw);
        }
        
        lastRebalance = block.timestamp;
        totalRebalances++;
        
        emit Rebalanced(targetLending, targetStaking, _calculateCurrentAPY());
    }
    
    function _depositToLending(uint256 amount) internal {
        uint256 balance = IERC1155(want).balanceOf(address(this), wantTokenId);
        if (balance < amount) {
            amount = balance;
        }
        
        if (amount > 0) {
            IERC1155(want).setApprovalForAll(address(lendingContract), true);
            lendingContract.supply(wantTokenId, amount);
            lendingBalance += amount;
        }
    }
    
    function _withdrawFromLending(uint256 amount) internal {
        if (amount > lendingBalance) {
            amount = lendingBalance;
        }
        
        if (amount > 0) {
            lendingContract.withdraw(wantTokenId, amount);
            lendingBalance -= amount;
        }
    }
    
    function _depositToStaking(uint256 amount) internal {
        uint256 balance = IERC1155(want).balanceOf(address(this), wantTokenId);
        if (balance < amount) {
            amount = balance;
        }
        
        if (amount > 0) {
            IERC1155(want).setApprovalForAll(address(stakingContract), true);
            uint256 stakeId = stakingContract.stake(
                allocationTarget.stakingPoolId,
                amount,
                30 days // Default lock period
            );
            activeStakeIds.push(stakeId);
            stakingBalance += amount;
        }
    }
    
    function _withdrawFromStaking(uint256 amount) internal {
        uint256 withdrawn = 0;
        
        // Withdraw from unlocked stakes first
        for (uint256 i = 0; i < activeStakeIds.length && withdrawn < amount; i++) {
            uint256 stakeId = activeStakeIds[i];
            (uint256 stakeAmount, , uint256 lockEndTime, , , , ) = stakingContract.userStakes(
                address(this),
                stakeId
            );
            
            if (block.timestamp >= lockEndTime && stakeAmount > 0) {
                stakingContract.withdraw(stakeId);
                withdrawn += stakeAmount;
                
                // Remove from active stakes
                activeStakeIds[i] = activeStakeIds[activeStakeIds.length - 1];
                activeStakeIds.pop();
                i--;
            }
        }
        
        stakingBalance -= withdrawn;
    }
    
    function _claimLendingRewards() internal {
        // Claim lending rewards if available
        // Implementation depends on lending contract interface
    }
    
    function _claimStakingRewards() internal {
        for (uint256 i = 0; i < activeStakeIds.length; i++) {
            stakingContract.claimReward(activeStakeIds[i]);
        }
    }
    
    function _calculateCurrentAPY() internal view returns (uint256) {
        uint256 totalAssets = _totalAssets();
        if (totalAssets == 0) return 0;
        
        uint256 lendingAPY = lendingContract.getSupplyAPY(wantTokenId);
        uint256 stakingAPY = _getStakingAPY();
        
        uint256 weightedAPY = (lendingBalance * lendingAPY + stakingBalance * stakingAPY) / totalAssets;
        return weightedAPY;
    }
    
    function _calculateOptimalAPY() internal view returns (uint256) {
        uint256 lendingAPY = lendingContract.getSupplyAPY(wantTokenId);
        uint256 stakingAPY = _getStakingAPY();
        
        return lendingAPY > stakingAPY ? lendingAPY : stakingAPY;
    }
    
    function _getStakingAPY() internal view returns (uint256) {
        // Get staking pool APY
        // This would query the staking contract for current rewards rate
        // For now, return a mock value
        return 800; // 8% APY
    }
    
    function _shouldRebalance(uint256 currentAPY, uint256 optimalAPY) internal pure returns (bool) {
        if (optimalAPY <= currentAPY) return false;
        
        uint256 difference = optimalAPY - currentAPY;
        return difference >= REBALANCE_THRESHOLD;
    }
    
    // Admin functions
    
    function setStrategist(address _strategist) external onlyAuthorized {
        strategist = _strategist;
    }
    
    function setPerformanceFee(uint256 _fee) external onlyAuthorized {
        require(_fee <= 5000, "Fee too high");
        performanceFee = _fee;
    }
    
    function emergencyWithdraw() external onlyAuthorized {
        // Withdraw all positions
        if (lendingBalance > 0) {
            _withdrawFromLending(lendingBalance);
        }
        
        if (stakingBalance > 0) {
            _withdrawFromStaking(stakingBalance);
        }
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