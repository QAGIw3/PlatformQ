// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/utils/math/SafeMath.sol";

contract PlatformToken is ERC20 {
    constructor() ERC20("Platform Governance Token", "PGT") {
        _mint(msg.sender, 1000000000 * 10**18); // 1 billion tokens
    }
}

contract YieldFarming is ReentrancyGuard, Ownable {
    using SafeMath for uint256;
    
    IERC20 public platformToken;
    
    struct Pool {
        uint256 poolId;
        IERC20 stakingToken;
        uint256 rewardRate; // Rewards per second
        uint256 totalStaked;
        uint256 lastUpdateTime;
        uint256 rewardPerTokenStored;
        uint256 startTime;
        uint256 endTime;
        bool isActive;
        uint256 minStakeAmount;
        uint256 lockPeriod;
    }
    
    struct UserInfo {
        uint256 stakedAmount;
        uint256 rewardDebt;
        uint256 lastStakeTime;
        uint256 pendingRewards;
        uint256 lockedUntil;
    }
    
    mapping(uint256 => Pool) public pools;
    mapping(uint256 => mapping(address => UserInfo)) public userInfo;
    mapping(address => uint256[]) public userPools;
    
    uint256 public nextPoolId;
    uint256 public totalRewardsDistributed;
    uint256 public platformFeePercentage = 100; // 1%
    
    // Boost multipliers for different NFT tiers
    mapping(address => uint256) public nftBoostMultiplier;
    
    event PoolCreated(uint256 indexed poolId, address indexed stakingToken);
    event Staked(uint256 indexed poolId, address indexed user, uint256 amount);
    event Withdrawn(uint256 indexed poolId, address indexed user, uint256 amount);
    event RewardsClaimed(uint256 indexed poolId, address indexed user, uint256 reward);
    event PoolUpdated(uint256 indexed poolId, uint256 newRewardRate);
    event EmergencyWithdraw(uint256 indexed poolId, address indexed user, uint256 amount);
    
    constructor(address _platformToken) {
        platformToken = IERC20(_platformToken);
    }
    
    function createPool(
        address stakingToken,
        uint256 rewardRate,
        uint256 duration,
        uint256 minStakeAmount,
        uint256 lockPeriod
    ) external onlyOwner {
        require(stakingToken != address(0), "Invalid token");
        require(rewardRate > 0, "Invalid reward rate");
        require(duration > 0, "Invalid duration");
        
        uint256 poolId = nextPoolId++;
        
        pools[poolId] = Pool({
            poolId: poolId,
            stakingToken: IERC20(stakingToken),
            rewardRate: rewardRate,
            totalStaked: 0,
            lastUpdateTime: block.timestamp,
            rewardPerTokenStored: 0,
            startTime: block.timestamp,
            endTime: block.timestamp + duration,
            isActive: true,
            minStakeAmount: minStakeAmount,
            lockPeriod: lockPeriod
        });
        
        emit PoolCreated(poolId, stakingToken);
    }
    
    function stake(uint256 poolId, uint256 amount) external nonReentrant {
        Pool storage pool = pools[poolId];
        require(pool.isActive, "Pool not active");
        require(block.timestamp < pool.endTime, "Pool ended");
        require(amount >= pool.minStakeAmount, "Below minimum stake");
        
        updateReward(poolId, msg.sender);
        
        UserInfo storage user = userInfo[poolId][msg.sender];
        
        // Transfer tokens
        pool.stakingToken.transferFrom(msg.sender, address(this), amount);
        
        // Update user info
        user.stakedAmount = user.stakedAmount.add(amount);
        user.lastStakeTime = block.timestamp;
        user.lockedUntil = block.timestamp + pool.lockPeriod;
        
        // Update pool info
        pool.totalStaked = pool.totalStaked.add(amount);
        
        // Add to user pools if first stake
        if (user.stakedAmount == amount) {
            userPools[msg.sender].push(poolId);
        }
        
        emit Staked(poolId, msg.sender, amount);
    }
    
    function withdraw(uint256 poolId, uint256 amount) external nonReentrant {
        UserInfo storage user = userInfo[poolId][msg.sender];
        require(user.stakedAmount >= amount, "Insufficient balance");
        require(block.timestamp >= user.lockedUntil, "Still locked");
        
        updateReward(poolId, msg.sender);
        
        Pool storage pool = pools[poolId];
        
        // Update balances
        user.stakedAmount = user.stakedAmount.sub(amount);
        pool.totalStaked = pool.totalStaked.sub(amount);
        
        // Transfer tokens back
        pool.stakingToken.transfer(msg.sender, amount);
        
        emit Withdrawn(poolId, msg.sender, amount);
    }
    
    function claimRewards(uint256 poolId) external nonReentrant {
        updateReward(poolId, msg.sender);
        
        UserInfo storage user = userInfo[poolId][msg.sender];
        uint256 reward = user.pendingRewards;
        
        if (reward > 0) {
            user.pendingRewards = 0;
            
            // Apply platform fee
            uint256 platformFee = reward.mul(platformFeePercentage).div(10000);
            uint256 userReward = reward.sub(platformFee);
            
            // Apply NFT boost if applicable
            uint256 boost = getNFTBoost(msg.sender);
            if (boost > 0) {
                userReward = userReward.mul(10000 + boost).div(10000);
            }
            
            // Transfer rewards
            platformToken.transfer(msg.sender, userReward);
            if (platformFee > 0) {
                platformToken.transfer(owner(), platformFee);
            }
            
            totalRewardsDistributed = totalRewardsDistributed.add(reward);
            
            emit RewardsClaimed(poolId, msg.sender, userReward);
        }
    }
    
    function claimAllRewards() external nonReentrant {
        uint256[] memory pools = userPools[msg.sender];
        uint256 totalReward = 0;
        
        for (uint i = 0; i < pools.length; i++) {
            updateReward(pools[i], msg.sender);
            
            UserInfo storage user = userInfo[pools[i]][msg.sender];
            if (user.pendingRewards > 0) {
                totalReward = totalReward.add(user.pendingRewards);
                user.pendingRewards = 0;
            }
        }
        
        if (totalReward > 0) {
            // Apply platform fee
            uint256 platformFee = totalReward.mul(platformFeePercentage).div(10000);
            uint256 userReward = totalReward.sub(platformFee);
            
            // Apply NFT boost
            uint256 boost = getNFTBoost(msg.sender);
            if (boost > 0) {
                userReward = userReward.mul(10000 + boost).div(10000);
            }
            
            // Transfer rewards
            platformToken.transfer(msg.sender, userReward);
            if (platformFee > 0) {
                platformToken.transfer(owner(), platformFee);
            }
            
            totalRewardsDistributed = totalRewardsDistributed.add(totalReward);
        }
    }
    
    function compound(uint256 poolId) external nonReentrant {
        require(address(pools[poolId].stakingToken) == address(platformToken), "Can only compound platform token");
        
        updateReward(poolId, msg.sender);
        
        UserInfo storage user = userInfo[poolId][msg.sender];
        uint256 reward = user.pendingRewards;
        
        if (reward > 0) {
            user.pendingRewards = 0;
            
            // Apply platform fee
            uint256 platformFee = reward.mul(platformFeePercentage).div(10000);
            uint256 compoundAmount = reward.sub(platformFee);
            
            // Apply NFT boost
            uint256 boost = getNFTBoost(msg.sender);
            if (boost > 0) {
                compoundAmount = compoundAmount.mul(10000 + boost).div(10000);
            }
            
            // Compound by staking rewards
            user.stakedAmount = user.stakedAmount.add(compoundAmount);
            pools[poolId].totalStaked = pools[poolId].totalStaked.add(compoundAmount);
            
            if (platformFee > 0) {
                platformToken.transfer(owner(), platformFee);
            }
            
            emit Staked(poolId, msg.sender, compoundAmount);
        }
    }
    
    function updateReward(uint256 poolId, address account) internal {
        Pool storage pool = pools[poolId];
        
        // Update pool reward per token
        pool.rewardPerTokenStored = rewardPerToken(poolId);
        pool.lastUpdateTime = lastTimeRewardApplicable(poolId);
        
        if (account != address(0)) {
            UserInfo storage user = userInfo[poolId][account];
            user.pendingRewards = earned(poolId, account);
            user.rewardDebt = pool.rewardPerTokenStored;
        }
    }
    
    function rewardPerToken(uint256 poolId) public view returns (uint256) {
        Pool memory pool = pools[poolId];
        
        if (pool.totalStaked == 0) {
            return pool.rewardPerTokenStored;
        }
        
        return pool.rewardPerTokenStored.add(
            lastTimeRewardApplicable(poolId)
                .sub(pool.lastUpdateTime)
                .mul(pool.rewardRate)
                .mul(1e18)
                .div(pool.totalStaked)
        );
    }
    
    function lastTimeRewardApplicable(uint256 poolId) public view returns (uint256) {
        return block.timestamp < pools[poolId].endTime ? block.timestamp : pools[poolId].endTime;
    }
    
    function earned(uint256 poolId, address account) public view returns (uint256) {
        UserInfo memory user = userInfo[poolId][account];
        
        return user.stakedAmount
            .mul(rewardPerToken(poolId).sub(user.rewardDebt))
            .div(1e18)
            .add(user.pendingRewards);
    }
    
    function getNFTBoost(address user) public view returns (uint256) {
        // Check if user holds platform NFTs and return boost percentage
        // This would integrate with the NFT contracts
        return nftBoostMultiplier[user];
    }
    
    function setNFTBoost(address nftContract, uint256 boostPercentage) external onlyOwner {
        require(boostPercentage <= 10000, "Boost too high"); // Max 100% boost
        nftBoostMultiplier[nftContract] = boostPercentage;
    }
    
    function updatePoolRewardRate(uint256 poolId, uint256 newRewardRate) external onlyOwner {
        updateReward(poolId, address(0));
        pools[poolId].rewardRate = newRewardRate;
        emit PoolUpdated(poolId, newRewardRate);
    }
    
    function extendPool(uint256 poolId, uint256 additionalDuration) external onlyOwner {
        pools[poolId].endTime = pools[poolId].endTime.add(additionalDuration);
    }
    
    function emergencyWithdraw(uint256 poolId) external nonReentrant {
        UserInfo storage user = userInfo[poolId][msg.sender];
        uint256 amount = user.stakedAmount;
        
        require(amount > 0, "Nothing to withdraw");
        
        // Reset user data
        user.stakedAmount = 0;
        user.pendingRewards = 0;
        user.rewardDebt = 0;
        
        // Update pool
        pools[poolId].totalStaked = pools[poolId].totalStaked.sub(amount);
        
        // Transfer tokens (no rewards in emergency)
        pools[poolId].stakingToken.transfer(msg.sender, amount);
        
        emit EmergencyWithdraw(poolId, msg.sender, amount);
    }
    
    function getPoolInfo(uint256 poolId) external view returns (
        address stakingToken,
        uint256 totalStaked,
        uint256 rewardRate,
        uint256 endTime,
        bool isActive
    ) {
        Pool memory pool = pools[poolId];
        return (
            address(pool.stakingToken),
            pool.totalStaked,
            pool.rewardRate,
            pool.endTime,
            pool.isActive
        );
    }
    
    function getUserInfo(uint256 poolId, address user) external view returns (
        uint256 stakedAmount,
        uint256 pendingRewards,
        uint256 lockedUntil
    ) {
        UserInfo memory info = userInfo[poolId][user];
        return (
            info.stakedAmount,
            earned(poolId, user),
            info.lockedUntil
        );
    }
    
    function getUserPools(address user) external view returns (uint256[] memory) {
        return userPools[user];
    }
} 