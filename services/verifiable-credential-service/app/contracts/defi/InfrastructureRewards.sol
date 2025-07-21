// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/utils/math/SafeMath.sol";
import "./ResourceAMM.sol";

/**
 * @title InfrastructureRewards
 * @notice Liquidity mining and provider incentive rewards for Infrastructure DeFi
 * @dev Distributes rewards to liquidity providers and resource providers
 */
contract InfrastructureRewards is ReentrancyGuard, Ownable {
    using SafeMath for uint256;
    
    // Reward token (platform governance token)
    IERC20 public immutable rewardToken;
    ResourceAMM public immutable resourceAMM;
    
    // Reward pools
    struct RewardPool {
        uint256 poolId;           // AMM pool ID
        uint256 rewardRate;       // Rewards per second
        uint256 totalStaked;      // Total LP tokens staked
        uint256 lastUpdateTime;   // Last reward calculation
        uint256 rewardPerTokenStored; // Accumulated rewards per token
        uint256 periodFinish;     // When rewards end
        bool isActive;
    }
    
    struct UserInfo {
        uint256 stakedAmount;
        uint256 rewardDebt;
        uint256 pendingRewards;
    }
    
    struct ProviderRewards {
        uint256 totalProvided;    // Total resources provided
        uint256 qualityScore;     // Average quality score (0-1000)
        uint256 uptimeScore;      // Uptime percentage (0-1000)
        uint256 rewardMultiplier; // Bonus multiplier
        uint256 lastClaimTime;
        uint256 accumulatedRewards;
    }
    
    // State variables
    mapping(uint256 => RewardPool) public rewardPools;
    mapping(uint256 => mapping(address => UserInfo)) public userInfo;
    mapping(address => ProviderRewards) public providerRewards;
    
    uint256 public constant REWARD_DURATION = 7 days;
    uint256 public constant PROVIDER_REWARD_RATE = 100 ether; // 100 tokens per day base rate
    
    // Events
    event RewardPoolCreated(uint256 indexed poolId, uint256 rewardRate);
    event Staked(uint256 indexed poolId, address indexed user, uint256 amount);
    event Withdrawn(uint256 indexed poolId, address indexed user, uint256 amount);
    event RewardPaid(address indexed user, uint256 reward);
    event ProviderRewardClaimed(address indexed provider, uint256 reward);
    event RewardRateUpdated(uint256 indexed poolId, uint256 newRate);
    
    /**
     * @dev Constructor
     * @param _rewardToken Address of the reward token
     * @param _resourceAMM Address of ResourceAMM contract
     */
    constructor(address _rewardToken, address _resourceAMM) {
        require(_rewardToken != address(0), "Invalid reward token");
        require(_resourceAMM != address(0), "Invalid AMM");
        
        rewardToken = IERC20(_rewardToken);
        resourceAMM = ResourceAMM(_resourceAMM);
    }
    
    /**
     * @notice Create or update a reward pool for AMM LP tokens
     * @param poolId AMM pool ID
     * @param rewardAmount Total rewards to distribute
     */
    function createRewardPool(
        uint256 poolId,
        uint256 rewardAmount
    ) external onlyOwner {
        require(rewardAmount > 0, "Invalid reward amount");
        
        // Transfer rewards from owner
        rewardToken.transferFrom(msg.sender, address(this), rewardAmount);
        
        RewardPool storage pool = rewardPools[poolId];
        
        if (pool.isActive && block.timestamp < pool.periodFinish) {
            // Update existing pool
            uint256 remaining = pool.periodFinish.sub(block.timestamp);
            uint256 leftover = remaining.mul(pool.rewardRate);
            pool.rewardRate = rewardAmount.add(leftover).div(REWARD_DURATION);
        } else {
            // New pool
            pool.rewardRate = rewardAmount.div(REWARD_DURATION);
            pool.isActive = true;
        }
        
        pool.poolId = poolId;
        pool.lastUpdateTime = block.timestamp;
        pool.periodFinish = block.timestamp.add(REWARD_DURATION);
        
        emit RewardPoolCreated(poolId, pool.rewardRate);
    }
    
    /**
     * @notice Stake LP tokens to earn rewards
     * @param poolId AMM pool ID
     * @param amount Amount of LP tokens to stake
     */
    function stake(uint256 poolId, uint256 amount) external nonReentrant {
        require(amount > 0, "Cannot stake 0");
        
        RewardPool storage pool = rewardPools[poolId];
        require(pool.isActive, "Pool not active");
        
        updateReward(poolId, msg.sender);
        
        // Get LP token address
        address lpToken = address(resourceAMM.lpTokens(poolId));
        require(lpToken != address(0), "Invalid pool");
        
        // Transfer LP tokens from user
        IERC20(lpToken).transferFrom(msg.sender, address(this), amount);
        
        // Update user info
        userInfo[poolId][msg.sender].stakedAmount = 
            userInfo[poolId][msg.sender].stakedAmount.add(amount);
        pool.totalStaked = pool.totalStaked.add(amount);
        
        emit Staked(poolId, msg.sender, amount);
    }
    
    /**
     * @notice Withdraw LP tokens and claim rewards
     * @param poolId AMM pool ID
     * @param amount Amount to withdraw
     */
    function withdraw(uint256 poolId, uint256 amount) external nonReentrant {
        require(amount > 0, "Cannot withdraw 0");
        
        UserInfo storage user = userInfo[poolId][msg.sender];
        require(user.stakedAmount >= amount, "Insufficient balance");
        
        updateReward(poolId, msg.sender);
        
        // Update balances
        user.stakedAmount = user.stakedAmount.sub(amount);
        rewardPools[poolId].totalStaked = rewardPools[poolId].totalStaked.sub(amount);
        
        // Transfer LP tokens back
        address lpToken = address(resourceAMM.lpTokens(poolId));
        IERC20(lpToken).transfer(msg.sender, amount);
        
        // Claim pending rewards
        uint256 reward = user.pendingRewards;
        if (reward > 0) {
            user.pendingRewards = 0;
            rewardToken.transfer(msg.sender, reward);
            emit RewardPaid(msg.sender, reward);
        }
        
        emit Withdrawn(poolId, msg.sender, amount);
    }
    
    /**
     * @notice Claim rewards without withdrawing
     * @param poolId AMM pool ID
     */
    function claimRewards(uint256 poolId) external nonReentrant {
        updateReward(poolId, msg.sender);
        
        UserInfo storage user = userInfo[poolId][msg.sender];
        uint256 reward = user.pendingRewards;
        
        if (reward > 0) {
            user.pendingRewards = 0;
            rewardToken.transfer(msg.sender, reward);
            emit RewardPaid(msg.sender, reward);
        }
    }
    
    /**
     * @notice Update provider rewards based on performance
     * @param provider Provider address
     * @param resourcesProvided Amount of resources provided
     * @param qualityScore Quality score (0-1000)
     * @param uptimeScore Uptime score (0-1000)
     */
    function updateProviderMetrics(
        address provider,
        uint256 resourcesProvided,
        uint256 qualityScore,
        uint256 uptimeScore
    ) external onlyOwner {
        require(qualityScore <= 1000, "Invalid quality score");
        require(uptimeScore <= 1000, "Invalid uptime score");
        
        ProviderRewards storage rewards = providerRewards[provider];
        
        // Update metrics with weighted average
        if (rewards.totalProvided > 0) {
            uint256 totalWeight = rewards.totalProvided.add(resourcesProvided);
            rewards.qualityScore = rewards.qualityScore.mul(rewards.totalProvided)
                .add(qualityScore.mul(resourcesProvided))
                .div(totalWeight);
            rewards.uptimeScore = rewards.uptimeScore.mul(rewards.totalProvided)
                .add(uptimeScore.mul(resourcesProvided))
                .div(totalWeight);
        } else {
            rewards.qualityScore = qualityScore;
            rewards.uptimeScore = uptimeScore;
        }
        
        rewards.totalProvided = rewards.totalProvided.add(resourcesProvided);
        
        // Calculate multiplier based on performance
        uint256 performanceScore = rewards.qualityScore.add(rewards.uptimeScore).div(2);
        if (performanceScore >= 950) {
            rewards.rewardMultiplier = 150; // 1.5x
        } else if (performanceScore >= 900) {
            rewards.rewardMultiplier = 125; // 1.25x
        } else if (performanceScore >= 800) {
            rewards.rewardMultiplier = 110; // 1.1x
        } else {
            rewards.rewardMultiplier = 100; // 1x
        }
        
        // Calculate pending rewards
        uint256 timeSinceLastClaim = block.timestamp.sub(rewards.lastClaimTime);
        if (timeSinceLastClaim > 0) {
            uint256 baseReward = PROVIDER_REWARD_RATE.mul(timeSinceLastClaim).div(86400);
            uint256 adjustedReward = baseReward.mul(rewards.rewardMultiplier).div(100);
            rewards.accumulatedRewards = rewards.accumulatedRewards.add(adjustedReward);
        }
        
        rewards.lastClaimTime = block.timestamp;
    }
    
    /**
     * @notice Claim provider rewards
     */
    function claimProviderRewards() external nonReentrant {
        ProviderRewards storage rewards = providerRewards[msg.sender];
        require(rewards.totalProvided > 0, "No resources provided");
        
        // Calculate any pending rewards since last update
        uint256 timeSinceLastClaim = block.timestamp.sub(rewards.lastClaimTime);
        if (timeSinceLastClaim > 0) {
            uint256 baseReward = PROVIDER_REWARD_RATE.mul(timeSinceLastClaim).div(86400);
            uint256 adjustedReward = baseReward.mul(rewards.rewardMultiplier).div(100);
            rewards.accumulatedRewards = rewards.accumulatedRewards.add(adjustedReward);
        }
        
        uint256 reward = rewards.accumulatedRewards;
        if (reward > 0) {
            rewards.accumulatedRewards = 0;
            rewards.lastClaimTime = block.timestamp;
            
            rewardToken.transfer(msg.sender, reward);
            emit ProviderRewardClaimed(msg.sender, reward);
        }
    }
    
    /**
     * @notice Update reward calculations
     */
    function updateReward(uint256 poolId, address account) internal {
        RewardPool storage pool = rewardPools[poolId];
        
        pool.rewardPerTokenStored = rewardPerToken(poolId);
        pool.lastUpdateTime = lastTimeRewardApplicable(poolId);
        
        if (account != address(0)) {
            UserInfo storage user = userInfo[poolId][account];
            user.pendingRewards = earned(poolId, account);
            user.rewardDebt = pool.rewardPerTokenStored;
        }
    }
    
    /**
     * @notice Calculate reward per token
     */
    function rewardPerToken(uint256 poolId) public view returns (uint256) {
        RewardPool memory pool = rewardPools[poolId];
        
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
    
    /**
     * @notice Calculate earned rewards
     */
    function earned(uint256 poolId, address account) public view returns (uint256) {
        UserInfo memory user = userInfo[poolId][account];
        
        return user.stakedAmount
            .mul(rewardPerToken(poolId).sub(user.rewardDebt))
            .div(1e18)
            .add(user.pendingRewards);
    }
    
    /**
     * @notice Get last applicable time for rewards
     */
    function lastTimeRewardApplicable(uint256 poolId) public view returns (uint256) {
        return block.timestamp < rewardPools[poolId].periodFinish
            ? block.timestamp
            : rewardPools[poolId].periodFinish;
    }
    
    /**
     * @notice Get provider performance stats
     */
    function getProviderStats(address provider) external view returns (
        uint256 totalProvided,
        uint256 qualityScore,
        uint256 uptimeScore,
        uint256 rewardMultiplier,
        uint256 pendingRewards
    ) {
        ProviderRewards memory rewards = providerRewards[provider];
        
        // Calculate pending rewards
        uint256 timeSinceLastClaim = block.timestamp.sub(rewards.lastClaimTime);
        uint256 pending = rewards.accumulatedRewards;
        
        if (timeSinceLastClaim > 0 && rewards.totalProvided > 0) {
            uint256 baseReward = PROVIDER_REWARD_RATE.mul(timeSinceLastClaim).div(86400);
            uint256 adjustedReward = baseReward.mul(rewards.rewardMultiplier).div(100);
            pending = pending.add(adjustedReward);
        }
        
        return (
            rewards.totalProvided,
            rewards.qualityScore,
            rewards.uptimeScore,
            rewards.rewardMultiplier,
            pending
        );
    }
    
    /**
     * @notice Emergency withdraw without rewards
     */
    function emergencyWithdraw(uint256 poolId) external {
        UserInfo storage user = userInfo[poolId][msg.sender];
        uint256 amount = user.stakedAmount;
        
        user.stakedAmount = 0;
        user.pendingRewards = 0;
        rewardPools[poolId].totalStaked = rewardPools[poolId].totalStaked.sub(amount);
        
        address lpToken = address(resourceAMM.lpTokens(poolId));
        IERC20(lpToken).transfer(msg.sender, amount);
        
        emit Withdrawn(poolId, msg.sender, amount);
    }
} 