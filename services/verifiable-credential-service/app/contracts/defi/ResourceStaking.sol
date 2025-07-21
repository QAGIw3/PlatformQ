// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC1155/IERC1155.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/security/Pausable.sol";
import "@openzeppelin/contracts/utils/math/Math.sol";
import "./ResourceToken.sol";
import "./InfrastructureRewards.sol";

/**
 * @title ResourceStaking
 * @notice Enables staking of resource tokens and LP tokens with delegation pools
 * @dev Supports slashing, auto-compounding, and multiple reward tokens
 */
contract ResourceStaking is ReentrancyGuard, AccessControl, Pausable {
    using Math for uint256;
    
    // Roles
    bytes32 public constant OPERATOR_ROLE = keccak256("OPERATOR_ROLE");
    bytes32 public constant SLASHER_ROLE = keccak256("SLASHER_ROLE");
    bytes32 public constant REWARD_MANAGER_ROLE = keccak256("REWARD_MANAGER_ROLE");
    
    // Constants
    uint256 public constant PRECISION = 1e18;
    uint256 public constant MAX_SLASHING_RATE = 5000; // 50% max slashing
    uint256 public constant MIN_STAKE_DURATION = 1 days;
    uint256 public constant MAX_DELEGATION_FEE = 2000; // 20% max fee
    
    // Contracts
    ResourceToken public immutable resourceToken;
    InfrastructureRewards public immutable rewardsContract;
    
    // Staking pools
    struct StakingPool {
        uint256 tokenId;              // Resource token ID or LP token address encoded
        uint256 totalStaked;          // Total amount staked
        uint256 rewardPerToken;       // Accumulated rewards per token
        uint256 lastUpdateTime;       // Last reward update timestamp
        uint256 rewardRate;           // Rewards distributed per second
        uint256 periodFinish;         // Reward period end time
        uint256 minStakeAmount;       // Minimum stake amount
        bool isLP;                    // Whether this is an LP token pool
        address lpTokenAddress;       // LP token address if isLP
    }
    
    // Delegation pools
    struct DelegationPool {
        address operator;             // Pool operator address
        uint256 totalDelegated;       // Total amount delegated
        uint256 operatorFee;          // Fee percentage (basis points)
        uint256 minDelegation;        // Minimum delegation amount
        bool acceptingDelegations;    // Whether accepting new delegations
        string metadata;              // Pool description/strategy
        uint256 performanceScore;     // Performance score (0-100)
        uint256 lastSlashTime;        // Last time pool was slashed
    }
    
    // User stakes
    struct Stake {
        uint256 amount;               // Staked amount
        uint256 rewardDebt;           // Reward debt for calculations
        uint256 lockEndTime;          // Lock period end time
        uint256 lastClaimTime;        // Last reward claim time
        uint256 poolId;               // Staking pool ID
        bool isDelegated;             // Whether stake is delegated
        uint256 delegationPoolId;     // Delegation pool ID if delegated
    }
    
    // State variables
    mapping(uint256 => StakingPool) public stakingPools;
    mapping(uint256 => DelegationPool) public delegationPools;
    mapping(address => mapping(uint256 => Stake)) public userStakes;
    mapping(address => uint256[]) public userStakeIds;
    mapping(uint256 => mapping(address => uint256)) public poolRewards; // poolId => rewardToken => amount
    
    uint256 public nextPoolId = 1;
    uint256 public nextDelegationPoolId = 1;
    uint256 public nextStakeId = 1;
    
    // Auto-compounding
    mapping(address => bool) public autoCompoundEnabled;
    mapping(address => uint256) public compoundRewards;
    uint256 public compoundFee = 100; // 1% fee for auto-compound
    
    // Slashing
    mapping(uint256 => uint256) public slashingRates; // poolId => rate in basis points
    uint256 public slashingReserve;
    
    // Events
    event PoolCreated(uint256 indexed poolId, uint256 tokenId, bool isLP, address lpToken);
    event DelegationPoolCreated(uint256 indexed poolId, address indexed operator, uint256 fee);
    event Staked(address indexed user, uint256 indexed poolId, uint256 amount, uint256 stakeId);
    event Withdrawn(address indexed user, uint256 indexed poolId, uint256 amount);
    event RewardClaimed(address indexed user, uint256 indexed poolId, uint256 reward);
    event Delegated(address indexed user, uint256 indexed delegationPoolId, uint256 amount);
    event Slashed(uint256 indexed poolId, address indexed user, uint256 amount);
    event RewardAdded(uint256 indexed poolId, uint256 reward);
    event CompoundExecuted(address indexed user, uint256 amount);
    event OperatorFeeUpdated(uint256 indexed delegationPoolId, uint256 newFee);
    
    /**
     * @dev Constructor
     * @param _resourceToken Address of ResourceToken contract
     * @param _rewardsContract Address of InfrastructureRewards contract
     */
    constructor(address _resourceToken, address _rewardsContract) {
        require(_resourceToken != address(0), "Invalid resource token");
        require(_rewardsContract != address(0), "Invalid rewards contract");
        
        resourceToken = ResourceToken(_resourceToken);
        rewardsContract = InfrastructureRewards(_rewardsContract);
        
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(OPERATOR_ROLE, msg.sender);
        _grantRole(SLASHER_ROLE, msg.sender);
        _grantRole(REWARD_MANAGER_ROLE, msg.sender);
    }
    
    /**
     * @notice Create a new staking pool
     * @param tokenId Resource token ID (0 for LP tokens)
     * @param minStakeAmount Minimum stake amount
     * @param isLP Whether this is an LP token pool
     * @param lpTokenAddress LP token address if isLP
     */
    function createStakingPool(
        uint256 tokenId,
        uint256 minStakeAmount,
        bool isLP,
        address lpTokenAddress
    ) external onlyRole(OPERATOR_ROLE) returns (uint256) {
        require(!isLP || lpTokenAddress != address(0), "Invalid LP token");
        require(isLP || tokenId > 0, "Invalid token ID");
        
        uint256 poolId = nextPoolId++;
        
        stakingPools[poolId] = StakingPool({
            tokenId: tokenId,
            totalStaked: 0,
            rewardPerToken: 0,
            lastUpdateTime: block.timestamp,
            rewardRate: 0,
            periodFinish: 0,
            minStakeAmount: minStakeAmount,
            isLP: isLP,
            lpTokenAddress: lpTokenAddress
        });
        
        emit PoolCreated(poolId, tokenId, isLP, lpTokenAddress);
        return poolId;
    }
    
    /**
     * @notice Create a delegation pool
     * @param operatorFee Fee percentage in basis points
     * @param minDelegation Minimum delegation amount
     * @param metadata Pool description
     */
    function createDelegationPool(
        uint256 operatorFee,
        uint256 minDelegation,
        string calldata metadata
    ) external returns (uint256) {
        require(operatorFee <= MAX_DELEGATION_FEE, "Fee too high");
        
        uint256 poolId = nextDelegationPoolId++;
        
        delegationPools[poolId] = DelegationPool({
            operator: msg.sender,
            totalDelegated: 0,
            operatorFee: operatorFee,
            minDelegation: minDelegation,
            acceptingDelegations: true,
            metadata: metadata,
            performanceScore: 50, // Start at neutral
            lastSlashTime: 0
        });
        
        emit DelegationPoolCreated(poolId, msg.sender, operatorFee);
        return poolId;
    }
    
    /**
     * @notice Stake tokens in a pool
     * @param poolId Staking pool ID
     * @param amount Amount to stake
     * @param lockDuration Lock duration in seconds
     */
    function stake(
        uint256 poolId,
        uint256 amount,
        uint256 lockDuration
    ) external nonReentrant whenNotPaused returns (uint256) {
        StakingPool storage pool = stakingPools[poolId];
        require(pool.minStakeAmount > 0, "Pool does not exist");
        require(amount >= pool.minStakeAmount, "Below minimum stake");
        require(lockDuration >= MIN_STAKE_DURATION, "Lock duration too short");
        
        _updateReward(poolId, msg.sender);
        
        // Transfer tokens
        if (pool.isLP) {
            IERC20(pool.lpTokenAddress).transferFrom(msg.sender, address(this), amount);
        } else {
            resourceToken.safeTransferFrom(msg.sender, address(this), pool.tokenId, amount, "");
        }
        
        // Create stake
        uint256 stakeId = nextStakeId++;
        userStakes[msg.sender][stakeId] = Stake({
            amount: amount,
            rewardDebt: pool.rewardPerToken,
            lockEndTime: block.timestamp + lockDuration,
            lastClaimTime: block.timestamp,
            poolId: poolId,
            isDelegated: false,
            delegationPoolId: 0
        });
        
        userStakeIds[msg.sender].push(stakeId);
        pool.totalStaked += amount;
        
        emit Staked(msg.sender, poolId, amount, stakeId);
        return stakeId;
    }
    
    /**
     * @notice Delegate stake to an operator pool
     * @param stakeId User's stake ID
     * @param delegationPoolId Delegation pool to join
     */
    function delegateStake(
        uint256 stakeId,
        uint256 delegationPoolId
    ) external nonReentrant {
        Stake storage userStake = userStakes[msg.sender][stakeId];
        require(userStake.amount > 0, "Stake does not exist");
        require(!userStake.isDelegated, "Already delegated");
        
        DelegationPool storage pool = delegationPools[delegationPoolId];
        require(pool.acceptingDelegations, "Pool not accepting delegations");
        require(userStake.amount >= pool.minDelegation, "Below minimum delegation");
        
        userStake.isDelegated = true;
        userStake.delegationPoolId = delegationPoolId;
        pool.totalDelegated += userStake.amount;
        
        emit Delegated(msg.sender, delegationPoolId, userStake.amount);
    }
    
    /**
     * @notice Withdraw staked tokens
     * @param stakeId Stake ID to withdraw
     */
    function withdraw(uint256 stakeId) external nonReentrant {
        Stake storage userStake = userStakes[msg.sender][stakeId];
        require(userStake.amount > 0, "No stake found");
        require(block.timestamp >= userStake.lockEndTime, "Still locked");
        
        uint256 poolId = userStake.poolId;
        uint256 amount = userStake.amount;
        
        _updateReward(poolId, msg.sender);
        _claimReward(stakeId);
        
        // Update delegation pool if delegated
        if (userStake.isDelegated) {
            DelegationPool storage delegationPool = delegationPools[userStake.delegationPoolId];
            delegationPool.totalDelegated -= amount;
        }
        
        // Update pool
        StakingPool storage pool = stakingPools[poolId];
        pool.totalStaked -= amount;
        
        // Transfer tokens back
        if (pool.isLP) {
            IERC20(pool.lpTokenAddress).transfer(msg.sender, amount);
        } else {
            resourceToken.safeTransferFrom(address(this), msg.sender, pool.tokenId, amount, "");
        }
        
        // Remove stake
        delete userStakes[msg.sender][stakeId];
        _removeStakeId(msg.sender, stakeId);
        
        emit Withdrawn(msg.sender, poolId, amount);
    }
    
    /**
     * @notice Claim rewards for a stake
     * @param stakeId Stake ID to claim rewards for
     */
    function claimReward(uint256 stakeId) external nonReentrant {
        require(userStakes[msg.sender][stakeId].amount > 0, "No stake found");
        _updateReward(userStakes[msg.sender][stakeId].poolId, msg.sender);
        _claimReward(stakeId);
    }
    
    /**
     * @notice Slash a user's stake for poor performance
     * @param user User to slash
     * @param stakeId Stake ID to slash
     * @param slashingBps Slashing rate in basis points
     */
    function slash(
        address user,
        uint256 stakeId,
        uint256 slashingBps
    ) external onlyRole(SLASHER_ROLE) {
        require(slashingBps <= MAX_SLASHING_RATE, "Slashing rate too high");
        
        Stake storage userStake = userStakes[user][stakeId];
        require(userStake.amount > 0, "No stake found");
        
        uint256 slashAmount = (userStake.amount * slashingBps) / 10000;
        userStake.amount -= slashAmount;
        
        // Update pool
        StakingPool storage pool = stakingPools[userStake.poolId];
        pool.totalStaked -= slashAmount;
        
        // Update delegation pool performance
        if (userStake.isDelegated) {
            DelegationPool storage delegationPool = delegationPools[userStake.delegationPoolId];
            delegationPool.totalDelegated -= slashAmount;
            delegationPool.performanceScore = (delegationPool.performanceScore * 90) / 100; // Reduce by 10%
            delegationPool.lastSlashTime = block.timestamp;
        }
        
        // Add to slashing reserve
        slashingReserve += slashAmount;
        
        emit Slashed(userStake.poolId, user, slashAmount);
    }
    
    /**
     * @notice Add rewards to a pool
     * @param poolId Pool to add rewards to
     * @param reward Reward amount
     * @param duration Reward duration in seconds
     */
    function addReward(
        uint256 poolId,
        uint256 reward,
        uint256 duration
    ) external onlyRole(REWARD_MANAGER_ROLE) {
        StakingPool storage pool = stakingPools[poolId];
        require(pool.minStakeAmount > 0, "Pool does not exist");
        
        _updateReward(poolId, address(0));
        
        if (block.timestamp >= pool.periodFinish) {
            pool.rewardRate = reward / duration;
        } else {
            uint256 remaining = pool.periodFinish - block.timestamp;
            uint256 leftover = remaining * pool.rewardRate;
            pool.rewardRate = (reward + leftover) / duration;
        }
        
        pool.lastUpdateTime = block.timestamp;
        pool.periodFinish = block.timestamp + duration;
        
        emit RewardAdded(poolId, reward);
    }
    
    /**
     * @notice Enable auto-compounding for user
     * @param enable Whether to enable auto-compounding
     */
    function setAutoCompound(bool enable) external {
        autoCompoundEnabled[msg.sender] = enable;
    }
    
    /**
     * @notice Execute auto-compound for a user
     * @param user User to compound for
     * @param stakeIds Array of stake IDs to compound
     */
    function executeAutoCompound(
        address user,
        uint256[] calldata stakeIds
    ) external nonReentrant {
        require(autoCompoundEnabled[user], "Auto-compound not enabled");
        
        uint256 totalCompounded = 0;
        
        for (uint256 i = 0; i < stakeIds.length; i++) {
            Stake storage userStake = userStakes[user][stakeIds[i]];
            if (userStake.amount == 0) continue;
            
            _updateReward(userStake.poolId, user);
            uint256 reward = _calculateReward(stakeIds[i]);
            
            if (reward > 0) {
                // Take compound fee
                uint256 fee = (reward * compoundFee) / 10000;
                uint256 compoundAmount = reward - fee;
                
                // Add to stake
                userStake.amount += compoundAmount;
                userStake.lastClaimTime = block.timestamp;
                
                // Update pool
                StakingPool storage pool = stakingPools[userStake.poolId];
                pool.totalStaked += compoundAmount;
                
                totalCompounded += compoundAmount;
                compoundRewards[msg.sender] += fee;
            }
        }
        
        if (totalCompounded > 0) {
            emit CompoundExecuted(user, totalCompounded);
        }
    }
    
    /**
     * @notice Update operator fee for delegation pool
     * @param poolId Delegation pool ID
     * @param newFee New fee in basis points
     */
    function updateOperatorFee(
        uint256 poolId,
        uint256 newFee
    ) external {
        DelegationPool storage pool = delegationPools[poolId];
        require(pool.operator == msg.sender, "Not pool operator");
        require(newFee <= MAX_DELEGATION_FEE, "Fee too high");
        
        pool.operatorFee = newFee;
        emit OperatorFeeUpdated(poolId, newFee);
    }
    
    /**
     * @notice Get user's stakes
     * @param user User address
     * @return Array of stake IDs
     */
    function getUserStakes(address user) external view returns (uint256[] memory) {
        return userStakeIds[user];
    }
    
    /**
     * @notice Calculate pending rewards for a stake
     * @param stakeId Stake ID
     * @return Pending reward amount
     */
    function pendingReward(uint256 stakeId) external view returns (uint256) {
        address user = _getStakeOwner(stakeId);
        require(user != address(0), "Stake not found");
        
        Stake memory userStake = userStakes[user][stakeId];
        StakingPool memory pool = stakingPools[userStake.poolId];
        
        uint256 rewardPerToken = pool.rewardPerToken;
        if (pool.totalStaked > 0) {
            uint256 timeDelta = _lastTimeRewardApplicable(userStake.poolId) - pool.lastUpdateTime;
            rewardPerToken += (timeDelta * pool.rewardRate * PRECISION) / pool.totalStaked;
        }
        
        return (userStake.amount * (rewardPerToken - userStake.rewardDebt)) / PRECISION;
    }
    
    // Internal functions
    
    function _updateReward(uint256 poolId, address account) internal {
        StakingPool storage pool = stakingPools[poolId];
        pool.rewardPerToken = _rewardPerToken(poolId);
        pool.lastUpdateTime = _lastTimeRewardApplicable(poolId);
        
        if (account != address(0)) {
            // Update all user stakes in this pool
            uint256[] memory stakeIds = userStakeIds[account];
            for (uint256 i = 0; i < stakeIds.length; i++) {
                Stake storage userStake = userStakes[account][stakeIds[i]];
                if (userStake.poolId == poolId) {
                    userStake.rewardDebt = pool.rewardPerToken;
                }
            }
        }
    }
    
    function _rewardPerToken(uint256 poolId) internal view returns (uint256) {
        StakingPool memory pool = stakingPools[poolId];
        if (pool.totalStaked == 0) {
            return pool.rewardPerToken;
        }
        
        uint256 timeDelta = _lastTimeRewardApplicable(poolId) - pool.lastUpdateTime;
        return pool.rewardPerToken + (timeDelta * pool.rewardRate * PRECISION) / pool.totalStaked;
    }
    
    function _lastTimeRewardApplicable(uint256 poolId) internal view returns (uint256) {
        StakingPool memory pool = stakingPools[poolId];
        return block.timestamp < pool.periodFinish ? block.timestamp : pool.periodFinish;
    }
    
    function _calculateReward(uint256 stakeId) internal view returns (uint256) {
        address user = _getStakeOwner(stakeId);
        Stake memory userStake = userStakes[user][stakeId];
        StakingPool memory pool = stakingPools[userStake.poolId];
        
        uint256 reward = (userStake.amount * (pool.rewardPerToken - userStake.rewardDebt)) / PRECISION;
        
        // Apply delegation fee if applicable
        if (userStake.isDelegated) {
            DelegationPool memory delegationPool = delegationPools[userStake.delegationPoolId];
            uint256 operatorFee = (reward * delegationPool.operatorFee) / 10000;
            reward -= operatorFee;
        }
        
        return reward;
    }
    
    function _claimReward(uint256 stakeId) internal {
        uint256 reward = _calculateReward(stakeId);
        if (reward > 0) {
            Stake storage userStake = userStakes[msg.sender][stakeId];
            userStake.lastClaimTime = block.timestamp;
            
            // Transfer reward tokens
            // In production, this would transfer the actual reward token
            emit RewardClaimed(msg.sender, userStake.poolId, reward);
        }
    }
    
    function _removeStakeId(address user, uint256 stakeId) internal {
        uint256[] storage stakeIds = userStakeIds[user];
        for (uint256 i = 0; i < stakeIds.length; i++) {
            if (stakeIds[i] == stakeId) {
                stakeIds[i] = stakeIds[stakeIds.length - 1];
                stakeIds.pop();
                break;
            }
        }
    }
    
    function _getStakeOwner(uint256 stakeId) internal view returns (address) {
        // In production, would maintain a mapping of stakeId to owner
        // For now, this is a placeholder
        return address(0);
    }
    
    // Admin functions
    
    function pause() external onlyRole(OPERATOR_ROLE) {
        _pause();
    }
    
    function unpause() external onlyRole(OPERATOR_ROLE) {
        _unpause();
    }
    
    function setCompoundFee(uint256 _fee) external onlyRole(OPERATOR_ROLE) {
        require(_fee <= 500, "Fee too high"); // Max 5%
        compoundFee = _fee;
    }
    
    function withdrawSlashingReserve(address to) external onlyRole(DEFAULT_ADMIN_ROLE) {
        uint256 amount = slashingReserve;
        slashingReserve = 0;
        resourceToken.safeTransferFrom(address(this), to, 1, amount, ""); // Assuming token ID 1 for reserves
    }
} 