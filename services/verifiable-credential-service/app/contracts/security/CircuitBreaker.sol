// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/security/Pausable.sol";

/**
 * @title CircuitBreaker
 * @dev Circuit breaker pattern for DeFi operations protection
 */
contract CircuitBreaker is AccessControl, Pausable {
    
    // Roles
    bytes32 public constant OPERATOR_ROLE = keccak256("OPERATOR_ROLE");
    bytes32 public constant GUARDIAN_ROLE = keccak256("GUARDIAN_ROLE");
    
    // Circuit breaker states
    enum CircuitState {
        CLOSED,     // Normal operation
        OPEN,       // Circuit tripped, operations blocked
        HALF_OPEN   // Testing recovery
    }
    
    // Operation types
    enum OperationType {
        NFT_MINT,
        LICENSE_PURCHASE,
        AUCTION,
        LENDING,
        STAKING,
        LIQUIDITY_PROVISION,
        ROYALTY_DISTRIBUTION,
        DAO_EXECUTION
    }
    
    // Circuit configuration
    struct CircuitConfig {
        uint256 failureThreshold;      // Number of failures to trip circuit
        uint256 failureWindow;         // Time window for counting failures
        uint256 recoveryTimeout;       // Time before attempting recovery
        uint256 successThreshold;      // Successes needed in half-open state
        bool enabled;                  // Whether this circuit is active
    }
    
    // Rate limiting configuration
    struct RateLimitConfig {
        uint256 maxOperations;         // Max operations in window
        uint256 timeWindow;            // Time window in seconds
        bool enabled;                  // Whether rate limiting is active
    }
    
    // Events
    event CircuitOpened(OperationType indexed operation, string reason);
    event CircuitClosed(OperationType indexed operation);
    event CircuitHalfOpen(OperationType indexed operation);
    event OperationExecuted(OperationType indexed operation, address indexed user);
    event OperationFailed(OperationType indexed operation, address indexed user, string reason);
    event ConfigurationUpdated(OperationType indexed operation);
    event EmergencyStop(address indexed guardian);
    event RateLimitExceeded(OperationType indexed operation, address indexed user);
    
    // State variables
    mapping(OperationType => CircuitState) public circuitStates;
    mapping(OperationType => CircuitConfig) public circuitConfigs;
    mapping(OperationType => RateLimitConfig) public rateLimitConfigs;
    
    // Tracking
    mapping(OperationType => uint256) public failureCount;
    mapping(OperationType => uint256) public successCount;
    mapping(OperationType => uint256) public lastFailureTime;
    mapping(OperationType => uint256) public circuitOpenTime;
    
    // Rate limiting tracking
    mapping(OperationType => mapping(address => uint256)) public operationCount;
    mapping(OperationType => mapping(address => uint256)) public lastOperationTime;
    
    // Global limits
    uint256 public constant MAX_DAILY_VOLUME = 1000000 ether;
    uint256 public dailyVolume;
    uint256 public lastVolumeReset;
    
    // Price deviation limits
    uint256 public constant MAX_PRICE_DEVIATION = 2000; // 20%
    mapping(address => uint256) public lastKnownPrices;
    
    /**
     * @dev Constructor
     */
    constructor() {
        _setupRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _setupRole(OPERATOR_ROLE, msg.sender);
        _setupRole(GUARDIAN_ROLE, msg.sender);
        
        // Initialize default configurations
        _initializeDefaultConfigs();
    }
    
    /**
     * @dev Initialize default circuit configurations
     */
    function _initializeDefaultConfigs() private {
        // NFT Minting
        circuitConfigs[OperationType.NFT_MINT] = CircuitConfig({
            failureThreshold: 10,
            failureWindow: 1 hours,
            recoveryTimeout: 30 minutes,
            successThreshold: 5,
            enabled: true
        });
        
        // Lending - more sensitive
        circuitConfigs[OperationType.LENDING] = CircuitConfig({
            failureThreshold: 5,
            failureWindow: 30 minutes,
            recoveryTimeout: 1 hours,
            successThreshold: 10,
            enabled: true
        });
        
        // Liquidity provision - most sensitive
        circuitConfigs[OperationType.LIQUIDITY_PROVISION] = CircuitConfig({
            failureThreshold: 3,
            failureWindow: 15 minutes,
            recoveryTimeout: 2 hours,
            successThreshold: 20,
            enabled: true
        });
        
        // Rate limits
        rateLimitConfigs[OperationType.NFT_MINT] = RateLimitConfig({
            maxOperations: 10,
            timeWindow: 1 hours,
            enabled: true
        });
        
        rateLimitConfigs[OperationType.LENDING] = RateLimitConfig({
            maxOperations: 5,
            timeWindow: 1 hours,
            enabled: true
        });
    }
    
    /**
     * @dev Check if operation is allowed
     */
    function canExecute(
        OperationType operation,
        address user,
        uint256 value
    ) public view returns (bool allowed, string memory reason) {
        // Check global pause
        if (paused()) {
            return (false, "System paused");
        }
        
        // Check circuit state
        CircuitState state = _getCurrentState(operation);
        if (state == CircuitState.OPEN) {
            return (false, "Circuit breaker open");
        }
        
        // Check rate limiting
        if (rateLimitConfigs[operation].enabled) {
            if (!_checkRateLimit(operation, user)) {
                return (false, "Rate limit exceeded");
            }
        }
        
        // Check daily volume
        if (_wouldExceedDailyVolume(value)) {
            return (false, "Daily volume limit exceeded");
        }
        
        return (true, "");
    }
    
    /**
     * @dev Record successful operation
     */
    function recordSuccess(
        OperationType operation,
        address user,
        uint256 value
    ) external onlyRole(OPERATOR_ROLE) {
        require(!paused(), "System paused");
        
        CircuitState currentState = _getCurrentState(operation);
        
        // Update success count
        if (currentState == CircuitState.HALF_OPEN) {
            successCount[operation]++;
            
            // Check if we can close the circuit
            if (successCount[operation] >= circuitConfigs[operation].successThreshold) {
                _closeCircuit(operation);
            }
        }
        
        // Update rate limiting
        if (rateLimitConfigs[operation].enabled) {
            operationCount[operation][user]++;
            lastOperationTime[operation][user] = block.timestamp;
        }
        
        // Update daily volume
        _updateDailyVolume(value);
        
        emit OperationExecuted(operation, user);
    }
    
    /**
     * @dev Record failed operation
     */
    function recordFailure(
        OperationType operation,
        address user,
        string memory reason
    ) external onlyRole(OPERATOR_ROLE) {
        CircuitState currentState = _getCurrentState(operation);
        
        // Update failure tracking
        if (block.timestamp - lastFailureTime[operation] > circuitConfigs[operation].failureWindow) {
            // Reset counter if outside window
            failureCount[operation] = 1;
        } else {
            failureCount[operation]++;
        }
        lastFailureTime[operation] = block.timestamp;
        
        // Check if we should open the circuit
        if (currentState == CircuitState.CLOSED || currentState == CircuitState.HALF_OPEN) {
            if (failureCount[operation] >= circuitConfigs[operation].failureThreshold) {
                _openCircuit(operation, reason);
            }
        }
        
        // If in half-open state, immediately re-open
        if (currentState == CircuitState.HALF_OPEN) {
            _openCircuit(operation, "Failure during recovery");
            successCount[operation] = 0;
        }
        
        emit OperationFailed(operation, user, reason);
    }
    
    /**
     * @dev Check for price manipulation
     */
    function checkPriceDeviation(
        address asset,
        uint256 currentPrice
    ) external view returns (bool safe) {
        uint256 lastPrice = lastKnownPrices[asset];
        if (lastPrice == 0) {
            return true; // First price record
        }
        
        uint256 deviation;
        if (currentPrice > lastPrice) {
            deviation = ((currentPrice - lastPrice) * 10000) / lastPrice;
        } else {
            deviation = ((lastPrice - currentPrice) * 10000) / lastPrice;
        }
        
        return deviation <= MAX_PRICE_DEVIATION;
    }
    
    /**
     * @dev Update known price
     */
    function updateKnownPrice(
        address asset,
        uint256 price
    ) external onlyRole(OPERATOR_ROLE) {
        lastKnownPrices[asset] = price;
    }
    
    /**
     * @dev Emergency stop - instantly pause all operations
     */
    function emergencyStop() external onlyRole(GUARDIAN_ROLE) {
        _pause();
        
        // Open all circuits
        for (uint8 i = 0; i <= uint8(OperationType.DAO_EXECUTION); i++) {
            if (circuitConfigs[OperationType(i)].enabled) {
                circuitStates[OperationType(i)] = CircuitState.OPEN;
                emit CircuitOpened(OperationType(i), "Emergency stop");
            }
        }
        
        emit EmergencyStop(msg.sender);
    }
    
    /**
     * @dev Resume operations after emergency
     */
    function resumeOperations() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _unpause();
        
        // Reset all circuits to closed
        for (uint8 i = 0; i <= uint8(OperationType.DAO_EXECUTION); i++) {
            if (circuitConfigs[OperationType(i)].enabled) {
                circuitStates[OperationType(i)] = CircuitState.CLOSED;
                failureCount[OperationType(i)] = 0;
                successCount[OperationType(i)] = 0;
                emit CircuitClosed(OperationType(i));
            }
        }
    }
    
    /**
     * @dev Update circuit configuration
     */
    function updateCircuitConfig(
        OperationType operation,
        CircuitConfig memory config
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        circuitConfigs[operation] = config;
        emit ConfigurationUpdated(operation);
    }
    
    /**
     * @dev Update rate limit configuration
     */
    function updateRateLimitConfig(
        OperationType operation,
        RateLimitConfig memory config
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        rateLimitConfigs[operation] = config;
        emit ConfigurationUpdated(operation);
    }
    
    /**
     * @dev Manually reset circuit
     */
    function resetCircuit(OperationType operation) external onlyRole(GUARDIAN_ROLE) {
        circuitStates[operation] = CircuitState.CLOSED;
        failureCount[operation] = 0;
        successCount[operation] = 0;
        emit CircuitClosed(operation);
    }
    
    /**
     * @dev Get current circuit state with timeout check
     */
    function _getCurrentState(OperationType operation) private view returns (CircuitState) {
        CircuitState state = circuitStates[operation];
        
        // Check if circuit should transition to half-open
        if (state == CircuitState.OPEN) {
            if (block.timestamp >= circuitOpenTime[operation] + circuitConfigs[operation].recoveryTimeout) {
                return CircuitState.HALF_OPEN;
            }
        }
        
        return state;
    }
    
    /**
     * @dev Open circuit breaker
     */
    function _openCircuit(OperationType operation, string memory reason) private {
        circuitStates[operation] = CircuitState.OPEN;
        circuitOpenTime[operation] = block.timestamp;
        failureCount[operation] = 0;
        emit CircuitOpened(operation, reason);
    }
    
    /**
     * @dev Close circuit breaker
     */
    function _closeCircuit(OperationType operation) private {
        circuitStates[operation] = CircuitState.CLOSED;
        failureCount[operation] = 0;
        successCount[operation] = 0;
        emit CircuitClosed(operation);
    }
    
    /**
     * @dev Check rate limit for user
     */
    function _checkRateLimit(
        OperationType operation,
        address user
    ) private view returns (bool) {
        RateLimitConfig memory config = rateLimitConfigs[operation];
        
        // Check if we're in a new time window
        if (block.timestamp >= lastOperationTime[operation][user] + config.timeWindow) {
            return true; // New window, allow operation
        }
        
        // Check if under limit
        return operationCount[operation][user] < config.maxOperations;
    }
    
    /**
     * @dev Check if value would exceed daily volume
     */
    function _wouldExceedDailyVolume(uint256 value) private view returns (bool) {
        // Reset daily volume if new day
        if (block.timestamp >= lastVolumeReset + 1 days) {
            return value > MAX_DAILY_VOLUME;
        }
        
        return dailyVolume + value > MAX_DAILY_VOLUME;
    }
    
    /**
     * @dev Update daily volume tracking
     */
    function _updateDailyVolume(uint256 value) private {
        // Reset if new day
        if (block.timestamp >= lastVolumeReset + 1 days) {
            dailyVolume = value;
            lastVolumeReset = block.timestamp;
        } else {
            dailyVolume += value;
        }
    }
    
    /**
     * @dev Get circuit status
     */
    function getCircuitStatus(OperationType operation) 
        external 
        view 
        returns (
            CircuitState state,
            uint256 failures,
            uint256 successes,
            uint256 timeUntilRecovery
        ) 
    {
        state = _getCurrentState(operation);
        failures = failureCount[operation];
        successes = successCount[operation];
        
        if (state == CircuitState.OPEN) {
            uint256 elapsed = block.timestamp - circuitOpenTime[operation];
            uint256 timeout = circuitConfigs[operation].recoveryTimeout;
            timeUntilRecovery = elapsed >= timeout ? 0 : timeout - elapsed;
        } else {
            timeUntilRecovery = 0;
        }
    }
} 