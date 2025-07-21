// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/security/Pausable.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";

/**
 * @title ComputeResourceOracle
 * @notice Oracle contract for compute resource quality measurements and scores
 */
contract ComputeResourceOracle is AccessControl, Pausable, ReentrancyGuard {
    bytes32 public constant ORACLE_ROLE = keccak256("ORACLE_ROLE");
    bytes32 public constant ADMIN_ROLE = keccak256("ADMIN_ROLE");
    
    // Measurement types
    enum MeasurementType {
        QUANTUM_FIDELITY,
        QUANTUM_COHERENCE,
        QUANTUM_ERROR_RATE,
        AI_BENCHMARK,
        AI_INFERENCE_LATENCY,
        AI_THERMAL,
        AI_POWER,
        NETWORK_LATENCY,
        NETWORK_BANDWIDTH,
        NETWORK_PACKET_LOSS,
        NETWORK_JITTER
    }
    
    // Oracle feed data
    struct OracleFeed {
        uint256 value;          // Scaled value (multiply by 1e6 for decimals)
        uint256 timestamp;      // Block timestamp
        uint256 confidence;     // Confidence score (0-100)
        uint256 measurementCount; // Number of measurements aggregated
        address oracle;         // Oracle that submitted the data
    }
    
    // Quality score data
    struct QualityScore {
        uint256 score;          // Overall quality score (0-100)
        uint256 timestamp;      // Last update timestamp
        uint256 componentCount; // Number of component scores
        mapping(string => uint256) components; // Component scores
    }
    
    // Storage
    mapping(string => mapping(MeasurementType => OracleFeed)) public feeds;
    mapping(string => QualityScore) public qualityScores;
    mapping(address => bool) public trustedOracles;
    
    // Configuration
    uint256 public maxDataAge = 3600; // Maximum age of data in seconds
    uint256 public minConfidence = 80; // Minimum confidence score
    uint256 public updateCooldown = 60; // Minimum time between updates
    
    // Events
    event FeedUpdated(
        string indexed resourceId,
        MeasurementType indexed measurementType,
        uint256 value,
        uint256 confidence,
        address oracle
    );
    
    event BatchFeedUpdated(
        string[] resourceIds,
        MeasurementType[] measurementTypes,
        uint256[] values,
        address oracle
    );
    
    event QualityScoreUpdated(
        string indexed resourceId,
        uint256 score,
        uint256 timestamp,
        address oracle
    );
    
    event OracleAdded(address indexed oracle);
    event OracleRemoved(address indexed oracle);
    event ConfigurationUpdated(uint256 maxDataAge, uint256 minConfidence, uint256 updateCooldown);
    
    constructor() {
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(ADMIN_ROLE, msg.sender);
        _grantRole(ORACLE_ROLE, msg.sender);
    }
    
    /**
     * @notice Update a single oracle feed
     * @param resourceId The resource identifier
     * @param measurementType The type of measurement
     * @param value The measurement value (scaled by 1e6)
     * @param timestamp The measurement timestamp
     * @param confidence The confidence score (0-100)
     */
    function updateFeed(
        string memory resourceId,
        MeasurementType measurementType,
        uint256 value,
        uint256 timestamp,
        uint256 confidence
    ) external onlyRole(ORACLE_ROLE) whenNotPaused {
        require(bytes(resourceId).length > 0, "Invalid resource ID");
        require(confidence >= minConfidence, "Confidence too low");
        require(timestamp <= block.timestamp, "Invalid timestamp");
        require(block.timestamp - timestamp <= maxDataAge, "Data too old");
        
        OracleFeed storage feed = feeds[resourceId][measurementType];
        
        // Check cooldown
        require(
            block.timestamp >= feed.timestamp + updateCooldown,
            "Update cooldown not met"
        );
        
        // Update feed
        feed.value = value;
        feed.timestamp = block.timestamp;
        feed.confidence = confidence;
        feed.measurementCount = 1;
        feed.oracle = msg.sender;
        
        emit FeedUpdated(resourceId, measurementType, value, confidence, msg.sender);
    }
    
    /**
     * @notice Update multiple oracle feeds in batch
     * @param resourceIds Array of resource identifiers
     * @param measurementTypes Array of measurement types
     * @param values Array of measurement values
     * @param timestamps Array of timestamps
     * @param confidences Array of confidence scores
     */
    function batchUpdateFeeds(
        string[] memory resourceIds,
        MeasurementType[] memory measurementTypes,
        uint256[] memory values,
        uint256[] memory timestamps,
        uint256[] memory confidences
    ) external onlyRole(ORACLE_ROLE) whenNotPaused {
        require(
            resourceIds.length == measurementTypes.length &&
            resourceIds.length == values.length &&
            resourceIds.length == timestamps.length &&
            resourceIds.length == confidences.length,
            "Array length mismatch"
        );
        
        for (uint256 i = 0; i < resourceIds.length; i++) {
            require(bytes(resourceIds[i]).length > 0, "Invalid resource ID");
            require(confidences[i] >= minConfidence, "Confidence too low");
            require(timestamps[i] <= block.timestamp, "Invalid timestamp");
            require(block.timestamp - timestamps[i] <= maxDataAge, "Data too old");
            
            OracleFeed storage feed = feeds[resourceIds[i]][measurementTypes[i]];
            
            // Update feed (skip cooldown check for batch)
            feed.value = values[i];
            feed.timestamp = block.timestamp;
            feed.confidence = confidences[i];
            feed.measurementCount = 1;
            feed.oracle = msg.sender;
        }
        
        emit BatchFeedUpdated(resourceIds, measurementTypes, values, msg.sender);
    }
    
    /**
     * @notice Update quality score for a resource
     * @param resourceId The resource identifier
     * @param score The overall quality score (0-100)
     * @param timestamp The score calculation timestamp
     */
    function updateQualityScore(
        string memory resourceId,
        uint256 score,
        uint256 timestamp
    ) external onlyRole(ORACLE_ROLE) whenNotPaused {
        require(bytes(resourceId).length > 0, "Invalid resource ID");
        require(score <= 100, "Invalid score");
        require(timestamp <= block.timestamp, "Invalid timestamp");
        require(block.timestamp - timestamp <= maxDataAge, "Data too old");
        
        QualityScore storage qualityScore = qualityScores[resourceId];
        
        // Check cooldown
        require(
            block.timestamp >= qualityScore.timestamp + updateCooldown,
            "Update cooldown not met"
        );
        
        // Update quality score
        qualityScore.score = score;
        qualityScore.timestamp = block.timestamp;
        
        emit QualityScoreUpdated(resourceId, score, block.timestamp, msg.sender);
    }
    
    /**
     * @notice Get oracle feed data
     * @param resourceId The resource identifier
     * @param measurementType The measurement type
     * @return feed The oracle feed data
     */
    function getFeed(
        string memory resourceId,
        MeasurementType measurementType
    ) external view returns (OracleFeed memory) {
        return feeds[resourceId][measurementType];
    }
    
    /**
     * @notice Get quality score for a resource
     * @param resourceId The resource identifier
     * @return score The quality score
     * @return timestamp The last update timestamp
     */
    function getQualityScore(
        string memory resourceId
    ) external view returns (uint256 score, uint256 timestamp) {
        QualityScore storage qualityScore = qualityScores[resourceId];
        return (qualityScore.score, qualityScore.timestamp);
    }
    
    /**
     * @notice Check if data is fresh
     * @param timestamp The data timestamp
     * @return bool Whether the data is fresh
     */
    function isDataFresh(uint256 timestamp) public view returns (bool) {
        return block.timestamp - timestamp <= maxDataAge;
    }
    
    /**
     * @notice Add a trusted oracle
     * @param oracle The oracle address
     */
    function addOracle(address oracle) external onlyRole(ADMIN_ROLE) {
        require(oracle != address(0), "Invalid oracle address");
        require(!trustedOracles[oracle], "Oracle already added");
        
        trustedOracles[oracle] = true;
        _grantRole(ORACLE_ROLE, oracle);
        
        emit OracleAdded(oracle);
    }
    
    /**
     * @notice Remove a trusted oracle
     * @param oracle The oracle address
     */
    function removeOracle(address oracle) external onlyRole(ADMIN_ROLE) {
        require(trustedOracles[oracle], "Oracle not found");
        
        trustedOracles[oracle] = false;
        _revokeRole(ORACLE_ROLE, oracle);
        
        emit OracleRemoved(oracle);
    }
    
    /**
     * @notice Update oracle configuration
     * @param _maxDataAge Maximum age of data in seconds
     * @param _minConfidence Minimum confidence score
     * @param _updateCooldown Minimum time between updates
     */
    function updateConfiguration(
        uint256 _maxDataAge,
        uint256 _minConfidence,
        uint256 _updateCooldown
    ) external onlyRole(ADMIN_ROLE) {
        require(_maxDataAge > 0, "Invalid max data age");
        require(_minConfidence <= 100, "Invalid min confidence");
        require(_updateCooldown > 0, "Invalid update cooldown");
        
        maxDataAge = _maxDataAge;
        minConfidence = _minConfidence;
        updateCooldown = _updateCooldown;
        
        emit ConfigurationUpdated(_maxDataAge, _minConfidence, _updateCooldown);
    }
    
    /**
     * @notice Pause oracle updates
     */
    function pause() external onlyRole(ADMIN_ROLE) {
        _pause();
    }
    
    /**
     * @notice Unpause oracle updates
     */
    function unpause() external onlyRole(ADMIN_ROLE) {
        _unpause();
    }
    
    /**
     * @notice Aggregate multiple oracle feeds
     * @param resourceId The resource identifier
     * @param measurementType The measurement type
     * @param oracles Array of oracle addresses
     * @return aggregatedValue The aggregated value
     * @return aggregatedConfidence The aggregated confidence
     */
    function aggregateFeeds(
        string memory resourceId,
        MeasurementType measurementType,
        address[] memory oracles
    ) external view returns (uint256 aggregatedValue, uint256 aggregatedConfidence) {
        require(oracles.length > 0, "No oracles provided");
        
        uint256 totalValue = 0;
        uint256 totalConfidence = 0;
        uint256 validFeeds = 0;
        
        for (uint256 i = 0; i < oracles.length; i++) {
            if (trustedOracles[oracles[i]]) {
                OracleFeed memory feed = feeds[resourceId][measurementType];
                
                if (feed.oracle == oracles[i] && isDataFresh(feed.timestamp)) {
                    totalValue += feed.value * feed.confidence;
                    totalConfidence += feed.confidence;
                    validFeeds++;
                }
            }
        }
        
        require(validFeeds > 0, "No valid feeds found");
        
        aggregatedValue = totalValue / totalConfidence;
        aggregatedConfidence = totalConfidence / validFeeds;
        
        return (aggregatedValue, aggregatedConfidence);
    }
} 