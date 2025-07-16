// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/utils/math/SafeCast.sol";
import "@openzeppelin/contracts/utils/math/Math.sol";

/**
 * @title ReputationOracle
 * @dev This contract provides a way to store and retrieve reputation scores for
 * user accounts, with support for historical lookups (snapshots). This is
 * crucial for governance, as it allows querying a user's voting power at the
 * time a proposal was created, preventing exploits where users could manipulate
 * their reputation score after a proposal is made.
 *
 * The logic is heavily inspired by OpenZeppelin's `Votes.sol`.
 * Enhanced to support multi-dimensional reputation scores.
 */
contract ReputationOracle is Ownable {
    using SafeCast for uint256;

    struct Checkpoint {
        uint32 fromBlock;
        uint256 score;
    }

    struct MultiDimensionalScore {
        uint256 technicalProwess;      // 0-100
        uint256 collaborationRating;    // 0-100
        uint256 governanceInfluence;    // 0-100
        uint256 creativityIndex;        // 0-100
        uint256 reliabilityScore;       // 0-100
        uint256 totalScore;             // Weighted average
        uint32 lastUpdated;
    }

    // Reputation dimensions
    enum ReputationDimension {
        TECHNICAL,
        COLLABORATION,
        GOVERNANCE,
        CREATIVITY,
        RELIABILITY
    }

    // Mapping from an account to a list of checkpoints
    mapping(address => Checkpoint[]) public checkpoints;
    
    // Multi-dimensional scores
    mapping(address => MultiDimensionalScore) public multiDimScores;
    mapping(address => mapping(ReputationDimension => Checkpoint[])) public dimensionCheckpoints;
    
    // VC-based trust score tracking
    mapping(address => bytes32) public trustScoreVCHashes;
    mapping(address => uint256) public trustScoreExpiry;
    mapping(address => bool) public authorizedIssuers;
    
    // Dimension weights for calculating total score (basis points)
    mapping(ReputationDimension => uint256) public dimensionWeights;

    // The EIP-712 typehash for the delegation struct used in the permit signature
    bytes32 public constant REPUTATION_TYPEHASH =
        keccak256("Reputation(address user,uint256 score,uint256 nonce)");
        
    event ReputationUpdated(address indexed entity, uint256 newScore);
    event TrustScoreVCAnchored(
        address indexed entity,
        bytes32 vcHash,
        uint256 score,
        uint256 expiry
    );
    event MultiDimensionalScoreUpdated(
        address indexed entity,
        ReputationDimension dimension,
        uint256 score
    );
    event DimensionWeightUpdated(
        ReputationDimension dimension,
        uint256 weight
    );

    constructor() {
        // Set default dimension weights (total 10000 basis points)
        dimensionWeights[ReputationDimension.TECHNICAL] = 2500;      // 25%
        dimensionWeights[ReputationDimension.COLLABORATION] = 2000;  // 20%
        dimensionWeights[ReputationDimension.GOVERNANCE] = 2000;     // 20%
        dimensionWeights[ReputationDimension.CREATIVITY] = 1500;     // 15%
        dimensionWeights[ReputationDimension.RELIABILITY] = 2000;    // 20%
    }

    /**
     * @dev Sets the reputation score for an entity. This function should be
     * protected by access control, allowing only trusted services to update scores.
     * It creates a new checkpoint to record the score at the current block.
     */
    function setReputation(address entity, uint256 score) public onlyOwner {
        _writeCheckpoint(entity, score);
        emit ReputationUpdated(entity, score);
    }

    /**
     * @dev Gets the reputation of an account at a specific block number.
     * @param account The address of the account.
     * @param blockNumber The block number to get the reputation at.
     * @return The reputation score at the given block number.
     */
    function getPastReputation(address account, uint256 blockNumber) public view returns (uint256) {
        require(blockNumber < block.number, "ReputationOracle: block not yet mined");

        Checkpoint[] storage accountCheckpoints = checkpoints[account];
        if (accountCheckpoints.length == 0) {
            return 0;
        }

        // Find the checkpoint using binary search
        uint256 lower = 0;
        uint256 upper = accountCheckpoints.length - 1;
        uint256 mid;
        while (lower < upper) {
            mid = Math.average(lower, upper);
            if (accountCheckpoints[mid].fromBlock > blockNumber) {
                upper = mid;
            } else {
                lower = mid + 1;
            }
        }

        if (accountCheckpoints[lower].fromBlock > blockNumber) {
            if (lower == 0) {
                return 0;
            } else {
                lower--;
            }
        }
        
        return accountCheckpoints[lower].score;
    }

    /**
     * @dev Gets the current reputation of an account.
     * @param account The address of the account.
     * @return The current reputation score.
     */
    function getReputation(address account) public view returns (uint256) {
        Checkpoint[] storage accountCheckpoints = checkpoints[account];
        return accountCheckpoints.length == 0 ? 0 : accountCheckpoints[accountCheckpoints.length - 1].score;
    }

    function _writeCheckpoint(address account, uint256 newScore) internal {
        uint32 blockNumber = block.number.toUint32();
        Checkpoint[] storage accountCheckpoints = checkpoints[account];
        uint256 nCheckpoints = accountCheckpoints.length;

        if (nCheckpoints > 0 && accountCheckpoints[nCheckpoints - 1].fromBlock == blockNumber) {
            // If the last checkpoint is at the same block, update it
            accountCheckpoints[nCheckpoints - 1].score = newScore;
        } else {
            // Otherwise, add a new checkpoint
            accountCheckpoints.push(Checkpoint({fromBlock: blockNumber, score: newScore}));
        }
    }

    /**
     * @dev Set an issuer as authorized to update trust scores
     */
    function setAuthorizedIssuer(address issuer, bool authorized) public onlyOwner {
        authorizedIssuers[issuer] = authorized;
    }

    /**
     * @dev Update trust score with VC verification
     * @param entity The address of the entity
     * @param score The new trust score (0-100)
     * @param vcHash Hash of the trust score VC
     * @param expiry Expiry timestamp of the VC
     * @param signature Signature from authorized VC issuer
     */
    function updateTrustScoreWithVC(
        address entity,
        uint256 score,
        bytes32 vcHash,
        uint256 expiry,
        bytes calldata signature
    ) external {
        // Verify signature (simplified - in production use proper ECDSA recovery)
        require(authorizedIssuers[msg.sender], "Unauthorized issuer");
        require(score <= 100, "Score must be <= 100");
        require(expiry > block.timestamp, "VC already expired");
        
        // Update reputation
        _writeCheckpoint(entity, score);
        
        // Store VC reference
        trustScoreVCHashes[entity] = vcHash;
        trustScoreExpiry[entity] = expiry;
        
        emit ReputationUpdated(entity, score);
        emit TrustScoreVCAnchored(entity, vcHash, score, expiry);
    }

    /**
     * @dev Get trust score with VC validation
     */
    function getTrustScore(address entity) public view returns (
        uint256 score,
        bytes32 vcHash,
        uint256 expiry,
        bool isValid
    ) {
        score = getReputation(entity);
        vcHash = trustScoreVCHashes[entity];
        expiry = trustScoreExpiry[entity];
        isValid = expiry > block.timestamp;
    }

    /**
     * @dev Check if an entity has a valid trust score VC
     */
    function hasValidTrustScore(address entity, uint256 minScore) public view returns (bool) {
        (uint256 score, , uint256 expiry, ) = getTrustScore(entity);
        return score >= minScore && expiry > block.timestamp;
    }

    /**
     * @dev Update a specific reputation dimension
     */
    function updateDimensionScore(
        address entity,
        ReputationDimension dimension,
        uint256 score
    ) external {
        require(authorizedIssuers[msg.sender], "Unauthorized issuer");
        require(score <= 100, "Score must be <= 100");
        
        // Update dimension score
        MultiDimensionalScore storage mds = multiDimScores[entity];
        
        if (dimension == ReputationDimension.TECHNICAL) {
            mds.technicalProwess = score;
        } else if (dimension == ReputationDimension.COLLABORATION) {
            mds.collaborationRating = score;
        } else if (dimension == ReputationDimension.GOVERNANCE) {
            mds.governanceInfluence = score;
        } else if (dimension == ReputationDimension.CREATIVITY) {
            mds.creativityIndex = score;
        } else if (dimension == ReputationDimension.RELIABILITY) {
            mds.reliabilityScore = score;
        }
        
        mds.lastUpdated = uint32(block.number);
        
        // Update dimension checkpoint
        _writeDimensionCheckpoint(entity, dimension, score);
        
        // Recalculate total score
        uint256 totalScore = _calculateTotalScore(entity);
        mds.totalScore = totalScore;
        
        // Update main reputation score
        _writeCheckpoint(entity, totalScore);
        
        emit MultiDimensionalScoreUpdated(entity, dimension, score);
        emit ReputationUpdated(entity, totalScore);
    }

    /**
     * @dev Batch update multiple dimension scores
     */
    function batchUpdateDimensionScores(
        address entity,
        ReputationDimension[] calldata dimensions,
        uint256[] calldata scores
    ) external {
        require(authorizedIssuers[msg.sender], "Unauthorized issuer");
        require(dimensions.length == scores.length, "Array length mismatch");
        
        MultiDimensionalScore storage mds = multiDimScores[entity];
        
        for (uint i = 0; i < dimensions.length; i++) {
            require(scores[i] <= 100, "Score must be <= 100");
            
            ReputationDimension dim = dimensions[i];
            uint256 score = scores[i];
            
            if (dim == ReputationDimension.TECHNICAL) {
                mds.technicalProwess = score;
            } else if (dim == ReputationDimension.COLLABORATION) {
                mds.collaborationRating = score;
            } else if (dim == ReputationDimension.GOVERNANCE) {
                mds.governanceInfluence = score;
            } else if (dim == ReputationDimension.CREATIVITY) {
                mds.creativityIndex = score;
            } else if (dim == ReputationDimension.RELIABILITY) {
                mds.reliabilityScore = score;
            }
            
            _writeDimensionCheckpoint(entity, dim, score);
            emit MultiDimensionalScoreUpdated(entity, dim, score);
        }
        
        mds.lastUpdated = uint32(block.number);
        
        // Recalculate total score
        uint256 totalScore = _calculateTotalScore(entity);
        mds.totalScore = totalScore;
        _writeCheckpoint(entity, totalScore);
        
        emit ReputationUpdated(entity, totalScore);
    }

    /**
     * @dev Get multi-dimensional scores for an entity
     */
    function getMultiDimensionalScores(address entity) 
        external 
        view 
        returns (MultiDimensionalScore memory) 
    {
        return multiDimScores[entity];
    }

    /**
     * @dev Get score for a specific dimension at a specific block
     */
    function getDimensionScoreAt(
        address entity,
        ReputationDimension dimension,
        uint256 blockNumber
    ) external view returns (uint256) {
        Checkpoint[] storage dimCheckpoints = dimensionCheckpoints[entity][dimension];
        
        if (dimCheckpoints.length == 0) {
            return 0;
        }

        // Binary search
        uint256 low = 0;
        uint256 high = dimCheckpoints.length;

        while (low < high) {
            uint256 mid = Math.average(low, high);
            if (dimCheckpoints[mid].fromBlock > blockNumber) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }

        return low == 0 ? 0 : dimCheckpoints[low - 1].score;
    }

    /**
     * @dev Update dimension weights (governance function)
     */
    function updateDimensionWeights(
        ReputationDimension[] calldata dimensions,
        uint256[] calldata weights
    ) external onlyOwner {
        require(dimensions.length == weights.length, "Array length mismatch");
        
        uint256 totalWeight = 0;
        
        // Update weights and calculate total
        for (uint i = 0; i < dimensions.length; i++) {
            dimensionWeights[dimensions[i]] = weights[i];
            totalWeight += weights[i];
            emit DimensionWeightUpdated(dimensions[i], weights[i]);
        }
        
        // Ensure total weight equals 10000 (100%)
        require(totalWeight == 10000, "Total weight must equal 10000");
    }

    /**
     * @dev Calculate weighted total score
     */
    function _calculateTotalScore(address entity) internal view returns (uint256) {
        MultiDimensionalScore memory mds = multiDimScores[entity];
        
        uint256 weightedSum = 
            (mds.technicalProwess * dimensionWeights[ReputationDimension.TECHNICAL]) +
            (mds.collaborationRating * dimensionWeights[ReputationDimension.COLLABORATION]) +
            (mds.governanceInfluence * dimensionWeights[ReputationDimension.GOVERNANCE]) +
            (mds.creativityIndex * dimensionWeights[ReputationDimension.CREATIVITY]) +
            (mds.reliabilityScore * dimensionWeights[ReputationDimension.RELIABILITY]);
            
        return weightedSum / 10000; // Divide by total basis points
    }

    /**
     * @dev Write checkpoint for a specific dimension
     */
    function _writeDimensionCheckpoint(
        address account,
        ReputationDimension dimension,
        uint256 newScore
    ) internal {
        uint32 blockNumber = block.number.toUint32();
        Checkpoint[] storage dimCheckpoints = dimensionCheckpoints[account][dimension];
        uint256 nCheckpoints = dimCheckpoints.length;

        if (nCheckpoints > 0 && dimCheckpoints[nCheckpoints - 1].fromBlock == blockNumber) {
            dimCheckpoints[nCheckpoints - 1].score = newScore;
        } else {
            dimCheckpoints.push(Checkpoint({fromBlock: blockNumber, score: newScore}));
        }
    }

    /**
     * @dev Get reputation score for a specific dimension and proposal context
     */
    function getReputationForProposal(
        address entity,
        ReputationDimension dimension,
        uint256 proposalBlock
    ) external view returns (uint256) {
        Checkpoint[] storage dimCheckpoints = dimensionCheckpoints[entity][dimension];
        
        if (dimCheckpoints.length == 0) {
            return 0;
        }

        // Use the dimension score at proposal creation time
        uint256 idx = _findCheckpointIndex(dimCheckpoints, proposalBlock);
        return idx == 0 ? 0 : dimCheckpoints[idx - 1].score;
    }

    /**
     * @dev Find checkpoint index for a given block
     */
    function _findCheckpointIndex(
        Checkpoint[] storage ckpts,
        uint256 blockNumber
    ) internal view returns (uint256) {
        uint256 low = 0;
        uint256 high = ckpts.length;

        while (low < high) {
            uint256 mid = Math.average(low, high);
            if (ckpts[mid].fromBlock > blockNumber) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }

        return low;
    }
} 