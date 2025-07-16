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
 */
contract ReputationOracle is Ownable {
    using SafeCast for uint256;

    struct Checkpoint {
        uint32 fromBlock;
        uint256 score;
    }

    // Mapping from an account to a list of checkpoints
    mapping(address => Checkpoint[]) public checkpoints;

    // The EIP-712 typehash for the delegation struct used in the permit signature
    bytes32 public constant REPUTATION_TYPEHASH =
        keccak256("Reputation(address user,uint256 score,uint256 nonce)");
        
    event ReputationUpdated(address indexed entity, uint256 newScore);

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
} 