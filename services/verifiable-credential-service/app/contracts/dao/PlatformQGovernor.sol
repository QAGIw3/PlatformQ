// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "@openzeppelin/contracts/governance/Governor.sol";
import "@openzeppelin/contracts/governance/extensions/GovernorSettings.sol";
import "@openzeppelin/contracts/governance/extensions/GovernorCountingSimple.sol";
import "./ReputationOracle.sol";

/**
 * @title PlatformQGovernor
 * @dev A custom governor for the PlatformQ DAO that uses a reputation-based
 * voting system instead of a token-based one. Voting power is determined
 * by a user's score in the ReputationOracle.
 */
contract PlatformQGovernor is Governor, GovernorSettings, GovernorCountingSimple {
    ReputationOracle public immutable reputationOracle;
    uint256 private _quorum;

    // solhint-disable-next-line func-name-mixedcase
    function CLOCK_MODE() public pure override returns (string memory) {
        // blocknumber is the default clock mode. This can be changed to "timestamp"
        // if the platform requires it, but the ReputationOracle would also need
        // to support timestamp-based snapshots.
        return "mode=blocknumber";
    }
    
    constructor(
        string memory name,
        ReputationOracle _reputationOracle,
        uint256 initialVotingDelay,
        uint256 initialVotingPeriod,
        uint256 initialProposalThreshold,
        uint256 initialQuorum
    )
        Governor(name)
        GovernorSettings(
            initialVotingDelay,
            initialVotingPeriod,
            initialProposalThreshold
        )
    {
        reputationOracle = _reputationOracle;
        _quorum = initialQuorum;
    }

    function clock() public view override returns (uint48) {
        return uint48(block.number);
    }

    function quorum(uint256 /* timepoint */) public view override returns (uint256) {
        return _quorum;
    }

    /**
     * @dev Overrides the standard `_getVotes` function to use the ReputationOracle.
     * A user's voting power is their reputation score at the time of the proposal snapshot.
     */
    function _getVotes(address account, uint256 blockNumber, bytes memory /* params */)
        internal
        view
        override
        returns (uint256)
    {
        try reputationOracle.getPastReputation(account, blockNumber) returns (uint256 votes) {
            return votes;
        } catch {
            revert("Governor: reputation oracle does not support past lookups");
        }
    }

    // The following functions are overrides required by Solidity.

    function state(uint256 proposalId)
        public
        view
        override(Governor)
        returns (ProposalState)
    {
        return super.state(proposalId);
    }

    function proposalThreshold()
        public
        view
        override(Governor, GovernorSettings)
        returns (uint256)
    {
        return super.proposalThreshold();
    }
} 