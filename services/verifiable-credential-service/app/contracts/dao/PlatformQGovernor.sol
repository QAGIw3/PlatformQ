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
 * by a user's score in the ReputationOracle, with different dimensions
 * weighted based on the proposal type.
 */
contract PlatformQGovernor is Governor, GovernorSettings, GovernorCountingSimple {
    ReputationOracle public immutable reputationOracle;
    uint256 private _quorum;
    
    // Proposal categories
    enum ProposalCategory {
        TECHNICAL,      // Code changes, technical architecture
        GOVERNANCE,     // DAO rules, voting mechanisms
        TREASURY,       // Fund allocation, financial decisions
        CREATIVE,       // Creative direction, design decisions
        GENERAL         // Default category
    }
    
    // Mapping from proposal ID to category
    mapping(uint256 => ProposalCategory) public proposalCategories;
    
    // Dimension weights for each proposal category (basis points)
    mapping(ProposalCategory => mapping(ReputationOracle.ReputationDimension => uint256)) 
        public categoryDimensionWeights;

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
        
        // Initialize default weights for proposal categories
        _initializeCategoryWeights();
    }
    
    function _initializeCategoryWeights() internal {
        // Technical proposals heavily weight technical prowess
        categoryDimensionWeights[ProposalCategory.TECHNICAL][ReputationOracle.ReputationDimension.TECHNICAL] = 5000;
        categoryDimensionWeights[ProposalCategory.TECHNICAL][ReputationOracle.ReputationDimension.COLLABORATION] = 2000;
        categoryDimensionWeights[ProposalCategory.TECHNICAL][ReputationOracle.ReputationDimension.RELIABILITY] = 3000;
        
        // Governance proposals weight governance influence and collaboration
        categoryDimensionWeights[ProposalCategory.GOVERNANCE][ReputationOracle.ReputationDimension.GOVERNANCE] = 4000;
        categoryDimensionWeights[ProposalCategory.GOVERNANCE][ReputationOracle.ReputationDimension.COLLABORATION] = 3000;
        categoryDimensionWeights[ProposalCategory.GOVERNANCE][ReputationOracle.ReputationDimension.RELIABILITY] = 3000;
        
        // Treasury proposals balance all dimensions
        categoryDimensionWeights[ProposalCategory.TREASURY][ReputationOracle.ReputationDimension.GOVERNANCE] = 3000;
        categoryDimensionWeights[ProposalCategory.TREASURY][ReputationOracle.ReputationDimension.RELIABILITY] = 3000;
        categoryDimensionWeights[ProposalCategory.TREASURY][ReputationOracle.ReputationDimension.TECHNICAL] = 2000;
        categoryDimensionWeights[ProposalCategory.TREASURY][ReputationOracle.ReputationDimension.COLLABORATION] = 2000;
        
        // Creative proposals emphasize creativity and collaboration
        categoryDimensionWeights[ProposalCategory.CREATIVE][ReputationOracle.ReputationDimension.CREATIVITY] = 5000;
        categoryDimensionWeights[ProposalCategory.CREATIVE][ReputationOracle.ReputationDimension.COLLABORATION] = 3000;
        categoryDimensionWeights[ProposalCategory.CREATIVE][ReputationOracle.ReputationDimension.TECHNICAL] = 2000;
        
        // General proposals use balanced weights
        categoryDimensionWeights[ProposalCategory.GENERAL][ReputationOracle.ReputationDimension.TECHNICAL] = 2000;
        categoryDimensionWeights[ProposalCategory.GENERAL][ReputationOracle.ReputationDimension.COLLABORATION] = 2000;
        categoryDimensionWeights[ProposalCategory.GENERAL][ReputationOracle.ReputationDimension.GOVERNANCE] = 2000;
        categoryDimensionWeights[ProposalCategory.GENERAL][ReputationOracle.ReputationDimension.CREATIVITY] = 2000;
        categoryDimensionWeights[ProposalCategory.GENERAL][ReputationOracle.ReputationDimension.RELIABILITY] = 2000;
    }

    function clock() public view override returns (uint48) {
        return uint48(block.number);
    }

    function quorum(uint256 /* timepoint */) public view override returns (uint256) {
        return _quorum;
    }

    /**
     * @dev Overrides the standard `_getVotes` function to use the ReputationOracle.
     * A user's voting power is calculated based on their multi-dimensional reputation
     * weighted according to the proposal category.
     */
    function _getVotes(address account, uint256 blockNumber, bytes memory params)
        internal
        view
        override
        returns (uint256)
    {
        // Decode proposal ID from params if provided
        uint256 proposalId = params.length >= 32 ? abi.decode(params, (uint256)) : 0;
        
        if (proposalId == 0) {
            // Fallback to total reputation if no proposal ID
            try reputationOracle.getPastReputation(account, blockNumber) returns (uint256 votes) {
                return votes;
            } catch {
                revert("Governor: reputation oracle does not support past lookups");
            }
        }
        
        // Get the proposal category
        ProposalCategory category = proposalCategories[proposalId];
        
        // Calculate weighted voting power based on dimensions
        return _calculateWeightedVotingPower(account, blockNumber, category);
    }
    
    /**
     * @dev Calculate weighted voting power based on proposal category
     */
    function _calculateWeightedVotingPower(
        address account,
        uint256 blockNumber,
        ProposalCategory category
    ) internal view returns (uint256) {
        uint256 weightedPower = 0;
        
        // Get dimension scores at the proposal block
        uint256 technical = reputationOracle.getDimensionScoreAt(
            account, 
            ReputationOracle.ReputationDimension.TECHNICAL, 
            blockNumber
        );
        uint256 collaboration = reputationOracle.getDimensionScoreAt(
            account, 
            ReputationOracle.ReputationDimension.COLLABORATION, 
            blockNumber
        );
        uint256 governance = reputationOracle.getDimensionScoreAt(
            account, 
            ReputationOracle.ReputationDimension.GOVERNANCE, 
            blockNumber
        );
        uint256 creativity = reputationOracle.getDimensionScoreAt(
            account, 
            ReputationOracle.ReputationDimension.CREATIVITY, 
            blockNumber
        );
        uint256 reliability = reputationOracle.getDimensionScoreAt(
            account, 
            ReputationOracle.ReputationDimension.RELIABILITY, 
            blockNumber
        );
        
        // Apply category-specific weights
        weightedPower = (technical * categoryDimensionWeights[category][ReputationOracle.ReputationDimension.TECHNICAL]) +
                       (collaboration * categoryDimensionWeights[category][ReputationOracle.ReputationDimension.COLLABORATION]) +
                       (governance * categoryDimensionWeights[category][ReputationOracle.ReputationDimension.GOVERNANCE]) +
                       (creativity * categoryDimensionWeights[category][ReputationOracle.ReputationDimension.CREATIVITY]) +
                       (reliability * categoryDimensionWeights[category][ReputationOracle.ReputationDimension.RELIABILITY]);
        
        // Normalize by total weight (10000 basis points)
        return weightedPower / 10000;
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
    
    /**
     * @dev Create a categorized proposal
     */
    function proposeCategorized(
        address[] memory targets,
        uint256[] memory values,
        bytes[] memory calldatas,
        string memory description,
        ProposalCategory category
    ) public returns (uint256) {
        uint256 proposalId = propose(targets, values, calldatas, description);
        proposalCategories[proposalId] = category;
        return proposalId;
    }
    
    /**
     * @dev Update category weights (governance function)
     */
    function updateCategoryWeights(
        ProposalCategory category,
        uint256[5] memory weights // [technical, collaboration, governance, creativity, reliability]
    ) external onlyOwner {
        require(weights[0] + weights[1] + weights[2] + weights[3] + weights[4] == 10000, 
                "Weights must sum to 10000");
        
        categoryDimensionWeights[category][ReputationOracle.ReputationDimension.TECHNICAL] = weights[0];
        categoryDimensionWeights[category][ReputationOracle.ReputationDimension.COLLABORATION] = weights[1];
        categoryDimensionWeights[category][ReputationOracle.ReputationDimension.GOVERNANCE] = weights[2];
        categoryDimensionWeights[category][ReputationOracle.ReputationDimension.CREATIVITY] = weights[3];
        categoryDimensionWeights[category][ReputationOracle.ReputationDimension.RELIABILITY] = weights[4];
    }
    
    /**
     * @dev Get category weights for a proposal category
     */
    function getCategoryWeights(ProposalCategory category) 
        external 
        view 
        returns (uint256[5] memory) 
    {
        return [
            categoryDimensionWeights[category][ReputationOracle.ReputationDimension.TECHNICAL],
            categoryDimensionWeights[category][ReputationOracle.ReputationDimension.COLLABORATION],
            categoryDimensionWeights[category][ReputationOracle.ReputationDimension.GOVERNANCE],
            categoryDimensionWeights[category][ReputationOracle.ReputationDimension.CREATIVITY],
            categoryDimensionWeights[category][ReputationOracle.ReputationDimension.RELIABILITY]
        ];
    }
} 