// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/utils/math/SafeMath.sol";

contract MarketplaceDAO is ReentrancyGuard {
    using SafeMath for uint256;
    
    IERC20 public governanceToken;
    
    struct Proposal {
        uint256 proposalId;
        address proposer;
        string title;
        string description;
        ProposalType proposalType;
        bytes data;
        uint256 forVotes;
        uint256 againstVotes;
        uint256 abstainVotes;
        uint256 startBlock;
        uint256 endBlock;
        ProposalStatus status;
        bool executed;
        mapping(address => bool) hasVoted;
        mapping(address => uint256) voteWeight;
    }
    
    struct FeeProposal {
        uint256 newPlatformFee;
        uint256 newListingFee;
        uint256 newTransactionFee;
    }
    
    struct PolicyProposal {
        string policyType;
        bytes policyData;
    }
    
    enum ProposalType {
        FeeChange,
        PolicyChange,
        TreasuryAllocation,
        EmergencyAction,
        ContractUpgrade
    }
    
    enum ProposalStatus {
        Pending,
        Active,
        Defeated,
        Succeeded,
        Queued,
        Executed,
        Cancelled
    }
    
    enum VoteType {
        Against,
        For,
        Abstain
    }
    
    mapping(uint256 => Proposal) public proposals;
    mapping(address => uint256) public delegatedPower;
    mapping(address => address) public delegates;
    mapping(address => uint256) public proposalCount;
    
    uint256 public nextProposalId;
    uint256 public proposalThreshold = 100000 * 10**18; // 100k tokens to propose
    uint256 public quorumPercentage = 400; // 4%
    uint256 public votingPeriod = 3 days;
    uint256 public votingDelay = 1 days;
    uint256 public executionDelay = 2 days;
    
    address public treasury;
    uint256 public currentPlatformFee = 250; // 2.5%
    uint256 public currentListingFee = 0.01 ether;
    uint256 public currentTransactionFee = 100; // 1%
    
    // Reputation system
    mapping(address => uint256) public memberReputation;
    mapping(address => uint256) public lastActivityBlock;
    
    event ProposalCreated(uint256 indexed proposalId, address indexed proposer, string title);
    event VoteCast(uint256 indexed proposalId, address indexed voter, VoteType vote, uint256 weight);
    event ProposalExecuted(uint256 indexed proposalId);
    event DelegateChanged(address indexed delegator, address indexed fromDelegate, address indexed toDelegate);
    event FeeUpdated(string feeType, uint256 oldFee, uint256 newFee);
    
    constructor(address _governanceToken, address _treasury) {
        governanceToken = IERC20(_governanceToken);
        treasury = _treasury;
    }
    
    function propose(
        string memory title,
        string memory description,
        ProposalType proposalType,
        bytes memory data
    ) external returns (uint256) {
        require(
            governanceToken.balanceOf(msg.sender) >= proposalThreshold,
            "Insufficient tokens to propose"
        );
        
        uint256 proposalId = nextProposalId++;
        
        Proposal storage proposal = proposals[proposalId];
        proposal.proposalId = proposalId;
        proposal.proposer = msg.sender;
        proposal.title = title;
        proposal.description = description;
        proposal.proposalType = proposalType;
        proposal.data = data;
        proposal.startBlock = block.number + votingDelay;
        proposal.endBlock = proposal.startBlock + votingPeriod;
        proposal.status = ProposalStatus.Pending;
        
        proposalCount[msg.sender]++;
        
        emit ProposalCreated(proposalId, msg.sender, title);
        
        return proposalId;
    }
    
    function vote(uint256 proposalId, VoteType voteType) external nonReentrant {
        Proposal storage proposal = proposals[proposalId];
        
        require(block.number >= proposal.startBlock, "Voting not started");
        require(block.number <= proposal.endBlock, "Voting ended");
        require(!proposal.hasVoted[msg.sender], "Already voted");
        
        uint256 weight = getVotingPower(msg.sender);
        require(weight > 0, "No voting power");
        
        proposal.hasVoted[msg.sender] = true;
        proposal.voteWeight[msg.sender] = weight;
        
        if (voteType == VoteType.For) {
            proposal.forVotes = proposal.forVotes.add(weight);
        } else if (voteType == VoteType.Against) {
            proposal.againstVotes = proposal.againstVotes.add(weight);
        } else {
            proposal.abstainVotes = proposal.abstainVotes.add(weight);
        }
        
        // Update activity and reputation
        lastActivityBlock[msg.sender] = block.number;
        memberReputation[msg.sender] = memberReputation[msg.sender].add(1);
        
        emit VoteCast(proposalId, msg.sender, voteType, weight);
    }
    
    function execute(uint256 proposalId) external nonReentrant {
        Proposal storage proposal = proposals[proposalId];
        
        require(proposal.status == ProposalStatus.Succeeded, "Proposal not succeeded");
        require(block.number >= proposal.endBlock + executionDelay, "Execution delay not met");
        require(!proposal.executed, "Already executed");
        
        proposal.executed = true;
        proposal.status = ProposalStatus.Executed;
        
        if (proposal.proposalType == ProposalType.FeeChange) {
            executeFeeChange(proposal.data);
        } else if (proposal.proposalType == ProposalType.PolicyChange) {
            executePolicyChange(proposal.data);
        } else if (proposal.proposalType == ProposalType.TreasuryAllocation) {
            executeTreasuryAllocation(proposal.data);
        }
        
        // Reward proposer
        memberReputation[proposal.proposer] = memberReputation[proposal.proposer].add(10);
        
        emit ProposalExecuted(proposalId);
    }
    
    function updateProposalStatus(uint256 proposalId) external {
        Proposal storage proposal = proposals[proposalId];
        
        require(block.number > proposal.endBlock, "Voting not ended");
        require(proposal.status == ProposalStatus.Pending || proposal.status == ProposalStatus.Active, "Invalid status");
        
        uint256 totalVotes = proposal.forVotes.add(proposal.againstVotes).add(proposal.abstainVotes);
        uint256 quorum = governanceToken.totalSupply().mul(quorumPercentage).div(10000);
        
        if (totalVotes < quorum) {
            proposal.status = ProposalStatus.Defeated;
        } else if (proposal.forVotes > proposal.againstVotes) {
            proposal.status = ProposalStatus.Succeeded;
        } else {
            proposal.status = ProposalStatus.Defeated;
        }
    }
    
    function delegate(address delegatee) external {
        require(delegatee != address(0), "Invalid delegatee");
        require(delegatee != msg.sender, "Cannot delegate to self");
        
        address currentDelegate = delegates[msg.sender];
        uint256 delegatorBalance = governanceToken.balanceOf(msg.sender);
        
        delegates[msg.sender] = delegatee;
        
        if (currentDelegate != address(0)) {
            delegatedPower[currentDelegate] = delegatedPower[currentDelegate].sub(delegatorBalance);
        }
        
        delegatedPower[delegatee] = delegatedPower[delegatee].add(delegatorBalance);
        
        emit DelegateChanged(msg.sender, currentDelegate, delegatee);
    }
    
    function getVotingPower(address account) public view returns (uint256) {
        uint256 balance = governanceToken.balanceOf(account);
        uint256 delegated = delegatedPower[account];
        uint256 reputation = memberReputation[account];
        
        // Voting power = balance + delegated + reputation bonus
        uint256 reputationBonus = balance.mul(reputation).div(1000); // 0.1% per reputation point
        
        return balance.add(delegated).add(reputationBonus);
    }
    
    function executeFeeChange(bytes memory data) internal {
        FeeProposal memory feeProposal = abi.decode(data, (FeeProposal));
        
        if (feeProposal.newPlatformFee > 0 && feeProposal.newPlatformFee <= 1000) {
            emit FeeUpdated("platform", currentPlatformFee, feeProposal.newPlatformFee);
            currentPlatformFee = feeProposal.newPlatformFee;
        }
        
        if (feeProposal.newListingFee > 0) {
            emit FeeUpdated("listing", currentListingFee, feeProposal.newListingFee);
            currentListingFee = feeProposal.newListingFee;
        }
        
        if (feeProposal.newTransactionFee > 0 && feeProposal.newTransactionFee <= 1000) {
            emit FeeUpdated("transaction", currentTransactionFee, feeProposal.newTransactionFee);
            currentTransactionFee = feeProposal.newTransactionFee;
        }
    }
    
    function executePolicyChange(bytes memory data) internal {
        PolicyProposal memory policyProposal = abi.decode(data, (PolicyProposal));
        
        // Implementation would update various marketplace policies
        // For example: listing requirements, prohibited content, verification standards
    }
    
    function executeTreasuryAllocation(bytes memory data) internal {
        (address recipient, uint256 amount, string memory purpose) = abi.decode(data, (address, uint256, string));
        
        require(recipient != address(0), "Invalid recipient");
        require(amount > 0, "Invalid amount");
        
        // Transfer from treasury
        (bool success, ) = treasury.call(
            abi.encodeWithSignature("allocateFunds(address,uint256,string)", recipient, amount, purpose)
        );
        require(success, "Treasury allocation failed");
    }
    
    function cancelProposal(uint256 proposalId) external {
        Proposal storage proposal = proposals[proposalId];
        
        require(msg.sender == proposal.proposer, "Not proposer");
        require(
            proposal.status == ProposalStatus.Pending || proposal.status == ProposalStatus.Active,
            "Cannot cancel"
        );
        
        proposal.status = ProposalStatus.Cancelled;
    }
    
    function getProposalDetails(uint256 proposalId) external view returns (
        address proposer,
        string memory title,
        ProposalStatus status,
        uint256 forVotes,
        uint256 againstVotes,
        uint256 abstainVotes
    ) {
        Proposal storage proposal = proposals[proposalId];
        return (
            proposal.proposer,
            proposal.title,
            proposal.status,
            proposal.forVotes,
            proposal.againstVotes,
            proposal.abstainVotes
        );
    }
    
    function hasVoted(uint256 proposalId, address account) external view returns (bool) {
        return proposals[proposalId].hasVoted[account];
    }
    
    function getMemberStats(address member) external view returns (
        uint256 reputation,
        uint256 votingPower,
        uint256 proposalsCreated,
        address delegatee
    ) {
        return (
            memberReputation[member],
            getVotingPower(member),
            proposalCount[member],
            delegates[member]
        );
    }
} 