// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/utils/math/SafeMath.sol";

contract ReputationSystem is ReentrancyGuard, AccessControl {
    using SafeMath for uint256;
    
    bytes32 public constant VERIFIER_ROLE = keccak256("VERIFIER_ROLE");
    bytes32 public constant ARBITRATOR_ROLE = keccak256("ARBITRATOR_ROLE");
    
    struct Reputation {
        uint256 score;
        uint256 totalTransactions;
        uint256 successfulTransactions;
        uint256 disputesInitiated;
        uint256 disputesLost;
        uint256 volumeTraded;
        uint256 joinedTimestamp;
        bool isVerified;
        mapping(address => Review) receivedReviews;
        address[] reviewers;
    }
    
    struct Review {
        uint8 rating; // 1-5 stars
        string comment;
        uint256 transactionId;
        uint256 timestamp;
        bool isDisputed;
    }
    
    struct Transaction {
        uint256 transactionId;
        address buyer;
        address seller;
        uint256 amount;
        uint256 timestamp;
        TransactionStatus status;
        bool buyerReviewed;
        bool sellerReviewed;
    }
    
    struct Dispute {
        uint256 disputeId;
        uint256 transactionId;
        address initiator;
        address respondent;
        string reason;
        DisputeStatus status;
        address arbitrator;
        string resolution;
    }
    
    enum TransactionStatus {
        Pending,
        Completed,
        Disputed,
        Cancelled
    }
    
    enum DisputeStatus {
        Open,
        UnderReview,
        Resolved,
        Dismissed
    }
    
    mapping(address => Reputation) public reputations;
    mapping(uint256 => Transaction) public transactions;
    mapping(uint256 => Dispute) public disputes;
    mapping(address => uint256[]) public userTransactions;
    mapping(address => uint256[]) public userDisputes;
    
    uint256 public nextTransactionId;
    uint256 public nextDisputeId;
    
    // Reputation calculation parameters
    uint256 public constant MAX_SCORE = 10000;
    uint256 public constant BASE_SCORE = 5000;
    uint256 public constant REVIEW_WEIGHT = 100;
    uint256 public constant TRANSACTION_WEIGHT = 50;
    uint256 public constant DISPUTE_PENALTY = 200;
    uint256 public constant VERIFICATION_BONUS = 1000;
    uint256 public constant TIME_BONUS_RATE = 10; // Per month
    
    event TransactionRecorded(uint256 indexed transactionId, address indexed buyer, address indexed seller);
    event ReviewSubmitted(uint256 indexed transactionId, address indexed reviewer, address indexed reviewed, uint8 rating);
    event DisputeInitiated(uint256 indexed disputeId, uint256 indexed transactionId, address indexed initiator);
    event DisputeResolved(uint256 indexed disputeId, string resolution);
    event UserVerified(address indexed user);
    event ReputationUpdated(address indexed user, uint256 newScore);
    
    constructor() {
        _setupRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _setupRole(VERIFIER_ROLE, msg.sender);
        _setupRole(ARBITRATOR_ROLE, msg.sender);
    }
    
    function recordTransaction(
        address buyer,
        address seller,
        uint256 amount
    ) external returns (uint256) {
        require(buyer != address(0) && seller != address(0), "Invalid addresses");
        require(buyer != seller, "Cannot transact with self");
        
        uint256 transactionId = nextTransactionId++;
        
        transactions[transactionId] = Transaction({
            transactionId: transactionId,
            buyer: buyer,
            seller: seller,
            amount: amount,
            timestamp: block.timestamp,
            status: TransactionStatus.Pending,
            buyerReviewed: false,
            sellerReviewed: false
        });
        
        userTransactions[buyer].push(transactionId);
        userTransactions[seller].push(transactionId);
        
        // Initialize reputation if first transaction
        if (reputations[buyer].joinedTimestamp == 0) {
            reputations[buyer].joinedTimestamp = block.timestamp;
            reputations[buyer].score = BASE_SCORE;
        }
        if (reputations[seller].joinedTimestamp == 0) {
            reputations[seller].joinedTimestamp = block.timestamp;
            reputations[seller].score = BASE_SCORE;
        }
        
        reputations[buyer].totalTransactions++;
        reputations[seller].totalTransactions++;
        
        emit TransactionRecorded(transactionId, buyer, seller);
        
        return transactionId;
    }
    
    function submitReview(
        uint256 transactionId,
        uint8 rating,
        string memory comment
    ) external nonReentrant {
        Transaction storage txn = transactions[transactionId];
        require(txn.status == TransactionStatus.Completed, "Transaction not completed");
        require(rating >= 1 && rating <= 5, "Invalid rating");
        
        address reviewer = msg.sender;
        address reviewed;
        
        if (reviewer == txn.buyer) {
            require(!txn.buyerReviewed, "Already reviewed");
            txn.buyerReviewed = true;
            reviewed = txn.seller;
        } else if (reviewer == txn.seller) {
            require(!txn.sellerReviewed, "Already reviewed");
            txn.sellerReviewed = true;
            reviewed = txn.buyer;
        } else {
            revert("Not a participant");
        }
        
        // Store review
        reputations[reviewed].receivedReviews[reviewer] = Review({
            rating: rating,
            comment: comment,
            transactionId: transactionId,
            timestamp: block.timestamp,
            isDisputed: false
        });
        
        reputations[reviewed].reviewers.push(reviewer);
        
        // Update reputation score
        updateReputationScore(reviewed);
        
        emit ReviewSubmitted(transactionId, reviewer, reviewed, rating);
    }
    
    function completeTransaction(uint256 transactionId) external {
        Transaction storage txn = transactions[transactionId];
        require(
            msg.sender == txn.buyer || msg.sender == txn.seller,
            "Not a participant"
        );
        require(txn.status == TransactionStatus.Pending, "Invalid status");
        
        txn.status = TransactionStatus.Completed;
        
        reputations[txn.buyer].successfulTransactions++;
        reputations[txn.seller].successfulTransactions++;
        reputations[txn.buyer].volumeTraded = reputations[txn.buyer].volumeTraded.add(txn.amount);
        reputations[txn.seller].volumeTraded = reputations[txn.seller].volumeTraded.add(txn.amount);
        
        updateReputationScore(txn.buyer);
        updateReputationScore(txn.seller);
    }
    
    function initiateDispute(uint256 transactionId, string memory reason) external returns (uint256) {
        Transaction storage txn = transactions[transactionId];
        require(
            msg.sender == txn.buyer || msg.sender == txn.seller,
            "Not a participant"
        );
        require(
            txn.status == TransactionStatus.Pending || txn.status == TransactionStatus.Completed,
            "Cannot dispute"
        );
        
        txn.status = TransactionStatus.Disputed;
        
        address respondent = msg.sender == txn.buyer ? txn.seller : txn.buyer;
        uint256 disputeId = nextDisputeId++;
        
        disputes[disputeId] = Dispute({
            disputeId: disputeId,
            transactionId: transactionId,
            initiator: msg.sender,
            respondent: respondent,
            reason: reason,
            status: DisputeStatus.Open,
            arbitrator: address(0),
            resolution: ""
        });
        
        userDisputes[msg.sender].push(disputeId);
        userDisputes[respondent].push(disputeId);
        
        reputations[msg.sender].disputesInitiated++;
        
        emit DisputeInitiated(disputeId, transactionId, msg.sender);
        
        return disputeId;
    }
    
    function resolveDispute(
        uint256 disputeId,
        bool favorInitiator,
        string memory resolution
    ) external onlyRole(ARBITRATOR_ROLE) {
        Dispute storage dispute = disputes[disputeId];
        require(dispute.status == DisputeStatus.Open || dispute.status == DisputeStatus.UnderReview, "Invalid status");
        
        dispute.status = DisputeStatus.Resolved;
        dispute.arbitrator = msg.sender;
        dispute.resolution = resolution;
        
        address loser = favorInitiator ? dispute.respondent : dispute.initiator;
        reputations[loser].disputesLost++;
        
        // Apply reputation penalty
        updateReputationScore(dispute.initiator);
        updateReputationScore(dispute.respondent);
        
        emit DisputeResolved(disputeId, resolution);
    }
    
    function verifyUser(address user) external onlyRole(VERIFIER_ROLE) {
        require(!reputations[user].isVerified, "Already verified");
        
        reputations[user].isVerified = true;
        updateReputationScore(user);
        
        emit UserVerified(user);
    }
    
    function updateReputationScore(address user) internal {
        Reputation storage rep = reputations[user];
        
        uint256 score = BASE_SCORE;
        
        // Transaction success rate bonus
        if (rep.totalTransactions > 0) {
            uint256 successRate = rep.successfulTransactions.mul(100).div(rep.totalTransactions);
            score = score.add(successRate.mul(TRANSACTION_WEIGHT).div(100));
        }
        
        // Review score bonus
        uint256 avgRating = calculateAverageRating(user);
        if (avgRating > 0) {
            score = score.add(avgRating.mul(REVIEW_WEIGHT));
        }
        
        // Dispute penalty
        score = score.sub(rep.disputesLost.mul(DISPUTE_PENALTY));
        
        // Verification bonus
        if (rep.isVerified) {
            score = score.add(VERIFICATION_BONUS);
        }
        
        // Time bonus (longevity)
        uint256 monthsActive = (block.timestamp - rep.joinedTimestamp) / 30 days;
        score = score.add(monthsActive.mul(TIME_BONUS_RATE));
        
        // Ensure score stays within bounds
        if (score > MAX_SCORE) {
            score = MAX_SCORE;
        } else if (score < 0) {
            score = 0;
        }
        
        rep.score = score;
        
        emit ReputationUpdated(user, score);
    }
    
    function calculateAverageRating(address user) public view returns (uint256) {
        address[] memory reviewers = reputations[user].reviewers;
        
        if (reviewers.length == 0) {
            return 0;
        }
        
        uint256 totalRating = 0;
        uint256 validReviews = 0;
        
        for (uint i = 0; i < reviewers.length; i++) {
            Review memory review = reputations[user].receivedReviews[reviewers[i]];
            if (!review.isDisputed) {
                totalRating = totalRating.add(review.rating);
                validReviews++;
            }
        }
        
        if (validReviews == 0) {
            return 0;
        }
        
        return totalRating.div(validReviews);
    }
    
    function getReputationDetails(address user) external view returns (
        uint256 score,
        uint256 totalTransactions,
        uint256 successfulTransactions,
        uint256 disputesInitiated,
        uint256 disputesLost,
        bool isVerified,
        uint256 averageRating
    ) {
        Reputation storage rep = reputations[user];
        
        return (
            rep.score,
            rep.totalTransactions,
            rep.successfulTransactions,
            rep.disputesInitiated,
            rep.disputesLost,
            rep.isVerified,
            calculateAverageRating(user)
        );
    }
    
    function getUserTransactions(address user) external view returns (uint256[] memory) {
        return userTransactions[user];
    }
    
    function getUserDisputes(address user) external view returns (uint256[] memory) {
        return userDisputes[user];
    }
    
    function getReview(address reviewed, address reviewer) external view returns (
        uint8 rating,
        string memory comment,
        uint256 transactionId,
        uint256 timestamp,
        bool isDisputed
    ) {
        Review memory review = reputations[reviewed].receivedReviews[reviewer];
        return (
            review.rating,
            review.comment,
            review.transactionId,
            review.timestamp,
            review.isDisputed
        );
    }
} 