// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/access/Ownable.sol";

contract RevenueSplitter is ReentrancyGuard, Ownable {
    
    struct RevenueShare {
        address payable recipient;
        uint256 sharePercentage; // Basis points (10000 = 100%)
    }
    
    struct RevenueSplit {
        uint256 splitId;
        string name;
        address creator;
        RevenueShare[] shares;
        uint256 totalRevenue;
        mapping(address => uint256) pendingWithdrawals;
        bool isActive;
        uint256 minPaymentThreshold;
    }
    
    mapping(uint256 => RevenueSplit) public revenueSplits;
    mapping(address => uint256[]) public userSplits;
    mapping(address => mapping(uint256 => uint256)) public userPendingTotal;
    
    uint256 public nextSplitId;
    uint256 public platformFeePercentage = 100; // 1%
    address payable public feeRecipient;
    
    event SplitCreated(uint256 indexed splitId, string name, address indexed creator);
    event RevenueReceived(uint256 indexed splitId, uint256 amount);
    event RevenueWithdrawn(uint256 indexed splitId, address indexed recipient, uint256 amount);
    event SplitUpdated(uint256 indexed splitId);
    event SplitDeactivated(uint256 indexed splitId);
    
    constructor(address payable _feeRecipient) {
        feeRecipient = _feeRecipient;
    }
    
    function createRevenueSplit(
        string memory name,
        address[] memory recipients,
        uint256[] memory sharePercentages,
        uint256 minPaymentThreshold
    ) external returns (uint256) {
        require(recipients.length > 0, "No recipients");
        require(recipients.length == sharePercentages.length, "Mismatched arrays");
        require(recipients.length <= 10, "Too many recipients");
        
        uint256 totalPercentage = 0;
        for (uint i = 0; i < sharePercentages.length; i++) {
            require(recipients[i] != address(0), "Invalid recipient");
            require(sharePercentages[i] > 0, "Invalid share");
            totalPercentage += sharePercentages[i];
        }
        require(totalPercentage == 10000, "Shares must total 100%");
        
        uint256 splitId = nextSplitId++;
        
        RevenueSplit storage newSplit = revenueSplits[splitId];
        newSplit.splitId = splitId;
        newSplit.name = name;
        newSplit.creator = msg.sender;
        newSplit.totalRevenue = 0;
        newSplit.isActive = true;
        newSplit.minPaymentThreshold = minPaymentThreshold;
        
        for (uint i = 0; i < recipients.length; i++) {
            newSplit.shares.push(RevenueShare({
                recipient: payable(recipients[i]),
                sharePercentage: sharePercentages[i]
            }));
            userSplits[recipients[i]].push(splitId);
        }
        
        userSplits[msg.sender].push(splitId);
        
        emit SplitCreated(splitId, name, msg.sender);
        
        return splitId;
    }
    
    function distributeRevenue(uint256 splitId) external payable nonReentrant {
        RevenueSplit storage split = revenueSplits[splitId];
        require(split.isActive, "Split not active");
        require(msg.value > 0, "No revenue to distribute");
        
        uint256 platformFee = (msg.value * platformFeePercentage) / 10000;
        uint256 distributableAmount = msg.value - platformFee;
        
        split.totalRevenue += msg.value;
        
        // Send platform fee
        if (platformFee > 0) {
            (bool success, ) = feeRecipient.call{value: platformFee}("");
            require(success, "Platform fee failed");
        }
        
        // Distribute to recipients
        for (uint i = 0; i < split.shares.length; i++) {
            uint256 recipientShare = (distributableAmount * split.shares[i].sharePercentage) / 10000;
            split.pendingWithdrawals[split.shares[i].recipient] += recipientShare;
            userPendingTotal[split.shares[i].recipient][splitId] += recipientShare;
        }
        
        emit RevenueReceived(splitId, msg.value);
    }
    
    function withdrawRevenue(uint256 splitId) external nonReentrant {
        RevenueSplit storage split = revenueSplits[splitId];
        uint256 pending = split.pendingWithdrawals[msg.sender];
        
        require(pending > 0, "No pending revenue");
        require(pending >= split.minPaymentThreshold || !split.isActive, "Below threshold");
        
        split.pendingWithdrawals[msg.sender] = 0;
        userPendingTotal[msg.sender][splitId] = 0;
        
        (bool success, ) = msg.sender.call{value: pending}("");
        require(success, "Withdrawal failed");
        
        emit RevenueWithdrawn(splitId, msg.sender, pending);
    }
    
    function withdrawAllRevenue() external nonReentrant {
        uint256[] memory userSplitIds = userSplits[msg.sender];
        uint256 totalPending = 0;
        
        for (uint i = 0; i < userSplitIds.length; i++) {
            uint256 splitId = userSplitIds[i];
            RevenueSplit storage split = revenueSplits[splitId];
            uint256 pending = split.pendingWithdrawals[msg.sender];
            
            if (pending > 0 && (pending >= split.minPaymentThreshold || !split.isActive)) {
                split.pendingWithdrawals[msg.sender] = 0;
                userPendingTotal[msg.sender][splitId] = 0;
                totalPending += pending;
                
                emit RevenueWithdrawn(splitId, msg.sender, pending);
            }
        }
        
        require(totalPending > 0, "No withdrawable revenue");
        
        (bool success, ) = msg.sender.call{value: totalPending}("");
        require(success, "Withdrawal failed");
    }
    
    function updateSplit(
        uint256 splitId,
        address[] memory recipients,
        uint256[] memory sharePercentages
    ) external {
        RevenueSplit storage split = revenueSplits[splitId];
        require(split.creator == msg.sender, "Not split creator");
        require(split.isActive, "Split not active");
        require(recipients.length == sharePercentages.length, "Mismatched arrays");
        
        // Ensure no pending withdrawals
        for (uint i = 0; i < split.shares.length; i++) {
            require(
                split.pendingWithdrawals[split.shares[i].recipient] == 0,
                "Pending withdrawals exist"
            );
        }
        
        uint256 totalPercentage = 0;
        for (uint i = 0; i < sharePercentages.length; i++) {
            require(recipients[i] != address(0), "Invalid recipient");
            require(sharePercentages[i] > 0, "Invalid share");
            totalPercentage += sharePercentages[i];
        }
        require(totalPercentage == 10000, "Shares must total 100%");
        
        // Clear old shares
        delete split.shares;
        
        // Add new shares
        for (uint i = 0; i < recipients.length; i++) {
            split.shares.push(RevenueShare({
                recipient: payable(recipients[i]),
                sharePercentage: sharePercentages[i]
            }));
        }
        
        emit SplitUpdated(splitId);
    }
    
    function deactivateSplit(uint256 splitId) external {
        RevenueSplit storage split = revenueSplits[splitId];
        require(split.creator == msg.sender, "Not split creator");
        require(split.isActive, "Already deactivated");
        
        split.isActive = false;
        emit SplitDeactivated(splitId);
    }
    
    function getSplitShares(uint256 splitId) external view returns (
        address[] memory recipients,
        uint256[] memory percentages
    ) {
        RevenueSplit storage split = revenueSplits[splitId];
        uint256 shareCount = split.shares.length;
        
        recipients = new address[](shareCount);
        percentages = new uint256[](shareCount);
        
        for (uint i = 0; i < shareCount; i++) {
            recipients[i] = split.shares[i].recipient;
            percentages[i] = split.shares[i].sharePercentage;
        }
        
        return (recipients, percentages);
    }
    
    function getPendingRevenue(address user, uint256 splitId) external view returns (uint256) {
        return revenueSplits[splitId].pendingWithdrawals[user];
    }
    
    function getUserSplits(address user) external view returns (uint256[] memory) {
        return userSplits[user];
    }
    
    function getTotalPendingRevenue(address user) external view returns (uint256) {
        uint256[] memory userSplitIds = userSplits[user];
        uint256 total = 0;
        
        for (uint i = 0; i < userSplitIds.length; i++) {
            total += revenueSplits[userSplitIds[i]].pendingWithdrawals[user];
        }
        
        return total;
    }
} 