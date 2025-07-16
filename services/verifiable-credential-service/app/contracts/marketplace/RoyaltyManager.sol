// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/utils/math/SafeMath.sol";
import "../CredentialRegistry.sol";

/**
 * @title RoyaltyManager
 * @dev Manages royalty distributions for derivative works based on asset lineage
 */
contract RoyaltyManager is Ownable, ReentrancyGuard {
    using SafeMath for uint256;

    struct RoyaltyConfig {
        uint256 percentage; // Basis points (e.g., 250 = 2.5%)
        address payable beneficiary;
        bool active;
    }

    struct AssetRoyalty {
        mapping(string => RoyaltyConfig) creators; // Original creators
        uint256 totalPercentage;
        bool exists;
    }

    struct PaymentDistribution {
        address[] recipients;
        uint256[] amounts;
        uint256 totalAmount;
        uint256 timestamp;
        string assetId;
    }

    // Constants
    uint256 public constant MAX_ROYALTY_PERCENTAGE = 5000; // 50%
    uint256 public constant BASIS_POINTS = 10000;

    // State variables
    CredentialRegistry public credentialRegistry;
    mapping(string => AssetRoyalty) public assetRoyalties;
    mapping(string => PaymentDistribution[]) public paymentHistory;
    mapping(address => uint256) public pendingWithdrawals;

    // Royalty splits for different tiers of contribution
    uint256 public originalCreatorPercentage = 1000; // 10%
    uint256 public majorContributorPercentage = 500; // 5%
    uint256 public minorContributorPercentage = 250; // 2.5%

    // Events
    event RoyaltyConfigured(
        string indexed assetId,
        string indexed creatorCredentialId,
        address beneficiary,
        uint256 percentage
    );
    
    event RoyaltyDistributed(
        string indexed assetId,
        uint256 amount,
        uint256 recipientCount
    );
    
    event WithdrawalProcessed(
        address indexed beneficiary,
        uint256 amount
    );

    event RoyaltyPercentagesUpdated(
        uint256 original,
        uint256 major,
        uint256 minor
    );

    constructor(address _credentialRegistry) {
        credentialRegistry = CredentialRegistry(_credentialRegistry);
    }

    /**
     * @dev Configure royalty for an asset based on creation credential
     */
    function configureRoyalty(
        string memory assetId,
        string memory creationCredentialId,
        address payable beneficiary,
        uint256 percentage
    ) external {
        require(percentage <= MAX_ROYALTY_PERCENTAGE, "Percentage too high");
        require(beneficiary != address(0), "Invalid beneficiary");
        
        // Verify the credential exists and is valid
        require(
            credentialRegistry.isValid(creationCredentialId),
            "Invalid creation credential"
        );

        AssetRoyalty storage royalty = assetRoyalties[assetId];
        
        // Check if adding this would exceed max total
        uint256 newTotal = royalty.totalPercentage.add(percentage);
        require(newTotal <= MAX_ROYALTY_PERCENTAGE, "Total royalty exceeds maximum");

        royalty.creators[creationCredentialId] = RoyaltyConfig({
            percentage: percentage,
            beneficiary: beneficiary,
            active: true
        });
        
        royalty.totalPercentage = newTotal;
        royalty.exists = true;

        emit RoyaltyConfigured(assetId, creationCredentialId, beneficiary, percentage);
    }

    /**
     * @dev Distribute royalties for a monetized derivative asset
     */
    function distributeRoyalties(
        string memory derivativeAssetId,
        string[] memory lineageCredentialIds
    ) external payable nonReentrant {
        require(msg.value > 0, "No payment provided");
        require(lineageCredentialIds.length > 0, "No lineage provided");

        uint256 totalDistributed = 0;
        address[] memory recipients = new address[](lineageCredentialIds.length);
        uint256[] memory amounts = new uint256[](lineageCredentialIds.length);
        uint256 recipientCount = 0;

        // Calculate distributions based on lineage
        for (uint i = 0; i < lineageCredentialIds.length; i++) {
            string memory credentialId = lineageCredentialIds[i];
            
            // Get the original asset ID from the credential
            // This would need to be implemented based on credential structure
            string memory originalAssetId = _getAssetIdFromCredential(credentialId);
            
            AssetRoyalty storage royalty = assetRoyalties[originalAssetId];
            if (royalty.exists) {
                // Distribute to all creators of this asset
                for (uint j = 0; j < lineageCredentialIds.length; j++) {
                    RoyaltyConfig memory config = royalty.creators[lineageCredentialIds[j]];
                    if (config.active && config.beneficiary != address(0)) {
                        uint256 amount = msg.value.mul(config.percentage).div(BASIS_POINTS);
                        
                        if (amount > 0) {
                            pendingWithdrawals[config.beneficiary] = 
                                pendingWithdrawals[config.beneficiary].add(amount);
                            
                            recipients[recipientCount] = config.beneficiary;
                            amounts[recipientCount] = amount;
                            recipientCount++;
                            
                            totalDistributed = totalDistributed.add(amount);
                        }
                    }
                }
            }
        }

        // Store payment history
        if (recipientCount > 0) {
            // Resize arrays to actual recipient count
            address[] memory finalRecipients = new address[](recipientCount);
            uint256[] memory finalAmounts = new uint256[](recipientCount);
            
            for (uint i = 0; i < recipientCount; i++) {
                finalRecipients[i] = recipients[i];
                finalAmounts[i] = amounts[i];
            }

            paymentHistory[derivativeAssetId].push(PaymentDistribution({
                recipients: finalRecipients,
                amounts: finalAmounts,
                totalAmount: totalDistributed,
                timestamp: block.timestamp,
                assetId: derivativeAssetId
            }));
        }

        // Send remaining funds to the derivative creator
        uint256 remaining = msg.value.sub(totalDistributed);
        if (remaining > 0) {
            pendingWithdrawals[msg.sender] = pendingWithdrawals[msg.sender].add(remaining);
        }

        emit RoyaltyDistributed(derivativeAssetId, totalDistributed, recipientCount);
    }

    /**
     * @dev Withdraw pending royalties
     */
    function withdraw() external nonReentrant {
        uint256 amount = pendingWithdrawals[msg.sender];
        require(amount > 0, "No pending withdrawals");

        pendingWithdrawals[msg.sender] = 0;
        
        (bool success, ) = msg.sender.call{value: amount}("");
        require(success, "Withdrawal failed");

        emit WithdrawalProcessed(msg.sender, amount);
    }

    /**
     * @dev Update royalty percentages (governance function)
     */
    function updateRoyaltyPercentages(
        uint256 _original,
        uint256 _major,
        uint256 _minor
    ) external onlyOwner {
        require(_original <= MAX_ROYALTY_PERCENTAGE, "Original percentage too high");
        require(_major <= MAX_ROYALTY_PERCENTAGE, "Major percentage too high");
        require(_minor <= MAX_ROYALTY_PERCENTAGE, "Minor percentage too high");

        originalCreatorPercentage = _original;
        majorContributorPercentage = _major;
        minorContributorPercentage = _minor;

        emit RoyaltyPercentagesUpdated(_original, _major, _minor);
    }

    /**
     * @dev Get payment history for an asset
     */
    function getPaymentHistory(string memory assetId) 
        external 
        view 
        returns (PaymentDistribution[] memory) 
    {
        return paymentHistory[assetId];
    }

    /**
     * @dev Check pending withdrawal amount
     */
    function getPendingWithdrawal(address account) 
        external 
        view 
        returns (uint256) 
    {
        return pendingWithdrawals[account];
    }

    /**
     * @dev Internal function to extract asset ID from credential
     * This is a placeholder - actual implementation would decode the credential
     */
    function _getAssetIdFromCredential(string memory credentialId) 
        internal 
        pure 
        returns (string memory) 
    {
        // In production, this would decode the credential and extract the asset ID
        // For now, we'll use a simple mapping approach
        return credentialId;
    }

    /**
     * @dev Emergency withdrawal for owner (safety mechanism)
     */
    function emergencyWithdraw() external onlyOwner {
        uint256 balance = address(this).balance;
        require(balance > 0, "No funds to withdraw");
        
        (bool success, ) = owner().call{value: balance}("");
        require(success, "Emergency withdrawal failed");
    }

    receive() external payable {
        // Accept payments
    }
} 