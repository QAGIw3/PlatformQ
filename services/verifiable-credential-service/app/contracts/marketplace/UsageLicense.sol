// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/utils/Counters.sol";
import "../CredentialRegistry.sol";
import "./PlatformAsset.sol";

/**
 * @title UsageLicense
 * @dev Manages temporary, usage-based access licenses for digital assets
 */
contract UsageLicense is Ownable, ReentrancyGuard {
    using Counters for Counters.Counter;

    struct License {
        uint256 licenseId;
        string assetId;
        address licensee;
        address licensor;
        uint256 price;
        uint256 startTime;
        uint256 duration; // In seconds
        bool active;
        string licenseType; // "view", "edit", "commercial", etc.
        uint256 usageCount;
        uint256 maxUsage; // 0 = unlimited
        string metadataURI; // IPFS URI for additional terms
    }

    enum LicenseType {
        Perpetual,
        TimeBased,
        UsageBased,
        Subscription
    }

    struct LicenseOffer {
        string assetId;
        address seller;
        uint256 price;
        uint256 duration;
        string licenseType;
        uint256 maxUsage;
        bool active;
        uint256 royaltyPercentage; // Basis points
    }

    // State variables
    Counters.Counter private _licenseIdCounter;
    CredentialRegistry public credentialRegistry;
    PlatformAsset public platformAsset;
    
    mapping(uint256 => License) public licenses;
    mapping(string => LicenseOffer[]) public assetOffers;
    mapping(address => uint256[]) public userLicenses;
    mapping(string => mapping(address => uint256[])) public assetUserLicenses;
    
    uint256 public platformFeePercentage = 250; // 2.5%
    address payable public feeRecipient;

    // Events
    event LicenseOfferCreated(
        string indexed assetId,
        address indexed seller,
        uint256 price,
        uint256 duration,
        string licenseType
    );

    event LicensePurchased(
        uint256 indexed licenseId,
        string indexed assetId,
        address indexed licensee,
        address licensor,
        uint256 price,
        LicenseType licenseType
    );

    event LicenseUsed(
        uint256 indexed licenseId,
        uint256 remainingUsage
    );

    event LicenseRevoked(
        uint256 indexed licenseId,
        address revokedBy
    );

    event PlatformFeeUpdated(
        uint256 newPercentage
    );

    modifier onlyLicenseOwner(uint256 licenseId) {
        require(licenses[licenseId].licensee == msg.sender, "Not license owner");
        _;
    }

    modifier licenseExists(uint256 licenseId) {
        require(licenses[licenseId].startTime > 0, "License does not exist");
        _;
    }

    constructor(address credentialRegistryAddress, address feeRecipientAddress, address platformAssetAddress) {
        credentialRegistry = CredentialRegistry(credentialRegistryAddress);
        feeRecipient = payable(feeRecipientAddress);
        platformAsset = PlatformAsset(platformAssetAddress);
    }

    function createLicenseOffer(
        string memory assetId,
        uint256 price,
        uint256 duration,
        string memory licenseType,
        uint256 maxUsage,
        uint256 royaltyPercentage
    ) public nonReentrant {
        // We can add a check here to ensure that the msg.sender is the owner of the asset NFT
        // require(platformAsset.ownerOf(assetId) == msg.sender, "Only asset owner can create license offer");
        
        assetOffers[assetId].push(LicenseOffer({
            assetId: assetId,
            seller: msg.sender,
            price: price,
            duration: duration,
            licenseType: licenseType,
            maxUsage: maxUsage,
            active: true,
            royaltyPercentage: royaltyPercentage
        }));

        emit LicenseOfferCreated(assetId, msg.sender, price, duration, licenseType);
    }

    function purchaseLicense(string memory assetId, uint256 offerIndex, LicenseType licenseType) public payable nonReentrant {
        require(offerIndex < assetOffers[assetId].length, "Offer does not exist");
        LicenseOffer storage offer = assetOffers[assetId][offerIndex];
        require(offer.active, "Offer is not active");
        
        uint256 requiredPayment = offer.price;
        if (licenseType == LicenseType.Subscription) {
            // For subscriptions, price is monthly, require first month payment
            requiredPayment = offer.price;
        }
        
        require(msg.value >= requiredPayment, "Insufficient payment for license");

        uint256 platformFee = (msg.value * platformFeePercentage) / 10000;
        uint256 paymentToSeller = msg.value - platformFee;

        (bool success, ) = feeRecipient.call{value: platformFee}("");
        require(success, "Platform fee transfer failed");

        (success, ) = payable(offer.seller).call{value: paymentToSeller}("");
        require(success, "Payment to seller failed");

        _createLicense(assetId, msg.sender, offer, licenseType);

        if (msg.value > offer.price) {
            (success, ) = payable(msg.sender).call{value: msg.value - offer.price}("");
            require(success, "Refund failed");
        }
    }

    function _createLicense(string memory assetId, address licensee, LicenseOffer memory offer, LicenseType licenseType) internal {
        uint256 licenseId = _licenseIdCounter.current();
        _licenseIdCounter.increment();

        licenses[licenseId] = License({
            licenseId: licenseId,
            assetId: assetId,
            licensee: licensee,
            licensor: offer.seller,
            price: offer.price,
            startTime: block.timestamp,
            duration: offer.duration,
            active: true,
            licenseType: offer.licenseType,
            usageCount: 0,
            maxUsage: offer.maxUsage,
            metadataURI: "" // Can be set later
        });

        userLicenses[licensee].push(licenseId);
        assetUserLicenses[assetId][licensee].push(licenseId);

        emit LicensePurchased(licenseId, assetId, licensee, offer.seller, offer.price, licenseType);
    }

    function isLicenseActive(uint256 licenseId) public view returns (bool) {
        License storage license = licenses[licenseId];
        if (!license.active) {
            return false;
        }

        if (license.duration > 0 && block.timestamp > license.startTime + license.duration) {
            return false;
        }

        if (license.maxUsage > 0 && license.usageCount >= license.maxUsage) {
            return false;
        }

        return true;
    }

    function recordUsage(uint256 licenseId) public {
        // Add a check to ensure only authorized callers can record usage
        License storage license = licenses[licenseId];
        require(isLicenseActive(licenseId), "License is not active");
        license.usageCount++;
    }

    /**
     * @dev Revoke a license (only by licensor)
     */
    function revokeLicense(uint256 licenseId, string memory reason) 
        external 
        licenseExists(licenseId)
    {
        License storage license = licenses[licenseId];
        require(
            msg.sender == license.licensor || msg.sender == owner(),
            "Not authorized to revoke"
        );
        
        license.active = false;
        
        // TODO: Store revocation reason in a mapping if needed
        
        emit LicenseRevoked(licenseId, msg.sender);
    }

    /**
     * @dev Get all licenses for a user
     */
    function getUserLicenses(address user) 
        external 
        view 
        returns (uint256[] memory) 
    {
        return userLicenses[user];
    }

    /**
     * @dev Get active licenses for an asset and user
     */
    function getActiveUserLicensesForAsset(string memory assetId, address user)
        external
        view
        returns (uint256[] memory)
    {
        uint256[] memory allLicenses = assetUserLicenses[assetId][user];
        uint256 activeCount = 0;
        
        // Count active licenses
        for (uint i = 0; i < allLicenses.length; i++) {
            if (isLicenseActive(allLicenses[i])) {
                activeCount++;
            }
        }
        
        // Create array of active licenses
        uint256[] memory activeLicenses = new uint256[](activeCount);
        uint256 currentIndex = 0;
        
        for (uint i = 0; i < allLicenses.length; i++) {
            if (isLicenseActive(allLicenses[i])) {
                activeLicenses[currentIndex] = allLicenses[i];
                currentIndex++;
            }
        }
        
        return activeLicenses;
    }

    /**
     * @dev Get all offers for an asset
     */
    function getAssetOffers(string memory assetId)
        external
        view
        returns (LicenseOffer[] memory)
    {
        return assetOffers[assetId];
    }

    /**
     * @dev Update platform fee (governance function)
     */
    function updatePlatformFee(uint256 newPercentage) external onlyOwner {
        require(newPercentage <= 1000, "Fee too high"); // Max 10%
        platformFeePercentage = newPercentage;
        emit PlatformFeeUpdated(newPercentage);
    }

    /**
     * @dev Update fee recipient
     */
    function updateFeeRecipient(address payable newRecipient) external onlyOwner {
        require(newRecipient != address(0), "Invalid recipient");
        feeRecipient = newRecipient;
    }

    /**
     * @dev Deactivate an offer
     */
    function deactivateOffer(string memory assetId, uint256 offerIndex) external {
        require(offerIndex < assetOffers[assetId].length, "Invalid offer");
        require(
            assetOffers[assetId][offerIndex].seller == msg.sender,
            "Not offer owner"
        );
        
        assetOffers[assetId][offerIndex].active = false;
    }

    /**
     * @dev Internal function to distribute payment
     */
    function _distributePayment(
        address seller,
        uint256 amount,
        uint256 royaltyPercentage
    ) internal {
        uint256 platformFee = (amount * platformFeePercentage) / 10000;
        uint256 sellerAmount = amount - platformFee;

        // Send platform fee
        if (platformFee > 0 && feeRecipient != address(0)) {
            (bool feeSuccess, ) = feeRecipient.call{value: platformFee}("");
            require(feeSuccess, "Platform fee transfer failed");
        }

        // Send to seller
        (bool sellerSuccess, ) = seller.call{value: sellerAmount}("");
        require(sellerSuccess, "Seller payment failed");
    }

    receive() external payable {
        // Accept payments
    }
} 