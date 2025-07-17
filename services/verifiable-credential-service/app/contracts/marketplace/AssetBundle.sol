// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC721/IERC721.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/access/Ownable.sol";

contract AssetBundle is ReentrancyGuard, Ownable {
    
    struct Bundle {
        uint256 bundleId;
        string name;
        string description;
        address creator;
        uint256[] assetTokenIds;
        address[] assetContracts;
        uint256 price;
        bool isActive;
        uint256 totalSold;
        uint256 maxSupply; // 0 for unlimited
        mapping(address => bool) purchasers;
    }
    
    struct BundleInfo {
        uint256 bundleId;
        string name;
        string description;
        address creator;
        uint256 price;
        bool isActive;
        uint256 totalSold;
        uint256 maxSupply;
    }
    
    mapping(uint256 => Bundle) public bundles;
    mapping(address => uint256[]) public userBundles;
    mapping(address => mapping(uint256 => bool)) public userOwnedBundles;
    
    uint256 public nextBundleId;
    uint256 public platformFeePercentage = 250; // 2.5%
    address payable public feeRecipient;
    
    event BundleCreated(uint256 indexed bundleId, address indexed creator, uint256 price);
    event BundlePurchased(uint256 indexed bundleId, address indexed buyer);
    event BundleDeactivated(uint256 indexed bundleId);
    event BundleUpdated(uint256 indexed bundleId, uint256 newPrice);
    
    constructor(address payable _feeRecipient) {
        feeRecipient = _feeRecipient;
    }
    
    function createBundle(
        string memory name,
        string memory description,
        uint256[] memory assetTokenIds,
        address[] memory assetContracts,
        uint256 price,
        uint256 maxSupply
    ) external nonReentrant returns (uint256) {
        require(assetTokenIds.length > 0, "Empty bundle");
        require(assetTokenIds.length == assetContracts.length, "Mismatched arrays");
        require(assetTokenIds.length <= 20, "Too many assets");
        
        // Verify ownership of all assets
        for (uint i = 0; i < assetTokenIds.length; i++) {
            IERC721 assetContract = IERC721(assetContracts[i]);
            require(assetContract.ownerOf(assetTokenIds[i]) == msg.sender, "Not asset owner");
        }
        
        uint256 bundleId = nextBundleId++;
        
        Bundle storage newBundle = bundles[bundleId];
        newBundle.bundleId = bundleId;
        newBundle.name = name;
        newBundle.description = description;
        newBundle.creator = msg.sender;
        newBundle.assetTokenIds = assetTokenIds;
        newBundle.assetContracts = assetContracts;
        newBundle.price = price;
        newBundle.isActive = true;
        newBundle.totalSold = 0;
        newBundle.maxSupply = maxSupply;
        
        userBundles[msg.sender].push(bundleId);
        
        emit BundleCreated(bundleId, msg.sender, price);
        
        return bundleId;
    }
    
    function purchaseBundle(uint256 bundleId) external payable nonReentrant {
        Bundle storage bundle = bundles[bundleId];
        require(bundle.isActive, "Bundle not active");
        require(msg.value >= bundle.price, "Insufficient payment");
        require(!bundle.purchasers[msg.sender], "Already purchased");
        
        if (bundle.maxSupply > 0) {
            require(bundle.totalSold < bundle.maxSupply, "Sold out");
        }
        
        // Verify creator still owns all assets
        for (uint i = 0; i < bundle.assetTokenIds.length; i++) {
            IERC721 assetContract = IERC721(bundle.assetContracts[i]);
            require(assetContract.ownerOf(bundle.assetTokenIds[i]) == bundle.creator, "Creator no longer owns assets");
        }
        
        bundle.purchasers[msg.sender] = true;
        bundle.totalSold++;
        userOwnedBundles[msg.sender][bundleId] = true;
        
        // Distribute payment
        uint256 platformFee = (bundle.price * platformFeePercentage) / 10000;
        uint256 creatorAmount = bundle.price - platformFee;
        
        (bool success, ) = bundle.creator.call{value: creatorAmount}("");
        require(success, "Creator payment failed");
        
        (success, ) = feeRecipient.call{value: platformFee}("");
        require(success, "Platform fee failed");
        
        // Refund excess
        if (msg.value > bundle.price) {
            (success, ) = msg.sender.call{value: msg.value - bundle.price}("");
            require(success, "Refund failed");
        }
        
        emit BundlePurchased(bundleId, msg.sender);
    }
    
    function updateBundlePrice(uint256 bundleId, uint256 newPrice) external {
        Bundle storage bundle = bundles[bundleId];
        require(bundle.creator == msg.sender, "Not bundle creator");
        require(bundle.isActive, "Bundle not active");
        
        bundle.price = newPrice;
        emit BundleUpdated(bundleId, newPrice);
    }
    
    function deactivateBundle(uint256 bundleId) external {
        Bundle storage bundle = bundles[bundleId];
        require(bundle.creator == msg.sender, "Not bundle creator");
        require(bundle.isActive, "Already deactivated");
        
        bundle.isActive = false;
        emit BundleDeactivated(bundleId);
    }
    
    function getBundleAssets(uint256 bundleId) external view returns (
        uint256[] memory tokenIds,
        address[] memory contracts
    ) {
        Bundle storage bundle = bundles[bundleId];
        return (bundle.assetTokenIds, bundle.assetContracts);
    }
    
    function getBundleInfo(uint256 bundleId) external view returns (BundleInfo memory) {
        Bundle storage bundle = bundles[bundleId];
        return BundleInfo({
            bundleId: bundle.bundleId,
            name: bundle.name,
            description: bundle.description,
            creator: bundle.creator,
            price: bundle.price,
            isActive: bundle.isActive,
            totalSold: bundle.totalSold,
            maxSupply: bundle.maxSupply
        });
    }
    
    function hasPurchasedBundle(address user, uint256 bundleId) external view returns (bool) {
        return bundles[bundleId].purchasers[user];
    }
    
    function getUserBundles(address user) external view returns (uint256[] memory) {
        return userBundles[user];
    }
    
    function setPlatformFee(uint256 newFee) external onlyOwner {
        require(newFee <= 1000, "Fee too high"); // Max 10%
        platformFeePercentage = newFee;
    }
} 