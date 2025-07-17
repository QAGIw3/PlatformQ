// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import "@openzeppelin/contracts/token/ERC721/IERC721.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/access/Ownable.sol";

contract FractionalOwnership is ERC20, ReentrancyGuard, Ownable {
    
    struct FractionalAsset {
        uint256 assetId;
        address nftContract;
        uint256 tokenId;
        uint256 totalShares;
        uint256 sharePrice;
        uint256 reservePrice;
        address curator;
        bool isLocked;
        bool isBuyoutActive;
        address buyoutInitiator;
        uint256 buyoutPrice;
        uint256 buyoutEndTime;
        mapping(address => bool) buyoutRejections;
        uint256 totalRevenue;
    }
    
    mapping(uint256 => FractionalAsset) public fractionalAssets;
    mapping(address => mapping(uint256 => uint256)) public userShares;
    mapping(uint256 => address[]) public assetShareholders;
    
    uint256 public nextAssetId;
    uint256 public platformFeePercentage = 250; // 2.5%
    uint256 public minSharesForProposal = 5100; // 51%
    uint256 public buyoutDuration = 7 days;
    
    event AssetFractionalized(uint256 indexed assetId, address indexed nftContract, uint256 tokenId, uint256 totalShares);
    event SharesPurchased(uint256 indexed assetId, address indexed buyer, uint256 shares);
    event BuyoutProposed(uint256 indexed assetId, address indexed proposer, uint256 price);
    event BuyoutCompleted(uint256 indexed assetId, address indexed buyer);
    event BuyoutRejected(uint256 indexed assetId);
    event RevenueDistributed(uint256 indexed assetId, uint256 amount);
    
    constructor() ERC20("Fractional Asset Share", "FAS") {}
    
    function fractionalizeAsset(
        address nftContract,
        uint256 tokenId,
        uint256 totalShares,
        uint256 sharePrice,
        uint256 reservePrice
    ) external nonReentrant returns (uint256) {
        require(totalShares > 0 && totalShares <= 10000, "Invalid share count");
        require(sharePrice > 0, "Invalid share price");
        
        IERC721 nft = IERC721(nftContract);
        require(nft.ownerOf(tokenId) == msg.sender, "Not NFT owner");
        
        // Transfer NFT to this contract
        nft.transferFrom(msg.sender, address(this), tokenId);
        
        uint256 assetId = nextAssetId++;
        
        FractionalAsset storage asset = fractionalAssets[assetId];
        asset.assetId = assetId;
        asset.nftContract = nftContract;
        asset.tokenId = tokenId;
        asset.totalShares = totalShares;
        asset.sharePrice = sharePrice;
        asset.reservePrice = reservePrice;
        asset.curator = msg.sender;
        asset.isLocked = false;
        
        // Mint shares to curator
        _mint(msg.sender, totalShares);
        userShares[msg.sender][assetId] = totalShares;
        assetShareholders[assetId].push(msg.sender);
        
        emit AssetFractionalized(assetId, nftContract, tokenId, totalShares);
        
        return assetId;
    }
    
    function purchaseShares(uint256 assetId, uint256 shares) external payable nonReentrant {
        FractionalAsset storage asset = fractionalAssets[assetId];
        require(!asset.isLocked, "Asset locked");
        require(shares > 0, "Invalid share amount");
        
        uint256 cost = shares * asset.sharePrice;
        require(msg.value >= cost, "Insufficient payment");
        
        address seller = asset.curator;
        uint256 sellerShares = userShares[seller][assetId];
        require(sellerShares >= shares, "Insufficient shares available");
        
        // Transfer shares
        _transfer(seller, msg.sender, shares);
        userShares[seller][assetId] -= shares;
        userShares[msg.sender][assetId] += shares;
        
        // Add to shareholders if new
        if (userShares[msg.sender][assetId] == shares) {
            assetShareholders[assetId].push(msg.sender);
        }
        
        // Distribute payment
        uint256 platformFee = (cost * platformFeePercentage) / 10000;
        uint256 sellerAmount = cost - platformFee;
        
        (bool success, ) = seller.call{value: sellerAmount}("");
        require(success, "Seller payment failed");
        
        // Refund excess
        if (msg.value > cost) {
            (success, ) = msg.sender.call{value: msg.value - cost}("");
            require(success, "Refund failed");
        }
        
        emit SharesPurchased(assetId, msg.sender, shares);
    }
    
    function proposeBuyout(uint256 assetId, uint256 buyoutPrice) external nonReentrant {
        FractionalAsset storage asset = fractionalAssets[assetId];
        require(!asset.isLocked, "Asset locked");
        require(!asset.isBuyoutActive, "Buyout already active");
        require(buyoutPrice >= asset.reservePrice, "Below reserve price");
        
        uint256 proposerShares = userShares[msg.sender][assetId];
        uint256 requiredShares = (asset.totalShares * minSharesForProposal) / 10000;
        require(proposerShares >= requiredShares, "Insufficient shares");
        
        asset.isBuyoutActive = true;
        asset.buyoutInitiator = msg.sender;
        asset.buyoutPrice = buyoutPrice;
        asset.buyoutEndTime = block.timestamp + buyoutDuration;
        
        emit BuyoutProposed(assetId, msg.sender, buyoutPrice);
    }
    
    function rejectBuyout(uint256 assetId) external {
        FractionalAsset storage asset = fractionalAssets[assetId];
        require(asset.isBuyoutActive, "No active buyout");
        require(block.timestamp < asset.buyoutEndTime, "Buyout ended");
        require(userShares[msg.sender][assetId] > 0, "Not a shareholder");
        
        asset.buyoutRejections[msg.sender] = true;
        
        // Check if enough rejections (>49% of shares)
        uint256 rejectingShares = 0;
        address[] memory shareholders = assetShareholders[assetId];
        
        for (uint i = 0; i < shareholders.length; i++) {
            if (asset.buyoutRejections[shareholders[i]]) {
                rejectingShares += userShares[shareholders[i]][assetId];
            }
        }
        
        if (rejectingShares > asset.totalShares * 4900 / 10000) {
            asset.isBuyoutActive = false;
            asset.buyoutInitiator = address(0);
            asset.buyoutPrice = 0;
            asset.buyoutEndTime = 0;
            
            // Clear rejections
            for (uint i = 0; i < shareholders.length; i++) {
                asset.buyoutRejections[shareholders[i]] = false;
            }
            
            emit BuyoutRejected(assetId);
        }
    }
    
    function completeBuyout(uint256 assetId) external payable nonReentrant {
        FractionalAsset storage asset = fractionalAssets[assetId];
        require(asset.isBuyoutActive, "No active buyout");
        require(block.timestamp >= asset.buyoutEndTime, "Buyout not ended");
        require(msg.sender == asset.buyoutInitiator, "Not buyout initiator");
        
        uint256 totalCost = asset.buyoutPrice * asset.totalShares;
        require(msg.value >= totalCost, "Insufficient payment");
        
        asset.isLocked = true;
        asset.isBuyoutActive = false;
        
        // Transfer NFT to buyer
        IERC721(asset.nftContract).transferFrom(address(this), msg.sender, asset.tokenId);
        
        // Distribute payment to all shareholders
        address[] memory shareholders = assetShareholders[assetId];
        for (uint i = 0; i < shareholders.length; i++) {
            address shareholder = shareholders[i];
            uint256 shares = userShares[shareholder][assetId];
            if (shares > 0) {
                uint256 payment = (totalCost * shares) / asset.totalShares;
                (bool success, ) = shareholder.call{value: payment}("");
                require(success, "Shareholder payment failed");
                
                // Burn shares
                _burn(shareholder, shares);
                userShares[shareholder][assetId] = 0;
            }
        }
        
        emit BuyoutCompleted(assetId, msg.sender);
    }
    
    function distributeRevenue(uint256 assetId) external payable nonReentrant {
        FractionalAsset storage asset = fractionalAssets[assetId];
        require(!asset.isLocked, "Asset locked");
        require(msg.value > 0, "No revenue to distribute");
        
        asset.totalRevenue += msg.value;
        
        // Distribute to all shareholders proportionally
        address[] memory shareholders = assetShareholders[assetId];
        for (uint i = 0; i < shareholders.length; i++) {
            address shareholder = shareholders[i];
            uint256 shares = userShares[shareholder][assetId];
            if (shares > 0) {
                uint256 payment = (msg.value * shares) / asset.totalShares;
                (bool success, ) = shareholder.call{value: payment}("");
                require(success, "Revenue distribution failed");
            }
        }
        
        emit RevenueDistributed(assetId, msg.value);
    }
    
    function getAssetShareholders(uint256 assetId) external view returns (address[] memory) {
        return assetShareholders[assetId];
    }
    
    function getShareholderCount(uint256 assetId) external view returns (uint256) {
        return assetShareholders[assetId].length;
    }
} 