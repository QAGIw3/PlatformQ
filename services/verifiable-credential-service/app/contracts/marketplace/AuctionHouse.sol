// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import "./PlatformAsset.sol";

contract AuctionHouse is ReentrancyGuard, Ownable {
    PlatformAsset public platformAsset;
    
    enum AuctionType { English, Dutch }
    enum AuctionStatus { Active, Successful, Failed, Cancelled }
    
    struct Auction {
        uint256 tokenId;
        address seller;
        AuctionType auctionType;
        uint256 startPrice;
        uint256 endPrice; // For Dutch auction
        uint256 currentPrice;
        uint256 startTime;
        uint256 endTime;
        uint256 priceDecrement; // For Dutch auction
        uint256 minBidIncrement; // For English auction
        address highestBidder;
        uint256 highestBid;
        AuctionStatus status;
    }
    
    mapping(uint256 => Auction) public auctions;
    mapping(uint256 => mapping(address => uint256)) public bids; // auctionId => bidder => amount
    uint256 public nextAuctionId;
    uint256 public platformFeePercentage = 250; // 2.5%
    
    event AuctionCreated(uint256 indexed auctionId, uint256 indexed tokenId, AuctionType auctionType);
    event BidPlaced(uint256 indexed auctionId, address indexed bidder, uint256 amount);
    event AuctionEnded(uint256 indexed auctionId, address winner, uint256 finalPrice);
    event AuctionCancelled(uint256 indexed auctionId);
    
    constructor(address _platformAsset) {
        platformAsset = PlatformAsset(_platformAsset);
    }
    
    function createEnglishAuction(
        uint256 tokenId,
        uint256 startPrice,
        uint256 minBidIncrement,
        uint256 duration
    ) external nonReentrant {
        require(platformAsset.ownerOf(tokenId) == msg.sender, "Not token owner");
        require(duration > 0 && duration <= 30 days, "Invalid duration");
        
        platformAsset.transferFrom(msg.sender, address(this), tokenId);
        
        auctions[nextAuctionId] = Auction({
            tokenId: tokenId,
            seller: msg.sender,
            auctionType: AuctionType.English,
            startPrice: startPrice,
            endPrice: 0,
            currentPrice: startPrice,
            startTime: block.timestamp,
            endTime: block.timestamp + duration,
            priceDecrement: 0,
            minBidIncrement: minBidIncrement,
            highestBidder: address(0),
            highestBid: 0,
            status: AuctionStatus.Active
        });
        
        emit AuctionCreated(nextAuctionId, tokenId, AuctionType.English);
        nextAuctionId++;
    }
    
    function createDutchAuction(
        uint256 tokenId,
        uint256 startPrice,
        uint256 endPrice,
        uint256 priceDecrement,
        uint256 duration
    ) external nonReentrant {
        require(platformAsset.ownerOf(tokenId) == msg.sender, "Not token owner");
        require(startPrice > endPrice, "Invalid price range");
        require(priceDecrement > 0, "Invalid decrement");
        require(duration > 0 && duration <= 7 days, "Invalid duration");
        
        platformAsset.transferFrom(msg.sender, address(this), tokenId);
        
        auctions[nextAuctionId] = Auction({
            tokenId: tokenId,
            seller: msg.sender,
            auctionType: AuctionType.Dutch,
            startPrice: startPrice,
            endPrice: endPrice,
            currentPrice: startPrice,
            startTime: block.timestamp,
            endTime: block.timestamp + duration,
            priceDecrement: priceDecrement,
            minBidIncrement: 0,
            highestBidder: address(0),
            highestBid: 0,
            status: AuctionStatus.Active
        });
        
        emit AuctionCreated(nextAuctionId, tokenId, AuctionType.Dutch);
        nextAuctionId++;
    }
    
    function getCurrentDutchPrice(uint256 auctionId) public view returns (uint256) {
        Auction memory auction = auctions[auctionId];
        require(auction.auctionType == AuctionType.Dutch, "Not Dutch auction");
        
        if (block.timestamp >= auction.endTime) {
            return auction.endPrice;
        }
        
        uint256 elapsed = block.timestamp - auction.startTime;
        uint256 totalDecrements = elapsed / 3600; // Decrease every hour
        uint256 priceReduction = totalDecrements * auction.priceDecrement;
        
        if (auction.startPrice <= priceReduction + auction.endPrice) {
            return auction.endPrice;
        }
        
        return auction.startPrice - priceReduction;
    }
    
    function bidEnglish(uint256 auctionId) external payable nonReentrant {
        Auction storage auction = auctions[auctionId];
        require(auction.status == AuctionStatus.Active, "Auction not active");
        require(auction.auctionType == AuctionType.English, "Not English auction");
        require(block.timestamp < auction.endTime, "Auction ended");
        require(msg.value >= auction.currentPrice + auction.minBidIncrement, "Bid too low");
        
        // Refund previous highest bidder
        if (auction.highestBidder != address(0)) {
            bids[auctionId][auction.highestBidder] += auction.highestBid;
        }
        
        auction.highestBidder = msg.sender;
        auction.highestBid = msg.value;
        auction.currentPrice = msg.value;
        
        emit BidPlaced(auctionId, msg.sender, msg.value);
    }
    
    function buyDutch(uint256 auctionId) external payable nonReentrant {
        Auction storage auction = auctions[auctionId];
        require(auction.status == AuctionStatus.Active, "Auction not active");
        require(auction.auctionType == AuctionType.Dutch, "Not Dutch auction");
        require(block.timestamp < auction.endTime, "Auction ended");
        
        uint256 currentPrice = getCurrentDutchPrice(auctionId);
        require(msg.value >= currentPrice, "Insufficient payment");
        
        auction.status = AuctionStatus.Successful;
        auction.highestBidder = msg.sender;
        auction.highestBid = currentPrice;
        
        // Transfer NFT to buyer
        platformAsset.transferFrom(address(this), msg.sender, auction.tokenId);
        
        // Distribute payment
        uint256 platformFee = (currentPrice * platformFeePercentage) / 10000;
        uint256 sellerAmount = currentPrice - platformFee;
        
        (bool success, ) = auction.seller.call{value: sellerAmount}("");
        require(success, "Seller payment failed");
        
        // Refund excess
        if (msg.value > currentPrice) {
            (success, ) = msg.sender.call{value: msg.value - currentPrice}("");
            require(success, "Refund failed");
        }
        
        emit AuctionEnded(auctionId, msg.sender, currentPrice);
    }
    
    function endEnglishAuction(uint256 auctionId) external nonReentrant {
        Auction storage auction = auctions[auctionId];
        require(auction.status == AuctionStatus.Active, "Auction not active");
        require(auction.auctionType == AuctionType.English, "Not English auction");
        require(block.timestamp >= auction.endTime, "Auction not ended");
        
        if (auction.highestBidder != address(0)) {
            auction.status = AuctionStatus.Successful;
            
            // Transfer NFT to winner
            platformAsset.transferFrom(address(this), auction.highestBidder, auction.tokenId);
            
            // Distribute payment
            uint256 platformFee = (auction.highestBid * platformFeePercentage) / 10000;
            uint256 sellerAmount = auction.highestBid - platformFee;
            
            (bool success, ) = auction.seller.call{value: sellerAmount}("");
            require(success, "Seller payment failed");
            
            emit AuctionEnded(auctionId, auction.highestBidder, auction.highestBid);
        } else {
            auction.status = AuctionStatus.Failed;
            
            // Return NFT to seller
            platformAsset.transferFrom(address(this), auction.seller, auction.tokenId);
            
            emit AuctionEnded(auctionId, address(0), 0);
        }
    }
    
    function cancelAuction(uint256 auctionId) external nonReentrant {
        Auction storage auction = auctions[auctionId];
        require(auction.seller == msg.sender, "Not auction seller");
        require(auction.status == AuctionStatus.Active, "Auction not active");
        require(auction.highestBidder == address(0), "Bids already placed");
        
        auction.status = AuctionStatus.Cancelled;
        
        // Return NFT to seller
        platformAsset.transferFrom(address(this), auction.seller, auction.tokenId);
        
        emit AuctionCancelled(auctionId);
    }
    
    function withdrawBid(uint256 auctionId) external nonReentrant {
        uint256 amount = bids[auctionId][msg.sender];
        require(amount > 0, "No bid to withdraw");
        
        bids[auctionId][msg.sender] = 0;
        
        (bool success, ) = msg.sender.call{value: amount}("");
        require(success, "Withdrawal failed");
    }
} 