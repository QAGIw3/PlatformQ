// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/token/ERC721/IERC721.sol";
import "@openzeppelin/contracts/token/ERC1155/IERC1155.sol";
import "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/security/Pausable.sol";
import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/utils/cryptography/ECDSA.sol";

/**
 * @title AuctionProtocol
 * @dev Decentralized auction protocol supporting multiple auction types
 * Supports: English auction, Dutch auction, Sealed bid auction
 */
contract AuctionProtocol is ReentrancyGuard, Pausable, AccessControl {
    using SafeERC20 for IERC20;
    using ECDSA for bytes32;

    // Roles
    bytes32 public constant OPERATOR_ROLE = keccak256("OPERATOR_ROLE");
    bytes32 public constant FEE_MANAGER_ROLE = keccak256("FEE_MANAGER_ROLE");

    // Auction types
    enum AuctionType { ENGLISH, DUTCH, SEALED_BID }
    
    // Asset types
    enum AssetType { ERC20, ERC721, ERC1155, NATIVE }
    
    // Auction status
    enum AuctionStatus { CREATED, ACTIVE, ENDED, CANCELLED, FINALIZED }

    // Auction structure
    struct Auction {
        uint256 auctionId;
        address seller;
        AuctionType auctionType;
        AssetType assetType;
        address assetContract;
        uint256 tokenId; // For NFTs
        uint256 amount; // For ERC20/ERC1155
        address paymentToken; // address(0) for native currency
        uint256 startPrice;
        uint256 reservePrice;
        uint256 buyNowPrice; // 0 if not applicable
        uint256 priceDecrement; // For Dutch auction
        uint256 startTime;
        uint256 endTime;
        uint256 timeExtension; // Time to extend on late bids
        uint256 minBidIncrement;
        AuctionStatus status;
        address highestBidder;
        uint256 highestBid;
        uint256 totalBids;
    }

    // Bid structure
    struct Bid {
        address bidder;
        uint256 amount;
        uint256 timestamp;
        bool isRevealed; // For sealed bid auctions
        bytes32 commitment; // For sealed bid auctions
    }

    // Fee structure
    struct FeeConfig {
        uint256 platformFee; // Basis points
        uint256 creatorRoyalty; // Basis points
        address feeRecipient;
    }

    // Constants
    uint256 public constant PRECISION = 10000; // Basis points
    uint256 public constant MAX_FEE = 1000; // 10%
    uint256 public constant MIN_AUCTION_DURATION = 1 hours;
    uint256 public constant MAX_AUCTION_DURATION = 30 days;

    // State variables
    uint256 public nextAuctionId;
    mapping(uint256 => Auction) public auctions;
    mapping(uint256 => Bid[]) public auctionBids;
    mapping(uint256 => mapping(address => uint256)) public pendingReturns;
    mapping(address => uint256[]) public userAuctions;
    mapping(address => uint256[]) public userBids;
    
    // Sealed bid auction data
    mapping(uint256 => mapping(address => bytes32)) public sealedBids;
    mapping(uint256 => mapping(address => bool)) public hasRevealed;
    
    // Fee configuration
    FeeConfig public feeConfig;
    
    // Creator royalties (NFT contract => royalty config)
    mapping(address => address) public royaltyRecipients;
    mapping(address => uint256) public royaltyPercentages;

    // Events
    event AuctionCreated(
        uint256 indexed auctionId,
        address indexed seller,
        AuctionType auctionType,
        address assetContract,
        uint256 tokenId,
        uint256 startPrice
    );
    
    event BidPlaced(
        uint256 indexed auctionId,
        address indexed bidder,
        uint256 amount,
        uint256 timestamp
    );
    
    event AuctionEnded(
        uint256 indexed auctionId,
        address indexed winner,
        uint256 winningBid
    );
    
    event AuctionCancelled(uint256 indexed auctionId);
    
    event AuctionFinalized(
        uint256 indexed auctionId,
        address indexed winner,
        uint256 finalPrice
    );
    
    event SealedBidCommitted(
        uint256 indexed auctionId,
        address indexed bidder,
        bytes32 commitment
    );
    
    event SealedBidRevealed(
        uint256 indexed auctionId,
        address indexed bidder,
        uint256 amount
    );

    constructor(address _feeRecipient) {
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(OPERATOR_ROLE, msg.sender);
        _grantRole(FEE_MANAGER_ROLE, msg.sender);
        
        feeConfig = FeeConfig({
            platformFee: 250, // 2.5%
            creatorRoyalty: 250, // 2.5%
            feeRecipient: _feeRecipient
        });
    }

    /**
     * @dev Create a new auction
     */
    function createAuction(
        AuctionType _auctionType,
        AssetType _assetType,
        address _assetContract,
        uint256 _tokenId,
        uint256 _amount,
        address _paymentToken,
        uint256 _startPrice,
        uint256 _reservePrice,
        uint256 _buyNowPrice,
        uint256 _priceDecrement,
        uint256 _startTime,
        uint256 _duration,
        uint256 _minBidIncrement
    ) external whenNotPaused nonReentrant returns (uint256) {
        require(_startTime >= block.timestamp, "Invalid start time");
        require(_duration >= MIN_AUCTION_DURATION && _duration <= MAX_AUCTION_DURATION, "Invalid duration");
        require(_startPrice > 0, "Invalid start price");
        require(_reservePrice <= _startPrice, "Reserve price too high");
        
        if (_auctionType == AuctionType.DUTCH) {
            require(_priceDecrement > 0, "Invalid price decrement");
            require(_reservePrice > 0, "Dutch auction needs reserve price");
        }
        
        if (_buyNowPrice > 0) {
            require(_buyNowPrice >= _startPrice, "Buy now price too low");
        }

        uint256 auctionId = nextAuctionId++;
        
        auctions[auctionId] = Auction({
            auctionId: auctionId,
            seller: msg.sender,
            auctionType: _auctionType,
            assetType: _assetType,
            assetContract: _assetContract,
            tokenId: _tokenId,
            amount: _amount,
            paymentToken: _paymentToken,
            startPrice: _startPrice,
            reservePrice: _reservePrice,
            buyNowPrice: _buyNowPrice,
            priceDecrement: _priceDecrement,
            startTime: _startTime,
            endTime: _startTime + _duration,
            timeExtension: 10 minutes,
            minBidIncrement: _minBidIncrement,
            status: AuctionStatus.CREATED,
            highestBidder: address(0),
            highestBid: 0,
            totalBids: 0
        });

        userAuctions[msg.sender].push(auctionId);

        // Transfer asset to contract
        _transferAssetIn(_assetType, _assetContract, msg.sender, _tokenId, _amount);

        emit AuctionCreated(
            auctionId,
            msg.sender,
            _auctionType,
            _assetContract,
            _tokenId,
            _startPrice
        );

        return auctionId;
    }

    /**
     * @dev Place a bid on an English auction
     */
    function placeBid(uint256 auctionId, uint256 bidAmount) 
        external 
        payable 
        nonReentrant 
        whenNotPaused 
    {
        Auction storage auction = auctions[auctionId];
        require(auction.status == AuctionStatus.CREATED || auction.status == AuctionStatus.ACTIVE, "Auction not active");
        require(block.timestamp >= auction.startTime, "Auction not started");
        require(block.timestamp < auction.endTime, "Auction ended");
        require(auction.auctionType == AuctionType.ENGLISH, "Not an English auction");
        
        if (auction.status == AuctionStatus.CREATED) {
            auction.status = AuctionStatus.ACTIVE;
        }

        // Handle payment
        uint256 actualBidAmount;
        if (auction.paymentToken == address(0)) {
            actualBidAmount = msg.value;
        } else {
            actualBidAmount = bidAmount;
            IERC20(auction.paymentToken).safeTransferFrom(msg.sender, address(this), bidAmount);
        }

        require(actualBidAmount >= auction.startPrice, "Bid too low");
        require(
            actualBidAmount >= auction.highestBid + auction.minBidIncrement,
            "Bid increment too small"
        );

        // Return previous highest bid
        if (auction.highestBidder != address(0)) {
            pendingReturns[auctionId][auction.highestBidder] += auction.highestBid;
        }

        // Update auction state
        auction.highestBidder = msg.sender;
        auction.highestBid = actualBidAmount;
        auction.totalBids++;

        // Record bid
        auctionBids[auctionId].push(Bid({
            bidder: msg.sender,
            amount: actualBidAmount,
            timestamp: block.timestamp,
            isRevealed: true,
            commitment: bytes32(0)
        }));

        userBids[msg.sender].push(auctionId);

        // Extend auction if bid is near end
        if (block.timestamp > auction.endTime - auction.timeExtension) {
            auction.endTime = block.timestamp + auction.timeExtension;
        }

        // Check buy now price
        if (auction.buyNowPrice > 0 && actualBidAmount >= auction.buyNowPrice) {
            auction.status = AuctionStatus.ENDED;
            emit AuctionEnded(auctionId, msg.sender, actualBidAmount);
        }

        emit BidPlaced(auctionId, msg.sender, actualBidAmount, block.timestamp);
    }

    /**
     * @dev Buy from a Dutch auction
     */
    function buyDutchAuction(uint256 auctionId) 
        external 
        payable 
        nonReentrant 
        whenNotPaused 
    {
        Auction storage auction = auctions[auctionId];
        require(auction.status == AuctionStatus.CREATED || auction.status == AuctionStatus.ACTIVE, "Auction not active");
        require(block.timestamp >= auction.startTime, "Auction not started");
        require(block.timestamp < auction.endTime, "Auction ended");
        require(auction.auctionType == AuctionType.DUTCH, "Not a Dutch auction");

        if (auction.status == AuctionStatus.CREATED) {
            auction.status = AuctionStatus.ACTIVE;
        }

        // Calculate current price
        uint256 currentPrice = _getCurrentDutchPrice(auction);

        // Handle payment
        if (auction.paymentToken == address(0)) {
            require(msg.value >= currentPrice, "Insufficient payment");
            // Refund excess
            if (msg.value > currentPrice) {
                payable(msg.sender).transfer(msg.value - currentPrice);
            }
        } else {
            IERC20(auction.paymentToken).safeTransferFrom(msg.sender, address(this), currentPrice);
        }

        // Update auction
        auction.highestBidder = msg.sender;
        auction.highestBid = currentPrice;
        auction.status = AuctionStatus.ENDED;

        emit BidPlaced(auctionId, msg.sender, currentPrice, block.timestamp);
        emit AuctionEnded(auctionId, msg.sender, currentPrice);
    }

    /**
     * @dev Commit a sealed bid
     */
    function commitSealedBid(uint256 auctionId, bytes32 commitment) 
        external 
        whenNotPaused 
    {
        Auction storage auction = auctions[auctionId];
        require(auction.status == AuctionStatus.CREATED || auction.status == AuctionStatus.ACTIVE, "Auction not active");
        require(block.timestamp >= auction.startTime, "Auction not started");
        require(block.timestamp < auction.endTime, "Bidding period ended");
        require(auction.auctionType == AuctionType.SEALED_BID, "Not a sealed bid auction");
        require(sealedBids[auctionId][msg.sender] == bytes32(0), "Already committed");

        if (auction.status == AuctionStatus.CREATED) {
            auction.status = AuctionStatus.ACTIVE;
        }

        sealedBids[auctionId][msg.sender] = commitment;
        auction.totalBids++;

        emit SealedBidCommitted(auctionId, msg.sender, commitment);
    }

    /**
     * @dev Reveal a sealed bid
     */
    function revealSealedBid(
        uint256 auctionId,
        uint256 bidAmount,
        uint256 nonce
    ) external payable nonReentrant whenNotPaused {
        Auction storage auction = auctions[auctionId];
        require(auction.status == AuctionStatus.ACTIVE, "Auction not active");
        require(block.timestamp >= auction.endTime, "Reveal phase not started");
        require(block.timestamp < auction.endTime + 1 hours, "Reveal phase ended");
        require(auction.auctionType == AuctionType.SEALED_BID, "Not a sealed bid auction");
        require(!hasRevealed[auctionId][msg.sender], "Already revealed");

        // Verify commitment
        bytes32 commitment = keccak256(abi.encodePacked(msg.sender, bidAmount, nonce));
        require(sealedBids[auctionId][msg.sender] == commitment, "Invalid reveal");

        hasRevealed[auctionId][msg.sender] = true;

        // Handle payment
        if (auction.paymentToken == address(0)) {
            require(msg.value == bidAmount, "Incorrect payment");
        } else {
            IERC20(auction.paymentToken).safeTransferFrom(msg.sender, address(this), bidAmount);
        }

        // Update highest bid if applicable
        if (bidAmount > auction.highestBid && bidAmount >= auction.reservePrice) {
            // Return previous highest bid
            if (auction.highestBidder != address(0)) {
                pendingReturns[auctionId][auction.highestBidder] += auction.highestBid;
            }
            
            auction.highestBidder = msg.sender;
            auction.highestBid = bidAmount;
        } else {
            // Return this bid
            pendingReturns[auctionId][msg.sender] += bidAmount;
        }

        emit SealedBidRevealed(auctionId, msg.sender, bidAmount);
    }

    /**
     * @dev End an auction
     */
    function endAuction(uint256 auctionId) external nonReentrant {
        Auction storage auction = auctions[auctionId];
        require(auction.status == AuctionStatus.ACTIVE, "Auction not active");
        require(block.timestamp >= auction.endTime, "Auction not ended");

        if (auction.auctionType == AuctionType.SEALED_BID) {
            require(block.timestamp >= auction.endTime + 1 hours, "Reveal phase not ended");
        }

        auction.status = AuctionStatus.ENDED;

        emit AuctionEnded(auctionId, auction.highestBidder, auction.highestBid);
    }

    /**
     * @dev Finalize an auction (transfer assets and payments)
     */
    function finalizeAuction(uint256 auctionId) external nonReentrant {
        Auction storage auction = auctions[auctionId];
        require(auction.status == AuctionStatus.ENDED, "Auction not ended");
        
        auction.status = AuctionStatus.FINALIZED;

        if (auction.highestBidder != address(0) && auction.highestBid >= auction.reservePrice) {
            // Calculate fees
            uint256 platformFeeAmount = auction.highestBid * feeConfig.platformFee / PRECISION;
            uint256 royaltyAmount = 0;
            
            if (auction.assetType == AssetType.ERC721 || auction.assetType == AssetType.ERC1155) {
                address royaltyRecipient = royaltyRecipients[auction.assetContract];
                if (royaltyRecipient != address(0)) {
                    royaltyAmount = auction.highestBid * royaltyPercentages[auction.assetContract] / PRECISION;
                }
            }
            
            uint256 sellerAmount = auction.highestBid - platformFeeAmount - royaltyAmount;

            // Transfer payment
            if (auction.paymentToken == address(0)) {
                payable(feeConfig.feeRecipient).transfer(platformFeeAmount);
                if (royaltyAmount > 0) {
                    payable(royaltyRecipients[auction.assetContract]).transfer(royaltyAmount);
                }
                payable(auction.seller).transfer(sellerAmount);
            } else {
                IERC20(auction.paymentToken).safeTransfer(feeConfig.feeRecipient, platformFeeAmount);
                if (royaltyAmount > 0) {
                    IERC20(auction.paymentToken).safeTransfer(
                        royaltyRecipients[auction.assetContract],
                        royaltyAmount
                    );
                }
                IERC20(auction.paymentToken).safeTransfer(auction.seller, sellerAmount);
            }

            // Transfer asset to winner
            _transferAssetOut(
                auction.assetType,
                auction.assetContract,
                auction.highestBidder,
                auction.tokenId,
                auction.amount
            );
        } else {
            // No winner, return asset to seller
            _transferAssetOut(
                auction.assetType,
                auction.assetContract,
                auction.seller,
                auction.tokenId,
                auction.amount
            );
        }

        emit AuctionFinalized(auctionId, auction.highestBidder, auction.highestBid);
    }

    /**
     * @dev Cancel an auction (only before bids)
     */
    function cancelAuction(uint256 auctionId) external nonReentrant {
        Auction storage auction = auctions[auctionId];
        require(msg.sender == auction.seller, "Not the seller");
        require(auction.status == AuctionStatus.CREATED, "Cannot cancel active auction");
        require(auction.totalBids == 0, "Bids already placed");

        auction.status = AuctionStatus.CANCELLED;

        // Return asset to seller
        _transferAssetOut(
            auction.assetType,
            auction.assetContract,
            auction.seller,
            auction.tokenId,
            auction.amount
        );

        emit AuctionCancelled(auctionId);
    }

    /**
     * @dev Withdraw pending returns
     */
    function withdrawPendingReturns(uint256 auctionId) external nonReentrant {
        uint256 amount = pendingReturns[auctionId][msg.sender];
        require(amount > 0, "No pending returns");

        pendingReturns[auctionId][msg.sender] = 0;

        Auction memory auction = auctions[auctionId];
        if (auction.paymentToken == address(0)) {
            payable(msg.sender).transfer(amount);
        } else {
            IERC20(auction.paymentToken).safeTransfer(msg.sender, amount);
        }
    }

    /**
     * @dev Get current Dutch auction price
     */
    function getCurrentDutchPrice(uint256 auctionId) external view returns (uint256) {
        return _getCurrentDutchPrice(auctions[auctionId]);
    }

    function _getCurrentDutchPrice(Auction memory auction) internal view returns (uint256) {
        if (block.timestamp < auction.startTime) {
            return auction.startPrice;
        }

        uint256 elapsed = block.timestamp - auction.startTime;
        uint256 decrementCount = elapsed / 1 minutes; // Price decreases every minute
        uint256 totalDecrement = decrementCount * auction.priceDecrement;

        if (auction.startPrice <= totalDecrement + auction.reservePrice) {
            return auction.reservePrice;
        }

        return auction.startPrice - totalDecrement;
    }

    /**
     * @dev Transfer asset into contract
     */
    function _transferAssetIn(
        AssetType assetType,
        address assetContract,
        address from,
        uint256 tokenId,
        uint256 amount
    ) internal {
        if (assetType == AssetType.ERC20) {
            IERC20(assetContract).safeTransferFrom(from, address(this), amount);
        } else if (assetType == AssetType.ERC721) {
            IERC721(assetContract).safeTransferFrom(from, address(this), tokenId);
        } else if (assetType == AssetType.ERC1155) {
            IERC1155(assetContract).safeTransferFrom(from, address(this), tokenId, amount, "");
        }
    }

    /**
     * @dev Transfer asset out of contract
     */
    function _transferAssetOut(
        AssetType assetType,
        address assetContract,
        address to,
        uint256 tokenId,
        uint256 amount
    ) internal {
        if (assetType == AssetType.ERC20) {
            IERC20(assetContract).safeTransfer(to, amount);
        } else if (assetType == AssetType.ERC721) {
            IERC721(assetContract).safeTransferFrom(address(this), to, tokenId);
        } else if (assetType == AssetType.ERC1155) {
            IERC1155(assetContract).safeTransferFrom(address(this), to, tokenId, amount, "");
        }
    }

    // Admin functions

    function setFeeConfig(uint256 _platformFee, address _feeRecipient) 
        external 
        onlyRole(FEE_MANAGER_ROLE) 
    {
        require(_platformFee <= MAX_FEE, "Fee too high");
        require(_feeRecipient != address(0), "Invalid recipient");
        
        feeConfig.platformFee = _platformFee;
        feeConfig.feeRecipient = _feeRecipient;
    }

    function setRoyaltyConfig(
        address nftContract,
        address recipient,
        uint256 percentage
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        require(percentage <= MAX_FEE, "Royalty too high");
        
        royaltyRecipients[nftContract] = recipient;
        royaltyPercentages[nftContract] = percentage;
    }

    function pause() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _pause();
    }

    function unpause() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _unpause();
    }
} 