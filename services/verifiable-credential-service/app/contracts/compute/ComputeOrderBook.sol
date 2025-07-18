// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts-upgradeable/access/AccessControlUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/security/PausableUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/security/ReentrancyGuardUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";
import "@chainlink/contracts/src/v0.8/interfaces/AggregatorV3Interface.sol";

interface IERC20 {
    function transfer(address to, uint256 amount) external returns (bool);
    function transferFrom(address from, address to, uint256 amount) external returns (bool);
    function balanceOf(address account) external view returns (uint256);
}

interface IComputeRegistry {
    function allocateResource(string memory resourceId, address user, uint256 amount) external;
    function releaseResource(string memory resourceId, address user, uint256 amount) external;
}

/**
 * @title ComputeOrderBook
 * @notice On-chain order book for decentralized compute resource trading
 * @dev Implements limit orders, market orders, and advanced order types with efficient matching
 */
contract ComputeOrderBook is 
    Initializable,
    AccessControlUpgradeable,
    PausableUpgradeable,
    ReentrancyGuardUpgradeable,
    UUPSUpgradeable 
{
    bytes32 public constant OPERATOR_ROLE = keccak256("OPERATOR_ROLE");
    bytes32 public constant MARKET_MAKER_ROLE = keccak256("MARKET_MAKER_ROLE");
    
    enum OrderType {
        LIMIT,
        MARKET,
        STOP_LOSS,
        TAKE_PROFIT,
        ICEBERG,
        GOOD_TILL_CANCELLED,
        IMMEDIATE_OR_CANCEL,
        FILL_OR_KILL
    }
    
    enum OrderSide {
        BUY,
        SELL
    }
    
    enum OrderStatus {
        OPEN,
        PARTIALLY_FILLED,
        FILLED,
        CANCELLED,
        EXPIRED
    }
    
    enum ResourceType {
        GPU,
        CPU,
        MEMORY,
        STORAGE,
        BANDWIDTH
    }
    
    struct Order {
        uint256 orderId;
        address trader;
        OrderType orderType;
        OrderSide side;
        ResourceType resourceType;
        uint256 price; // Price per unit
        uint256 quantity; // Total quantity
        uint256 filledQuantity; // Amount already filled
        uint256 visibleQuantity; // For iceberg orders
        uint256 stopPrice; // For stop orders
        uint256 timestamp;
        uint256 expiry; // For GTT orders
        OrderStatus status;
        bool postOnly; // Maker only order
        uint256 minExecuteQuantity; // Minimum fill size
        uint256 deliveryTime; // When resources should be delivered
        string specifications; // JSON resource specifications
    }
    
    struct Trade {
        uint256 tradeId;
        uint256 buyOrderId;
        uint256 sellOrderId;
        address buyer;
        address seller;
        uint256 price;
        uint256 quantity;
        uint256 timestamp;
        ResourceType resourceType;
        uint256 settlementTime;
        bool isSettled;
    }
    
    struct OrderBookLevel {
        uint256 price;
        uint256 totalQuantity;
        uint256[] orderIds;
    }
    
    struct MarketData {
        uint256 lastPrice;
        uint256 highPrice24h;
        uint256 lowPrice24h;
        uint256 volume24h;
        uint256 bestBid;
        uint256 bestAsk;
        uint256 spread;
        uint256 lastUpdateTime;
    }
    
    // State variables
    IERC20 public settlementToken;
    IComputeRegistry public computeRegistry;
    AggregatorV3Interface public priceOracle;
    
    uint256 public nextOrderId;
    uint256 public nextTradeId;
    
    mapping(uint256 => Order) public orders;
    mapping(address => uint256[]) public userOrders;
    mapping(address => mapping(ResourceType => uint256)) public userBalances;
    
    // Order book structure: resourceType -> side -> price -> orderIds
    mapping(ResourceType => mapping(OrderSide => mapping(uint256 => uint256[]))) public orderBook;
    mapping(ResourceType => uint256[]) public bidPrices; // Sorted descending
    mapping(ResourceType => uint256[]) public askPrices; // Sorted ascending
    
    mapping(uint256 => Trade) public trades;
    mapping(ResourceType => MarketData) public marketData;
    
    // Fee structure
    uint256 public makerFee = 10; // 0.1% in basis points
    uint256 public takerFee = 25; // 0.25% in basis points
    uint256 public marketMakerRebate = 5; // 0.05% rebate
    
    // Risk parameters
    uint256 public maxOrderSize = 10000; // Maximum order size
    uint256 public minOrderSize = 1; // Minimum order size
    uint256 public priceTickSize = 1e15; // Minimum price increment
    
    // Circuit breakers
    uint256 public priceMovementLimit = 1000; // 10% in basis points
    uint256 public circuitBreakerCooldown = 5 minutes;
    mapping(ResourceType => uint256) public lastCircuitBreaker;
    
    // Events
    event OrderPlaced(
        uint256 indexed orderId,
        address indexed trader,
        OrderSide side,
        ResourceType resourceType,
        uint256 price,
        uint256 quantity
    );
    
    event OrderCancelled(uint256 indexed orderId, address indexed trader);
    
    event OrderMatched(
        uint256 indexed tradeId,
        uint256 buyOrderId,
        uint256 sellOrderId,
        uint256 price,
        uint256 quantity
    );
    
    event MarketDataUpdated(
        ResourceType indexed resourceType,
        uint256 lastPrice,
        uint256 volume
    );
    
    event CircuitBreakerTriggered(
        ResourceType indexed resourceType,
        uint256 priceMovement
    );
    
    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }
    
    function initialize(
        address _settlementToken,
        address _computeRegistry,
        address _priceOracle
    ) public initializer {
        __AccessControl_init();
        __Pausable_init();
        __ReentrancyGuard_init();
        __UUPSUpgradeable_init();
        
        settlementToken = IERC20(_settlementToken);
        computeRegistry = IComputeRegistry(_computeRegistry);
        priceOracle = AggregatorV3Interface(_priceOracle);
        
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(OPERATOR_ROLE, msg.sender);
    }
    
    /**
     * @notice Place a new order
     */
    function placeOrder(
        OrderType orderType,
        OrderSide side,
        ResourceType resourceType,
        uint256 price,
        uint256 quantity,
        uint256 visibleQuantity,
        uint256 stopPrice,
        uint256 expiry,
        bool postOnly,
        string memory specifications
    ) external nonReentrant whenNotPaused returns (uint256) {
        require(quantity >= minOrderSize && quantity <= maxOrderSize, "Invalid quantity");
        require(price % priceTickSize == 0, "Invalid price tick");
        
        // Check circuit breaker
        require(
            block.timestamp >= lastCircuitBreaker[resourceType] + circuitBreakerCooldown,
            "Circuit breaker active"
        );
        
        // Validate order type specific requirements
        if (orderType == OrderType.ICEBERG) {
            require(visibleQuantity > 0 && visibleQuantity <= quantity, "Invalid iceberg size");
        }
        if (orderType == OrderType.STOP_LOSS || orderType == OrderType.TAKE_PROFIT) {
            require(stopPrice > 0, "Stop price required");
        }
        
        uint256 orderId = nextOrderId++;
        
        orders[orderId] = Order({
            orderId: orderId,
            trader: msg.sender,
            orderType: orderType,
            side: side,
            resourceType: resourceType,
            price: price,
            quantity: quantity,
            filledQuantity: 0,
            visibleQuantity: orderType == OrderType.ICEBERG ? visibleQuantity : quantity,
            stopPrice: stopPrice,
            timestamp: block.timestamp,
            expiry: expiry,
            status: OrderStatus.OPEN,
            postOnly: postOnly,
            minExecuteQuantity: 1,
            deliveryTime: block.timestamp + 1 hours,
            specifications: specifications
        });
        
        userOrders[msg.sender].push(orderId);
        
        // Process order based on type
        if (orderType == OrderType.MARKET) {
            _executeMarketOrder(orderId);
        } else if (orderType == OrderType.IMMEDIATE_OR_CANCEL) {
            _executeIOCOrder(orderId);
        } else if (orderType == OrderType.FILL_OR_KILL) {
            _executeFOKOrder(orderId);
        } else {
            _addToOrderBook(orderId);
            _tryMatchOrder(orderId);
        }
        
        emit OrderPlaced(orderId, msg.sender, side, resourceType, price, quantity);
        
        return orderId;
    }
    
    /**
     * @notice Cancel an open order
     */
    function cancelOrder(uint256 orderId) external nonReentrant {
        Order storage order = orders[orderId];
        require(order.trader == msg.sender, "Not order owner");
        require(
            order.status == OrderStatus.OPEN || order.status == OrderStatus.PARTIALLY_FILLED,
            "Order not cancellable"
        );
        
        // Remove from order book
        _removeFromOrderBook(orderId);
        
        // Update status
        order.status = OrderStatus.CANCELLED;
        
        // Return any locked funds
        if (order.side == OrderSide.BUY) {
            uint256 remainingValue = (order.quantity - order.filledQuantity) * order.price / 1e18;
            settlementToken.transfer(msg.sender, remainingValue);
        }
        
        emit OrderCancelled(orderId, msg.sender);
    }
    
    /**
     * @notice Get order book depth
     */
    function getOrderBookDepth(
        ResourceType resourceType,
        uint256 levels
    ) external view returns (
        OrderBookLevel[] memory bids,
        OrderBookLevel[] memory asks
    ) {
        bids = new OrderBookLevel[](levels);
        asks = new OrderBookLevel[](levels);
        
        // Get bid levels
        uint256 bidCount = 0;
        for (uint256 i = 0; i < bidPrices[resourceType].length && bidCount < levels; i++) {
            uint256 price = bidPrices[resourceType][i];
            uint256[] memory orderIds = orderBook[resourceType][OrderSide.BUY][price];
            if (orderIds.length > 0) {
                uint256 totalQty = 0;
                for (uint256 j = 0; j < orderIds.length; j++) {
                    Order memory order = orders[orderIds[j]];
                    if (order.status == OrderStatus.OPEN || order.status == OrderStatus.PARTIALLY_FILLED) {
                        totalQty += order.visibleQuantity - order.filledQuantity;
                    }
                }
                if (totalQty > 0) {
                    bids[bidCount] = OrderBookLevel(price, totalQty, orderIds);
                    bidCount++;
                }
            }
        }
        
        // Get ask levels
        uint256 askCount = 0;
        for (uint256 i = 0; i < askPrices[resourceType].length && askCount < levels; i++) {
            uint256 price = askPrices[resourceType][i];
            uint256[] memory orderIds = orderBook[resourceType][OrderSide.SELL][price];
            if (orderIds.length > 0) {
                uint256 totalQty = 0;
                for (uint256 j = 0; j < orderIds.length; j++) {
                    Order memory order = orders[orderIds[j]];
                    if (order.status == OrderStatus.OPEN || order.status == OrderStatus.PARTIALLY_FILLED) {
                        totalQty += order.visibleQuantity - order.filledQuantity;
                    }
                }
                if (totalQty > 0) {
                    asks[askCount] = OrderBookLevel(price, totalQty, orderIds);
                    askCount++;
                }
            }
        }
        
        return (bids, asks);
    }
    
    /**
     * @notice Process expired orders
     */
    function processExpiredOrders(uint256[] calldata orderIds) external {
        for (uint256 i = 0; i < orderIds.length; i++) {
            Order storage order = orders[orderIds[i]];
            if (order.expiry > 0 && block.timestamp >= order.expiry && 
                (order.status == OrderStatus.OPEN || order.status == OrderStatus.PARTIALLY_FILLED)) {
                _removeFromOrderBook(orderIds[i]);
                order.status = OrderStatus.EXPIRED;
            }
        }
    }
    
    /**
     * @notice Update market maker rebates
     */
    function claimMarketMakerRebate() external nonReentrant {
        uint256 rebate = _calculateRebate(msg.sender);
        require(rebate > 0, "No rebate available");
        
        settlementToken.transfer(msg.sender, rebate);
    }
    
    // Internal functions
    
    function _addToOrderBook(uint256 orderId) internal {
        Order storage order = orders[orderId];
        
        // Add to price level
        orderBook[order.resourceType][order.side][order.price].push(orderId);
        
        // Update sorted price arrays
        if (order.side == OrderSide.BUY) {
            _insertBidPrice(order.resourceType, order.price);
        } else {
            _insertAskPrice(order.resourceType, order.price);
        }
        
        // Update market data
        _updateMarketData(order.resourceType);
    }
    
    function _removeFromOrderBook(uint256 orderId) internal {
        Order storage order = orders[orderId];
        uint256[] storage orderIds = orderBook[order.resourceType][order.side][order.price];
        
        // Find and remove order
        for (uint256 i = 0; i < orderIds.length; i++) {
            if (orderIds[i] == orderId) {
                orderIds[i] = orderIds[orderIds.length - 1];
                orderIds.pop();
                break;
            }
        }
        
        // Remove price level if empty
        if (orderIds.length == 0) {
            if (order.side == OrderSide.BUY) {
                _removeBidPrice(order.resourceType, order.price);
            } else {
                _removeAskPrice(order.resourceType, order.price);
            }
        }
    }
    
    function _tryMatchOrder(uint256 orderId) internal {
        Order storage order = orders[orderId];
        
        if (order.side == OrderSide.BUY) {
            _matchBuyOrder(orderId);
        } else {
            _matchSellOrder(orderId);
        }
    }
    
    function _matchBuyOrder(uint256 buyOrderId) internal {
        Order storage buyOrder = orders[buyOrderId];
        uint256[] memory askPriceArray = askPrices[buyOrder.resourceType];
        
        for (uint256 i = 0; i < askPriceArray.length; i++) {
            uint256 askPrice = askPriceArray[i];
            if (askPrice > buyOrder.price) break; // No match possible
            
            uint256[] memory sellOrderIds = orderBook[buyOrder.resourceType][OrderSide.SELL][askPrice];
            
            for (uint256 j = 0; j < sellOrderIds.length; j++) {
                if (buyOrder.filledQuantity >= buyOrder.quantity) return; // Fully filled
                
                Order storage sellOrder = orders[sellOrderIds[j]];
                if (sellOrder.status != OrderStatus.OPEN && sellOrder.status != OrderStatus.PARTIALLY_FILLED) continue;
                
                // Calculate match quantity
                uint256 buyRemaining = buyOrder.quantity - buyOrder.filledQuantity;
                uint256 sellRemaining = sellOrder.quantity - sellOrder.filledQuantity;
                uint256 matchQuantity = buyRemaining < sellRemaining ? buyRemaining : sellRemaining;
                
                // Execute trade
                _executeTrade(buyOrderId, sellOrderIds[j], askPrice, matchQuantity);
            }
        }
    }
    
    function _matchSellOrder(uint256 sellOrderId) internal {
        Order storage sellOrder = orders[sellOrderId];
        uint256[] memory bidPriceArray = bidPrices[sellOrder.resourceType];
        
        for (uint256 i = 0; i < bidPriceArray.length; i++) {
            uint256 bidPrice = bidPriceArray[i];
            if (bidPrice < sellOrder.price) break; // No match possible
            
            uint256[] memory buyOrderIds = orderBook[sellOrder.resourceType][OrderSide.BUY][bidPrice];
            
            for (uint256 j = 0; j < buyOrderIds.length; j++) {
                if (sellOrder.filledQuantity >= sellOrder.quantity) return; // Fully filled
                
                Order storage buyOrder = orders[buyOrderIds[j]];
                if (buyOrder.status != OrderStatus.OPEN && buyOrder.status != OrderStatus.PARTIALLY_FILLED) continue;
                
                // Calculate match quantity
                uint256 sellRemaining = sellOrder.quantity - sellOrder.filledQuantity;
                uint256 buyRemaining = buyOrder.quantity - buyOrder.filledQuantity;
                uint256 matchQuantity = sellRemaining < buyRemaining ? sellRemaining : buyRemaining;
                
                // Execute trade
                _executeTrade(buyOrderIds[j], sellOrderId, bidPrice, matchQuantity);
            }
        }
    }
    
    function _executeTrade(
        uint256 buyOrderId,
        uint256 sellOrderId,
        uint256 price,
        uint256 quantity
    ) internal {
        Order storage buyOrder = orders[buyOrderId];
        Order storage sellOrder = orders[sellOrderId];
        
        // Update filled quantities
        buyOrder.filledQuantity += quantity;
        sellOrder.filledQuantity += quantity;
        
        // Update order statuses
        if (buyOrder.filledQuantity >= buyOrder.quantity) {
            buyOrder.status = OrderStatus.FILLED;
            _removeFromOrderBook(buyOrderId);
        } else {
            buyOrder.status = OrderStatus.PARTIALLY_FILLED;
            // Handle iceberg order refill
            if (buyOrder.orderType == OrderType.ICEBERG) {
                _refillIcebergOrder(buyOrderId);
            }
        }
        
        if (sellOrder.filledQuantity >= sellOrder.quantity) {
            sellOrder.status = OrderStatus.FILLED;
            _removeFromOrderBook(sellOrderId);
        } else {
            sellOrder.status = OrderStatus.PARTIALLY_FILLED;
            // Handle iceberg order refill
            if (sellOrder.orderType == OrderType.ICEBERG) {
                _refillIcebergOrder(sellOrderId);
            }
        }
        
        // Create trade record
        uint256 tradeId = nextTradeId++;
        trades[tradeId] = Trade({
            tradeId: tradeId,
            buyOrderId: buyOrderId,
            sellOrderId: sellOrderId,
            buyer: buyOrder.trader,
            seller: sellOrder.trader,
            price: price,
            quantity: quantity,
            timestamp: block.timestamp,
            resourceType: buyOrder.resourceType,
            settlementTime: block.timestamp + 30 minutes,
            isSettled: false
        });
        
        // Transfer tokens and fees
        _settleTrade(tradeId);
        
        // Update market data
        _updateMarketData(buyOrder.resourceType);
        _checkCircuitBreaker(buyOrder.resourceType, price);
        
        emit OrderMatched(tradeId, buyOrderId, sellOrderId, price, quantity);
    }
    
    function _settleTrade(uint256 tradeId) internal {
        Trade storage trade = trades[tradeId];
        
        // Calculate fees
        uint256 buyerFee = (trade.quantity * trade.price * takerFee) / (1e18 * 10000);
        uint256 sellerFee = (trade.quantity * trade.price * makerFee) / (1e18 * 10000);
        
        // Transfer from buyer to seller
        uint256 totalCost = (trade.quantity * trade.price / 1e18) + buyerFee;
        settlementToken.transferFrom(trade.buyer, address(this), totalCost);
        
        uint256 sellerReceives = (trade.quantity * trade.price / 1e18) - sellerFee;
        settlementToken.transfer(trade.seller, sellerReceives);
        
        // Allocate compute resources
        computeRegistry.allocateResource(
            string(abi.encodePacked("TRADE_", uint2str(tradeId))),
            trade.buyer,
            trade.quantity
        );
        
        trade.isSettled = true;
    }
    
    function _executeMarketOrder(uint256 orderId) internal {
        Order storage order = orders[orderId];
        
        if (order.side == OrderSide.BUY) {
            // Buy at any price up to slippage limit
            _matchBuyOrder(orderId);
        } else {
            // Sell at any price down to slippage limit
            _matchSellOrder(orderId);
        }
        
        // Cancel remaining if not fully filled
        if (order.filledQuantity < order.quantity) {
            order.status = OrderStatus.CANCELLED;
        }
    }
    
    function _executeIOCOrder(uint256 orderId) internal {
        _tryMatchOrder(orderId);
        
        Order storage order = orders[orderId];
        if (order.filledQuantity < order.quantity) {
            order.status = OrderStatus.CANCELLED;
        }
    }
    
    function _executeFOKOrder(uint256 orderId) internal {
        Order storage order = orders[orderId];
        
        // Check if full quantity can be filled
        uint256 availableQuantity = _getAvailableQuantity(
            order.resourceType,
            order.side == OrderSide.BUY ? OrderSide.SELL : OrderSide.BUY,
            order.price
        );
        
        if (availableQuantity >= order.quantity) {
            _tryMatchOrder(orderId);
        } else {
            order.status = OrderStatus.CANCELLED;
        }
    }
    
    function _refillIcebergOrder(uint256 orderId) internal {
        Order storage order = orders[orderId];
        uint256 remaining = order.quantity - order.filledQuantity;
        
        if (remaining > 0) {
            order.visibleQuantity = remaining < order.visibleQuantity ? remaining : order.visibleQuantity;
        }
    }
    
    function _insertBidPrice(ResourceType resourceType, uint256 price) internal {
        uint256[] storage prices = bidPrices[resourceType];
        
        // Binary search to find insertion point
        uint256 left = 0;
        uint256 right = prices.length;
        
        while (left < right) {
            uint256 mid = (left + right) / 2;
            if (prices[mid] > price) {
                left = mid + 1;
            } else if (prices[mid] < price) {
                right = mid;
            } else {
                return; // Price already exists
            }
        }
        
        // Insert at position
        prices.push(price);
        for (uint256 i = prices.length - 1; i > left; i--) {
            prices[i] = prices[i - 1];
        }
        prices[left] = price;
    }
    
    function _insertAskPrice(ResourceType resourceType, uint256 price) internal {
        uint256[] storage prices = askPrices[resourceType];
        
        // Binary search to find insertion point
        uint256 left = 0;
        uint256 right = prices.length;
        
        while (left < right) {
            uint256 mid = (left + right) / 2;
            if (prices[mid] < price) {
                left = mid + 1;
            } else if (prices[mid] > price) {
                right = mid;
            } else {
                return; // Price already exists
            }
        }
        
        // Insert at position
        prices.push(price);
        for (uint256 i = prices.length - 1; i > left; i--) {
            prices[i] = prices[i - 1];
        }
        prices[left] = price;
    }
    
    function _removeBidPrice(ResourceType resourceType, uint256 price) internal {
        uint256[] storage prices = bidPrices[resourceType];
        for (uint256 i = 0; i < prices.length; i++) {
            if (prices[i] == price) {
                prices[i] = prices[prices.length - 1];
                prices.pop();
                break;
            }
        }
    }
    
    function _removeAskPrice(ResourceType resourceType, uint256 price) internal {
        uint256[] storage prices = askPrices[resourceType];
        for (uint256 i = 0; i < prices.length; i++) {
            if (prices[i] == price) {
                prices[i] = prices[prices.length - 1];
                prices.pop();
                break;
            }
        }
    }
    
    function _updateMarketData(ResourceType resourceType) internal {
        MarketData storage data = marketData[resourceType];
        
        // Update best bid/ask
        if (bidPrices[resourceType].length > 0) {
            data.bestBid = bidPrices[resourceType][0];
        } else {
            data.bestBid = 0;
        }
        
        if (askPrices[resourceType].length > 0) {
            data.bestAsk = askPrices[resourceType][0];
        } else {
            data.bestAsk = 0;
        }
        
        // Calculate spread
        if (data.bestBid > 0 && data.bestAsk > 0) {
            data.spread = data.bestAsk - data.bestBid;
        } else {
            data.spread = 0;
        }
        
        data.lastUpdateTime = block.timestamp;
        
        emit MarketDataUpdated(resourceType, data.lastPrice, data.volume24h);
    }
    
    function _checkCircuitBreaker(ResourceType resourceType, uint256 newPrice) internal {
        MarketData storage data = marketData[resourceType];
        
        if (data.lastPrice > 0) {
            uint256 priceMovement = newPrice > data.lastPrice ?
                ((newPrice - data.lastPrice) * 10000) / data.lastPrice :
                ((data.lastPrice - newPrice) * 10000) / data.lastPrice;
                
            if (priceMovement > priceMovementLimit) {
                lastCircuitBreaker[resourceType] = block.timestamp;
                _pause();
                emit CircuitBreakerTriggered(resourceType, priceMovement);
            }
        }
        
        data.lastPrice = newPrice;
    }
    
    function _getAvailableQuantity(
        ResourceType resourceType,
        OrderSide side,
        uint256 priceLimit
    ) internal view returns (uint256) {
        uint256 totalQuantity = 0;
        
        if (side == OrderSide.BUY) {
            uint256[] memory prices = bidPrices[resourceType];
            for (uint256 i = 0; i < prices.length; i++) {
                if (prices[i] < priceLimit) break;
                
                uint256[] memory orderIds = orderBook[resourceType][side][prices[i]];
                for (uint256 j = 0; j < orderIds.length; j++) {
                    Order memory order = orders[orderIds[j]];
                    if (order.status == OrderStatus.OPEN || order.status == OrderStatus.PARTIALLY_FILLED) {
                        totalQuantity += order.quantity - order.filledQuantity;
                    }
                }
            }
        } else {
            uint256[] memory prices = askPrices[resourceType];
            for (uint256 i = 0; i < prices.length; i++) {
                if (prices[i] > priceLimit) break;
                
                uint256[] memory orderIds = orderBook[resourceType][side][prices[i]];
                for (uint256 j = 0; j < orderIds.length; j++) {
                    Order memory order = orders[orderIds[j]];
                    if (order.status == OrderStatus.OPEN || order.status == OrderStatus.PARTIALLY_FILLED) {
                        totalQuantity += order.quantity - order.filledQuantity;
                    }
                }
            }
        }
        
        return totalQuantity;
    }
    
    function _calculateRebate(address trader) internal view returns (uint256) {
        // Simplified rebate calculation
        return 0;
    }
    
    function uint2str(uint256 _i) internal pure returns (string memory) {
        if (_i == 0) {
            return "0";
        }
        uint256 j = _i;
        uint256 length;
        while (j != 0) {
            length++;
            j /= 10;
        }
        bytes memory bstr = new bytes(length);
        uint256 k = length;
        while (_i != 0) {
            k = k - 1;
            uint8 temp = (48 + uint8(_i - _i / 10 * 10));
            bytes1 b1 = bytes1(temp);
            bstr[k] = b1;
            _i /= 10;
        }
        return string(bstr);
    }
    
    function _authorizeUpgrade(address newImplementation) internal override onlyRole(DEFAULT_ADMIN_ROLE) {}
} 