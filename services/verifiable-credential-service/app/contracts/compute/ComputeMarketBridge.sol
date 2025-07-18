// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts-upgradeable/access/AccessControlUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/security/PausableUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/security/ReentrancyGuardUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";

interface IComputeFutures {
    function createFuturesContract(
        address seller,
        uint8 resourceType,
        uint256 quantity,
        uint256 price,
        uint256 deliveryTime,
        uint256 duration,
        string memory resourceSpec,
        uint256 slaLevel
    ) external returns (uint256);
}

interface IComputeOptions {
    function writeOption(
        uint8 optionType,
        uint8 exerciseStyle,
        uint256 strikePrice,
        uint256 quantity,
        uint256 expiry,
        uint8 resourceType,
        uint256 premium
    ) external returns (uint256);
}

interface ILayerZeroEndpoint {
    function send(
        uint16 _dstChainId,
        bytes calldata _destination,
        bytes calldata _payload,
        address payable _refundAddress,
        address _zroPaymentAddress,
        bytes calldata _adapterParams
    ) external payable;
    
    function receivePayload(
        uint16 _srcChainId,
        bytes calldata _srcAddress,
        address _dstAddress,
        uint64 _nonce,
        uint _gasLimit,
        bytes calldata _payload
    ) external;
}

/**
 * @title ComputeMarketBridge
 * @notice Cross-chain bridge for compute markets enabling multi-chain trading and settlement
 * @dev Uses LayerZero for cross-chain messaging and state synchronization
 */
contract ComputeMarketBridge is 
    Initializable,
    AccessControlUpgradeable,
    PausableUpgradeable,
    ReentrancyGuardUpgradeable,
    UUPSUpgradeable 
{
    bytes32 public constant OPERATOR_ROLE = keccak256("OPERATOR_ROLE");
    bytes32 public constant RELAYER_ROLE = keccak256("RELAYER_ROLE");
    
    enum MessageType {
        FUTURES_ORDER,
        OPTIONS_ORDER,
        SETTLEMENT,
        COLLATERAL_TRANSFER,
        STATE_SYNC,
        EMERGENCY_PAUSE
    }
    
    enum ChainId {
        ETHEREUM,
        POLYGON,
        ARBITRUM,
        OPTIMISM,
        BSC,
        AVALANCHE
    }
    
    struct CrossChainMessage {
        uint256 messageId;
        MessageType messageType;
        uint16 sourceChainId;
        uint16 destChainId;
        address sender;
        bytes payload;
        uint256 timestamp;
        bool processed;
        bytes32 payloadHash;
    }
    
    struct ChainConfig {
        uint16 lzChainId;
        address computeFutures;
        address computeOptions;
        address settlementToken;
        bool isActive;
        uint256 gasLimit;
        uint256 minGasPrice;
    }
    
    struct PendingOrder {
        uint256 orderId;
        address user;
        uint8 orderType; // 0=futures, 1=options
        bytes orderData;
        uint256 collateralAmount;
        uint16 originChain;
        uint16 executionChain;
        bool isExecuted;
        uint256 timestamp;
    }
    
    // State variables
    ILayerZeroEndpoint public lzEndpoint;
    uint256 public nextMessageId;
    uint256 public nextOrderId;
    
    mapping(uint16 => ChainConfig) public chainConfigs;
    mapping(uint256 => CrossChainMessage) public messages;
    mapping(uint256 => PendingOrder) public pendingOrders;
    mapping(bytes32 => bool) public processedMessages;
    
    // User balances across chains
    mapping(address => mapping(uint16 => uint256)) public userBalances;
    mapping(address => uint256) public totalUserBalance;
    
    // Cross-chain order book references
    mapping(uint16 => mapping(uint256 => uint256)) public crossChainOrderBook; // chainId -> localOrderId -> globalOrderId
    
    // Bridge fees and parameters
    uint256 public bridgeFee = 10; // 0.1% in basis points
    uint256 public minBridgeAmount = 1e18; // Minimum 1 token
    uint256 public maxBridgeAmount = 1e24; // Maximum 1M tokens
    uint256 public bridgeDelay = 5 minutes; // Security delay
    
    // Nonce tracking for replay protection
    mapping(address => uint256) public userNonces;
    
    // Events
    event MessageSent(
        uint256 indexed messageId,
        MessageType messageType,
        uint16 indexed sourceChain,
        uint16 indexed destChain,
        address sender
    );
    
    event MessageReceived(
        uint256 indexed messageId,
        MessageType messageType,
        uint16 indexed sourceChain,
        address sender
    );
    
    event CrossChainOrderCreated(
        uint256 indexed orderId,
        address indexed user,
        uint16 originChain,
        uint16 executionChain,
        uint8 orderType
    );
    
    event OrderExecuted(
        uint256 indexed orderId,
        uint16 indexed executionChain,
        uint256 localOrderId
    );
    
    event CollateralBridged(
        address indexed user,
        uint16 indexed fromChain,
        uint16 indexed toChain,
        uint256 amount
    );
    
    event ChainConfigured(
        uint16 indexed chainId,
        address computeFutures,
        address computeOptions
    );
    
    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }
    
    function initialize(address _lzEndpoint) public initializer {
        __AccessControl_init();
        __Pausable_init();
        __ReentrancyGuard_init();
        __UUPSUpgradeable_init();
        
        lzEndpoint = ILayerZeroEndpoint(_lzEndpoint);
        
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(OPERATOR_ROLE, msg.sender);
    }
    
    /**
     * @notice Configure a chain for cross-chain operations
     */
    function configureChain(
        uint16 chainId,
        uint16 lzChainId,
        address computeFutures,
        address computeOptions,
        address settlementToken,
        uint256 gasLimit
    ) external onlyRole(OPERATOR_ROLE) {
        chainConfigs[chainId] = ChainConfig({
            lzChainId: lzChainId,
            computeFutures: computeFutures,
            computeOptions: computeOptions,
            settlementToken: settlementToken,
            isActive: true,
            gasLimit: gasLimit,
            minGasPrice: 1 gwei
        });
        
        emit ChainConfigured(chainId, computeFutures, computeOptions);
    }
    
    /**
     * @notice Create a cross-chain futures order
     */
    function createCrossChainFuturesOrder(
        uint16 executionChain,
        address seller,
        uint8 resourceType,
        uint256 quantity,
        uint256 price,
        uint256 deliveryTime,
        uint256 duration,
        string memory resourceSpec,
        uint256 slaLevel,
        uint256 collateral
    ) external payable nonReentrant whenNotPaused returns (uint256) {
        require(chainConfigs[executionChain].isActive, "Chain not active");
        require(msg.value >= estimateBridgeFee(executionChain), "Insufficient fee");
        
        uint256 orderId = nextOrderId++;
        
        // Encode order data
        bytes memory orderData = abi.encode(
            seller,
            resourceType,
            quantity,
            price,
            deliveryTime,
            duration,
            resourceSpec,
            slaLevel
        );
        
        pendingOrders[orderId] = PendingOrder({
            orderId: orderId,
            user: msg.sender,
            orderType: 0, // futures
            orderData: orderData,
            collateralAmount: collateral,
            originChain: uint16(block.chainid),
            executionChain: executionChain,
            isExecuted: false,
            timestamp: block.timestamp
        });
        
        // Send cross-chain message
        _sendCrossChainMessage(
            executionChain,
            MessageType.FUTURES_ORDER,
            abi.encode(orderId, orderData, collateral)
        );
        
        emit CrossChainOrderCreated(orderId, msg.sender, uint16(block.chainid), executionChain, 0);
        
        return orderId;
    }
    
    /**
     * @notice Create a cross-chain options order
     */
    function createCrossChainOptionsOrder(
        uint16 executionChain,
        uint8 optionType,
        uint8 exerciseStyle,
        uint256 strikePrice,
        uint256 quantity,
        uint256 expiry,
        uint8 resourceType,
        uint256 premium,
        uint256 collateral
    ) external payable nonReentrant whenNotPaused returns (uint256) {
        require(chainConfigs[executionChain].isActive, "Chain not active");
        require(msg.value >= estimateBridgeFee(executionChain), "Insufficient fee");
        
        uint256 orderId = nextOrderId++;
        
        // Encode order data
        bytes memory orderData = abi.encode(
            optionType,
            exerciseStyle,
            strikePrice,
            quantity,
            expiry,
            resourceType,
            premium
        );
        
        pendingOrders[orderId] = PendingOrder({
            orderId: orderId,
            user: msg.sender,
            orderType: 1, // options
            orderData: orderData,
            collateralAmount: collateral,
            originChain: uint16(block.chainid),
            executionChain: executionChain,
            isExecuted: false,
            timestamp: block.timestamp
        });
        
        // Send cross-chain message
        _sendCrossChainMessage(
            executionChain,
            MessageType.OPTIONS_ORDER,
            abi.encode(orderId, orderData, collateral)
        );
        
        emit CrossChainOrderCreated(orderId, msg.sender, uint16(block.chainid), executionChain, 1);
        
        return orderId;
    }
    
    /**
     * @notice Bridge collateral between chains
     */
    function bridgeCollateral(
        uint16 destChain,
        uint256 amount
    ) external payable nonReentrant whenNotPaused {
        require(chainConfigs[destChain].isActive, "Chain not active");
        require(amount >= minBridgeAmount && amount <= maxBridgeAmount, "Invalid amount");
        require(msg.value >= estimateBridgeFee(destChain), "Insufficient fee");
        
        // Deduct from source chain balance
        require(userBalances[msg.sender][uint16(block.chainid)] >= amount, "Insufficient balance");
        userBalances[msg.sender][uint16(block.chainid)] -= amount;
        
        // Apply bridge fee
        uint256 fee = (amount * bridgeFee) / 10000;
        uint256 netAmount = amount - fee;
        
        // Send cross-chain message
        _sendCrossChainMessage(
            destChain,
            MessageType.COLLATERAL_TRANSFER,
            abi.encode(msg.sender, netAmount)
        );
        
        emit CollateralBridged(msg.sender, uint16(block.chainid), destChain, netAmount);
    }
    
    /**
     * @notice Receive and process cross-chain messages
     */
    function lzReceive(
        uint16 _srcChainId,
        bytes memory _srcAddress,
        uint64 _nonce,
        bytes memory _payload
    ) external {
        require(msg.sender == address(lzEndpoint), "Invalid endpoint caller");
        
        // Decode message
        (uint256 messageId, MessageType messageType, bytes memory data) = abi.decode(
            _payload,
            (uint256, MessageType, bytes)
        );
        
        // Check for replay
        bytes32 messageHash = keccak256(abi.encode(_srcChainId, messageId, _nonce));
        require(!processedMessages[messageHash], "Message already processed");
        processedMessages[messageHash] = true;
        
        // Store message
        messages[messageId] = CrossChainMessage({
            messageId: messageId,
            messageType: messageType,
            sourceChainId: _srcChainId,
            destChainId: uint16(block.chainid),
            sender: address(0), // Would decode from _srcAddress
            payload: data,
            timestamp: block.timestamp,
            processed: false,
            payloadHash: keccak256(data)
        });
        
        // Process based on message type
        if (messageType == MessageType.FUTURES_ORDER) {
            _processFuturesOrder(messageId, data);
        } else if (messageType == MessageType.OPTIONS_ORDER) {
            _processOptionsOrder(messageId, data);
        } else if (messageType == MessageType.COLLATERAL_TRANSFER) {
            _processCollateralTransfer(data);
        } else if (messageType == MessageType.SETTLEMENT) {
            _processSettlement(data);
        } else if (messageType == MessageType.STATE_SYNC) {
            _processStateSync(data);
        } else if (messageType == MessageType.EMERGENCY_PAUSE) {
            _processEmergencyPause();
        }
        
        messages[messageId].processed = true;
        
        emit MessageReceived(messageId, messageType, _srcChainId, address(0));
    }
    
    /**
     * @notice Sync state across chains
     */
    function syncState(uint16[] calldata chains) external onlyRole(OPERATOR_ROLE) {
        bytes memory stateData = _collectStateData();
        
        for (uint256 i = 0; i < chains.length; i++) {
            if (chains[i] != uint16(block.chainid)) {
                _sendCrossChainMessage(
                    chains[i],
                    MessageType.STATE_SYNC,
                    stateData
                );
            }
        }
    }
    
    /**
     * @notice Emergency pause all chains
     */
    function emergencyPauseAllChains(uint16[] calldata chains) external onlyRole(OPERATOR_ROLE) {
        // Pause local
        _pause();
        
        // Send pause message to all chains
        for (uint256 i = 0; i < chains.length; i++) {
            if (chains[i] != uint16(block.chainid)) {
                _sendCrossChainMessage(
                    chains[i],
                    MessageType.EMERGENCY_PAUSE,
                    ""
                );
            }
        }
    }
    
    /**
     * @notice Estimate bridge fee for a destination chain
     */
    function estimateBridgeFee(uint16 destChain) public view returns (uint256) {
        ChainConfig memory config = chainConfigs[destChain];
        return config.gasLimit * config.minGasPrice;
    }
    
    // Internal functions
    
    function _sendCrossChainMessage(
        uint16 destChain,
        MessageType messageType,
        bytes memory data
    ) internal {
        uint256 messageId = nextMessageId++;
        ChainConfig memory config = chainConfigs[destChain];
        
        bytes memory payload = abi.encode(messageId, messageType, data);
        bytes memory adapterParams = abi.encodePacked(uint16(1), config.gasLimit);
        
        lzEndpoint.send{value: msg.value}(
            config.lzChainId,
            abi.encodePacked(address(this)),
            payload,
            payable(msg.sender),
            address(0),
            adapterParams
        );
        
        emit MessageSent(messageId, messageType, uint16(block.chainid), destChain, msg.sender);
    }
    
    function _processFuturesOrder(uint256 messageId, bytes memory data) internal {
        (uint256 orderId, bytes memory orderData, uint256 collateral) = abi.decode(
            data,
            (uint256, bytes, uint256)
        );
        
        // Decode order parameters
        (
            address seller,
            uint8 resourceType,
            uint256 quantity,
            uint256 price,
            uint256 deliveryTime,
            uint256 duration,
            string memory resourceSpec,
            uint256 slaLevel
        ) = abi.decode(
            orderData,
            (address, uint8, uint256, uint256, uint256, uint256, string, uint256)
        );
        
        // Execute on local futures contract
        ChainConfig memory config = chainConfigs[uint16(block.chainid)];
        uint256 localOrderId = IComputeFutures(config.computeFutures).createFuturesContract(
            seller,
            resourceType,
            quantity,
            price,
            deliveryTime,
            duration,
            resourceSpec,
            slaLevel
        );
        
        // Map cross-chain order
        crossChainOrderBook[uint16(block.chainid)][localOrderId] = orderId;
        
        emit OrderExecuted(orderId, uint16(block.chainid), localOrderId);
    }
    
    function _processOptionsOrder(uint256 messageId, bytes memory data) internal {
        (uint256 orderId, bytes memory orderData, uint256 collateral) = abi.decode(
            data,
            (uint256, bytes, uint256)
        );
        
        // Decode order parameters
        (
            uint8 optionType,
            uint8 exerciseStyle,
            uint256 strikePrice,
            uint256 quantity,
            uint256 expiry,
            uint8 resourceType,
            uint256 premium
        ) = abi.decode(
            orderData,
            (uint8, uint8, uint256, uint256, uint256, uint8, uint256)
        );
        
        // Execute on local options contract
        ChainConfig memory config = chainConfigs[uint16(block.chainid)];
        uint256 localOptionId = IComputeOptions(config.computeOptions).writeOption(
            optionType,
            exerciseStyle,
            strikePrice,
            quantity,
            expiry,
            resourceType,
            premium
        );
        
        // Map cross-chain order
        crossChainOrderBook[uint16(block.chainid)][localOptionId] = orderId;
        
        emit OrderExecuted(orderId, uint16(block.chainid), localOptionId);
    }
    
    function _processCollateralTransfer(bytes memory data) internal {
        (address user, uint256 amount) = abi.decode(data, (address, uint256));
        
        // Credit user balance on destination chain
        userBalances[user][uint16(block.chainid)] += amount;
        totalUserBalance[user] += amount;
    }
    
    function _processSettlement(bytes memory data) internal {
        // Process cross-chain settlement
        // Implementation depends on settlement mechanism
    }
    
    function _processStateSync(bytes memory data) internal {
        // Sync state data from other chains
        // Implementation depends on what state needs syncing
    }
    
    function _processEmergencyPause() internal {
        _pause();
    }
    
    function _collectStateData() internal view returns (bytes memory) {
        // Collect relevant state for synchronization
        return abi.encode(
            nextOrderId,
            block.timestamp,
            totalUserBalance[address(0)] // Example data
        );
    }
    
    function _authorizeUpgrade(address newImplementation) internal override onlyRole(DEFAULT_ADMIN_ROLE) {}
} 