// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";

interface IComputeOracle {
    function getResourcePrice(uint256 resourceId) external view returns (uint256);
    function getQualityScore(uint256 resourceId) external view returns (uint256, uint256);
}

/**
 * @title ComputeDerivativesFactory
 * @notice Factory for creating futures and options on compute resources
 */
contract ComputeDerivativesFactory is AccessControl, ReentrancyGuard {
    bytes32 public constant OPERATOR_ROLE = keccak256("OPERATOR_ROLE");
    bytes32 public constant SETTLER_ROLE = keccak256("SETTLER_ROLE");
    
    // Contract types
    enum ContractType { FUTURE, CALL_OPTION, PUT_OPTION }
    enum SettlementType { PHYSICAL, CASH }
    
    // Future contract structure
    struct ComputeFuture {
        string resourceType;
        string resourceSpecs;
        uint256 quantity;
        uint256 deliveryTime;
        uint256 strikePrice;
        address longParty;
        address shortParty;
        uint256 margin;
        SettlementType settlementType;
        bool settled;
        uint256 createdAt;
    }
    
    // Option contract structure
    struct ComputeOption {
        string resourceType;
        string resourceSpecs;
        ContractType optionType;
        uint256 strikePrice;
        uint256 quantity;
        uint256 expiration;
        uint256 premium;
        address writer;
        address holder;
        bool american; // American or European style
        bool exercised;
        uint256 createdAt;
    }
    
    // State
    IERC20 public immutable settlementToken;
    IComputeOracle public immutable oracle;
    
    mapping(string => ComputeFuture) public futures;
    mapping(string => ComputeOption) public options;
    mapping(address => uint256) public marginBalances;
    
    uint256 public futureCounter;
    uint256 public optionCounter;
    
    // Parameters
    uint256 public minMarginRate = 500; // 5%
    uint256 public maintenanceMarginRate = 300; // 3%
    uint256 public settlementFee = 10; // 0.1%
    uint256 public constant PRECISION = 10000;
    
    // Events
    event FutureCreated(
        string indexed futureId,
        string resourceType,
        uint256 quantity,
        uint256 deliveryTime,
        uint256 margin
    );
    
    event OptionCreated(
        string indexed optionId,
        ContractType optionType,
        uint256 strikePrice,
        uint256 expiration,
        uint256 premium
    );
    
    event FutureSettled(string indexed futureId, uint256 settlementPrice);
    event OptionExercised(string indexed optionId, uint256 spotPrice);
    event MarginCall(address indexed account, uint256 required);
    
    constructor(address _settlementToken, address _oracle) {
        settlementToken = IERC20(_settlementToken);
        oracle = IComputeOracle(_oracle);
        
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(OPERATOR_ROLE, msg.sender);
        _grantRole(SETTLER_ROLE, msg.sender);
    }
    
    /**
     * @notice Create a compute future contract
     * @param resourceType Type of compute resource
     * @param resourceSpecs JSON specs for the resource
     * @param quantity Amount of resources
     * @param deliveryTimestamp Delivery timestamp
     * @param isPhysical Physical or cash settlement
     * @param initialMargin Initial margin amount
     * @return futureId Created future ID
     */
    function createComputeFuture(
        string memory resourceType,
        string memory resourceSpecs,
        uint256 quantity,
        uint256 deliveryTimestamp,
        bool isPhysical,
        uint256 initialMargin
    ) external nonReentrant returns (string memory futureId) {
        require(deliveryTimestamp > block.timestamp, "Invalid delivery time");
        require(quantity > 0, "Invalid quantity");
        
        // Calculate required margin
        uint256 notionalValue = _estimateNotionalValue(resourceType, quantity);
        uint256 requiredMargin = notionalValue * minMarginRate / PRECISION;
        require(initialMargin >= requiredMargin, "Insufficient margin");
        
        // Transfer margin
        require(
            settlementToken.transferFrom(msg.sender, address(this), initialMargin),
            "Margin transfer failed"
        );
        
        marginBalances[msg.sender] += initialMargin;
        
        // Create future
        futureId = string(abi.encodePacked("F", _toString(++futureCounter)));
        
        futures[futureId] = ComputeFuture({
            resourceType: resourceType,
            resourceSpecs: resourceSpecs,
            quantity: quantity,
            deliveryTime: deliveryTimestamp,
            strikePrice: notionalValue / quantity, // Current price as initial strike
            longParty: msg.sender,
            shortParty: address(0), // To be filled
            margin: initialMargin,
            settlementType: isPhysical ? SettlementType.PHYSICAL : SettlementType.CASH,
            settled: false,
            createdAt: block.timestamp
        });
        
        emit FutureCreated(
            futureId,
            resourceType,
            quantity,
            deliveryTimestamp,
            initialMargin
        );
    }
    
    /**
     * @notice Create a compute option contract
     * @param resourceType Type of compute resource
     * @param resourceSpecs JSON specs for the resource
     * @param isCall Call or put option
     * @param strikePrice Strike price per unit
     * @param quantity Number of units
     * @param expirationTimestamp Expiration timestamp
     * @param isAmerican American or European style
     * @param premium Option premium
     * @return optionId Created option ID
     */
    function createComputeOption(
        string memory resourceType,
        string memory resourceSpecs,
        ContractType optionType,
        uint256 strikePrice,
        uint256 quantity,
        uint256 expirationTimestamp,
        bool isAmerican,
        uint256 premium
    ) external nonReentrant returns (string memory optionId) {
        require(expirationTimestamp > block.timestamp, "Invalid expiration");
        require(quantity > 0, "Invalid quantity");
        require(strikePrice > 0, "Invalid strike");
        require(
            optionType == ContractType.CALL_OPTION || 
            optionType == ContractType.PUT_OPTION,
            "Invalid option type"
        );
        
        // Transfer premium from buyer
        require(
            settlementToken.transferFrom(msg.sender, address(this), premium),
            "Premium transfer failed"
        );
        
        // Create option
        optionId = string(abi.encodePacked("O", _toString(++optionCounter)));
        
        options[optionId] = ComputeOption({
            resourceType: resourceType,
            resourceSpecs: resourceSpecs,
            optionType: optionType,
            strikePrice: strikePrice,
            quantity: quantity,
            expiration: expirationTimestamp,
            premium: premium,
            writer: address(0), // To be filled by writer
            holder: msg.sender,
            american: isAmerican,
            exercised: false,
            createdAt: block.timestamp
        });
        
        emit OptionCreated(
            optionId,
            optionType,
            strikePrice,
            expirationTimestamp,
            premium
        );
    }
    
    /**
     * @notice Take the short side of a future
     * @param futureId Future contract ID
     * @param margin Margin to post
     */
    function takeFutureShort(
        string memory futureId,
        uint256 margin
    ) external nonReentrant {
        ComputeFuture storage future = futures[futureId];
        require(future.createdAt > 0, "Future not found");
        require(future.shortParty == address(0), "Already filled");
        require(!future.settled, "Already settled");
        
        // Check margin requirement
        uint256 notionalValue = future.strikePrice * future.quantity;
        uint256 requiredMargin = notionalValue * minMarginRate / PRECISION;
        require(margin >= requiredMargin, "Insufficient margin");
        
        // Transfer margin
        require(
            settlementToken.transferFrom(msg.sender, address(this), margin),
            "Margin transfer failed"
        );
        
        marginBalances[msg.sender] += margin;
        future.shortParty = msg.sender;
        future.margin += margin;
    }
    
    /**
     * @notice Write an option (take short position)
     * @param optionId Option contract ID
     * @param collateral Collateral to post
     */
    function writeOption(
        string memory optionId,
        uint256 collateral
    ) external nonReentrant {
        ComputeOption storage option = options[optionId];
        require(option.createdAt > 0, "Option not found");
        require(option.writer == address(0), "Already written");
        require(!option.exercised, "Already exercised");
        require(block.timestamp < option.expiration, "Expired");
        
        // Check collateral requirement
        uint256 requiredCollateral;
        if (option.optionType == ContractType.CALL_OPTION) {
            // For calls, need to cover potential upside
            requiredCollateral = option.strikePrice * option.quantity * 150 / 100; // 150% of strike
        } else {
            // For puts, need to cover strike price
            requiredCollateral = option.strikePrice * option.quantity;
        }
        
        require(collateral >= requiredCollateral, "Insufficient collateral");
        
        // Transfer collateral
        require(
            settlementToken.transferFrom(msg.sender, address(this), collateral),
            "Collateral transfer failed"
        );
        
        // Transfer premium to writer
        require(
            settlementToken.transfer(msg.sender, option.premium),
            "Premium transfer failed"
        );
        
        marginBalances[msg.sender] += collateral;
        option.writer = msg.sender;
    }
    
    /**
     * @notice Exercise an option
     * @param optionId Option to exercise
     */
    function exerciseOption(string memory optionId) external nonReentrant {
        ComputeOption storage option = options[optionId];
        require(option.createdAt > 0, "Option not found");
        require(option.holder == msg.sender, "Not holder");
        require(!option.exercised, "Already exercised");
        require(block.timestamp < option.expiration, "Expired");
        
        // For European options, can only exercise at expiration
        if (!option.american) {
            require(
                block.timestamp >= option.expiration - 1 hours,
                "European option not at expiration"
            );
        }
        
        // Get current spot price
        uint256 spotPrice = _getCurrentPrice(option.resourceType);
        
        // Check if exercise is profitable
        bool profitable = false;
        uint256 payoff = 0;
        
        if (option.optionType == ContractType.CALL_OPTION) {
            if (spotPrice > option.strikePrice) {
                profitable = true;
                payoff = (spotPrice - option.strikePrice) * option.quantity;
            }
        } else {
            if (spotPrice < option.strikePrice) {
                profitable = true;
                payoff = (option.strikePrice - spotPrice) * option.quantity;
            }
        }
        
        require(profitable, "Not profitable to exercise");
        
        // Execute settlement
        option.exercised = true;
        
        // Return unused collateral to writer
        uint256 writerCollateral = marginBalances[option.writer];
        uint256 remainingCollateral = writerCollateral > payoff ? 
            writerCollateral - payoff : 0;
        
        if (remainingCollateral > 0) {
            marginBalances[option.writer] -= remainingCollateral;
            require(
                settlementToken.transfer(option.writer, remainingCollateral),
                "Collateral return failed"
            );
        }
        
        // Pay holder
        require(
            settlementToken.transfer(option.holder, payoff),
            "Payoff transfer failed"
        );
        
        emit OptionExercised(optionId, spotPrice);
    }
    
    /**
     * @notice Settle a future at delivery
     * @param futureId Future to settle
     */
    function settleFuture(string memory futureId) external nonReentrant onlyRole(SETTLER_ROLE) {
        ComputeFuture storage future = futures[futureId];
        require(future.createdAt > 0, "Future not found");
        require(!future.settled, "Already settled");
        require(block.timestamp >= future.deliveryTime, "Not at delivery");
        require(future.shortParty != address(0), "No short party");
        
        // Get settlement price
        uint256 settlementPrice = _getCurrentPrice(future.resourceType);
        
        // Calculate P&L
        uint256 notionalValue = future.quantity * future.strikePrice;
        uint256 settlementValue = future.quantity * settlementPrice;
        
        uint256 profit;
        address winner;
        address loser;
        
        if (settlementValue > notionalValue) {
            // Long wins
            profit = settlementValue - notionalValue;
            winner = future.longParty;
            loser = future.shortParty;
        } else {
            // Short wins
            profit = notionalValue - settlementValue;
            winner = future.shortParty;
            loser = future.longParty;
        }
        
        // Apply settlement fee
        uint256 fee = profit * settlementFee / PRECISION;
        uint256 netProfit = profit - fee;
        
        // Settle
        future.settled = true;
        
        // Return margins and pay profit
        uint256 longMargin = marginBalances[future.longParty];
        uint256 shortMargin = marginBalances[future.shortParty];
        
        marginBalances[future.longParty] = 0;
        marginBalances[future.shortParty] = 0;
        
        if (winner == future.longParty) {
            require(
                settlementToken.transfer(future.longParty, longMargin + netProfit),
                "Long settlement failed"
            );
            require(
                settlementToken.transfer(future.shortParty, shortMargin > profit ? shortMargin - profit : 0),
                "Short settlement failed"
            );
        } else {
            require(
                settlementToken.transfer(future.shortParty, shortMargin + netProfit),
                "Short settlement failed"
            );
            require(
                settlementToken.transfer(future.longParty, longMargin > profit ? longMargin - profit : 0),
                "Long settlement failed"
            );
        }
        
        emit FutureSettled(futureId, settlementPrice);
    }
    
    /**
     * @notice Check and execute margin calls
     * @param futureId Future to check
     */
    function checkMarginCall(string memory futureId) external {
        ComputeFuture storage future = futures[futureId];
        require(future.createdAt > 0, "Future not found");
        require(!future.settled, "Already settled");
        
        uint256 currentPrice = _getCurrentPrice(future.resourceType);
        uint256 notionalValue = future.quantity * currentPrice;
        uint256 requiredMaintenance = notionalValue * maintenanceMarginRate / PRECISION;
        
        // Check long party
        if (marginBalances[future.longParty] < requiredMaintenance) {
            emit MarginCall(future.longParty, requiredMaintenance);
        }
        
        // Check short party
        if (future.shortParty != address(0) && 
            marginBalances[future.shortParty] < requiredMaintenance) {
            emit MarginCall(future.shortParty, requiredMaintenance);
        }
    }
    
    /**
     * @notice Add margin to position
     * @param amount Amount to add
     */
    function addMargin(uint256 amount) external nonReentrant {
        require(amount > 0, "Zero amount");
        
        require(
            settlementToken.transferFrom(msg.sender, address(this), amount),
            "Transfer failed"
        );
        
        marginBalances[msg.sender] += amount;
    }
    
    // Internal functions
    
    function _estimateNotionalValue(
        string memory resourceType,
        uint256 quantity
    ) internal view returns (uint256) {
        // Simplified estimation
        uint256 basePrice = _getCurrentPrice(resourceType);
        return basePrice * quantity;
    }
    
    function _getCurrentPrice(string memory resourceType) internal view returns (uint256) {
        // In production, would query oracle with proper resource ID
        // For now, return mock prices
        bytes32 typeHash = keccak256(bytes(resourceType));
        
        if (typeHash == keccak256("quantum")) {
            return 500 * 1e18; // $500
        } else if (typeHash == keccak256("ai")) {
            return 100 * 1e18; // $100
        } else if (typeHash == keccak256("network")) {
            return 50 * 1e18; // $50
        }
        
        return 100 * 1e18; // Default
    }
    
    function _toString(uint256 value) internal pure returns (string memory) {
        if (value == 0) return "0";
        
        uint256 temp = value;
        uint256 digits;
        
        while (temp != 0) {
            digits++;
            temp /= 10;
        }
        
        bytes memory buffer = new bytes(digits);
        
        while (value != 0) {
            digits -= 1;
            buffer[digits] = bytes1(uint8(48 + value % 10));
            value /= 10;
        }
        
        return string(buffer);
    }
} 