// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/security/Pausable.sol";
import "@openzeppelin/contracts/utils/math/SafeMath.sol";
import "./ExtendedResourceToken.sol";

/**
 * @title NetworkBandwidthExchange
 * @notice Exchange for trading network bandwidth with QoS guarantees
 * @dev Implements real-time bandwidth auctions and path optimization
 */
contract NetworkBandwidthExchange is AccessControl, ReentrancyGuard, Pausable {
    using SafeMath for uint256;
    
    // Roles
    bytes32 public constant NETWORK_OPERATOR_ROLE = keccak256("NETWORK_OPERATOR_ROLE");
    bytes32 public constant PATH_ORACLE_ROLE = keccak256("PATH_ORACLE_ROLE");
    bytes32 public constant QOS_MONITOR_ROLE = keccak256("QOS_MONITOR_ROLE");
    
    // Constants
    uint256 public constant MIN_BANDWIDTH = 1; // Minimum 1 Gbps
    uint256 public constant MAX_LATENCY = 1000; // Maximum 1000ms
    uint256 public constant CONGESTION_UPDATE_INTERVAL = 5 minutes;
    uint256 public constant SLA_VIOLATION_PENALTY = 1000; // 10% penalty
    
    // Contracts
    ExtendedResourceToken public immutable resourceToken;
    
    // QoS Classes
    enum QoSClass {
        BestEffort,     // No guarantees
        Bronze,         // Basic guarantees
        Silver,         // Standard guarantees
        Gold,           // Premium guarantees
        Platinum        // Ultra-low latency
    }
    
    // Network Path struct
    struct NetworkPath {
        string sourcePOP;              // Source point of presence
        string destinationPOP;         // Destination point of presence
        address provider;              // Path provider
        uint256 totalBandwidth;        // Total bandwidth capacity (Gbps)
        uint256 availableBandwidth;    // Currently available (Gbps)
        uint256 baseLatency;           // Base latency (ms)
        uint256 currentLatency;        // Current latency with congestion (ms)
        uint256 packetLossRate;        // Packet loss rate (basis points)
        uint256 pathDiversity;         // Number of diverse physical paths
        bool isActive;                 // Whether path is active
        uint256 lastCongestionUpdate;  // Last congestion measurement
        uint256 congestionFactor;      // Current congestion (0-10000)
    }
    
    // Bandwidth Allocation struct
    struct BandwidthAllocation {
        uint256 pathId;                // Network path ID
        address user;                  // User address
        uint256 bandwidth;             // Allocated bandwidth (Gbps)
        uint256 startTime;             // Allocation start time
        uint256 endTime;               // Allocation end time
        QoSClass qosClass;             // Quality of service class
        uint256 maxLatency;            // Maximum allowed latency (ms)
        uint256 price;                 // Total price
        bool isBurst;                  // Whether this is burst capacity
        uint256 tokenId;               // Resource token ID
        uint256 slaViolations;         // Number of SLA violations
    }
    
    // Latency Future Contract
    struct LatencyFuture {
        uint256[] pathIds;             // Network paths involved
        address buyer;                 // Future buyer
        uint256 maxLatency;            // Guaranteed max latency (ms)
        uint256 deliveryDate;          // When guarantee starts
        uint256 duration;              // Duration of guarantee
        uint256 premium;               // Premium paid
        bool isExercised;              // Whether future was exercised
        uint256 actualLatency;         // Actual latency achieved
    }
    
    // Circuit Reservation
    struct DedicatedCircuit {
        uint256 pathId;                // Primary path
        uint256[] backupPaths;         // Backup paths for redundancy
        address user;                  // Circuit user
        uint256 bandwidth;             // Reserved bandwidth (Gbps)
        uint256 startTime;             // Reservation start
        uint256 endTime;               // Reservation end
        uint256 maxLatency;            // Maximum latency guarantee
        uint256 uptime;                // Required uptime (basis points)
        uint256 price;                 // Total price
        bool isActive;                 // Whether circuit is active
    }
    
    // State variables
    mapping(uint256 => NetworkPath) public networkPaths;
    mapping(uint256 => BandwidthAllocation) public allocations;
    mapping(uint256 => LatencyFuture) public latencyFutures;
    mapping(uint256 => DedicatedCircuit) public dedicatedCircuits;
    
    uint256 public nextPathId;
    uint256 public nextAllocationId;
    uint256 public nextFutureId;
    uint256 public nextCircuitId;
    
    // Path performance tracking
    mapping(uint256 => uint256[]) public pathAllocationHistory; // Path ID => allocation IDs
    mapping(uint256 => uint256) public pathRevenue; // Path ID => total revenue
    mapping(uint256 => uint256) public pathUtilization; // Path ID => current utilization
    
    // Congestion pricing
    mapping(uint256 => uint256) public congestionPricing; // Path ID => price multiplier
    mapping(string => mapping(string => uint256[])) public popPairPaths; // Source => Dest => Path IDs
    
    // Pricing parameters
    uint256 public basePricePerGbpsHour = 100000000000000000; // 0.1 ETH per Gbps-hour
    mapping(QoSClass => uint256) public qosMultipliers;
    uint256 public burstPremium = 200; // 2x for burst
    uint256 public dedicatedCircuitPremium = 300; // 3x for dedicated
    
    // Events
    event PathRegistered(
        uint256 indexed pathId,
        string sourcePOP,
        string destinationPOP,
        address indexed provider,
        uint256 bandwidth
    );
    
    event BandwidthAllocated(
        uint256 indexed allocationId,
        uint256 indexed pathId,
        address indexed user,
        uint256 bandwidth,
        uint256 duration
    );
    
    event LatencyFutureCreated(
        uint256 indexed futureId,
        address indexed buyer,
        uint256 maxLatency,
        uint256 deliveryDate
    );
    
    event DedicatedCircuitCreated(
        uint256 indexed circuitId,
        uint256 indexed pathId,
        address indexed user,
        uint256 bandwidth,
        uint256 duration
    );
    
    event CongestionUpdated(
        uint256 indexed pathId,
        uint256 oldFactor,
        uint256 newFactor
    );
    
    event SLAViolation(
        uint256 indexed allocationId,
        uint256 actualLatency,
        uint256 guaranteedLatency
    );
    
    /**
     * @dev Constructor
     * @param _resourceToken ExtendedResourceToken contract address
     */
    constructor(address _resourceToken) {
        require(_resourceToken != address(0), "Invalid resource token");
        resourceToken = ExtendedResourceToken(_resourceToken);
        
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(NETWORK_OPERATOR_ROLE, msg.sender);
        _grantRole(PATH_ORACLE_ROLE, msg.sender);
        _grantRole(QOS_MONITOR_ROLE, msg.sender);
        
        // Initialize QoS multipliers
        qosMultipliers[QoSClass.BestEffort] = 100; // 1x
        qosMultipliers[QoSClass.Bronze] = 120; // 1.2x
        qosMultipliers[QoSClass.Silver] = 150; // 1.5x
        qosMultipliers[QoSClass.Gold] = 200; // 2x
        qosMultipliers[QoSClass.Platinum] = 300; // 3x
    }
    
    /**
     * @notice Register a network path
     * @param sourcePOP Source point of presence
     * @param destinationPOP Destination point of presence
     * @param totalBandwidth Total bandwidth capacity
     * @param baseLatency Base latency without congestion
     * @param pathDiversity Number of diverse paths
     */
    function registerPath(
        string memory sourcePOP,
        string memory destinationPOP,
        uint256 totalBandwidth,
        uint256 baseLatency,
        uint256 pathDiversity
    ) external onlyRole(NETWORK_OPERATOR_ROLE) returns (uint256) {
        require(totalBandwidth >= MIN_BANDWIDTH, "Insufficient bandwidth");
        require(baseLatency <= MAX_LATENCY, "Latency too high");
        require(bytes(sourcePOP).length > 0, "Invalid source POP");
        require(bytes(destinationPOP).length > 0, "Invalid destination POP");
        
        uint256 pathId = nextPathId++;
        
        networkPaths[pathId] = NetworkPath({
            sourcePOP: sourcePOP,
            destinationPOP: destinationPOP,
            provider: msg.sender,
            totalBandwidth: totalBandwidth,
            availableBandwidth: totalBandwidth,
            baseLatency: baseLatency,
            currentLatency: baseLatency,
            packetLossRate: 0,
            pathDiversity: pathDiversity,
            isActive: true,
            lastCongestionUpdate: block.timestamp,
            congestionFactor: 0
        });
        
        // Add to POP pair mapping
        popPairPaths[sourcePOP][destinationPOP].push(pathId);
        
        emit PathRegistered(pathId, sourcePOP, destinationPOP, msg.sender, totalBandwidth);
        
        return pathId;
    }
    
    /**
     * @notice Allocate bandwidth on a path
     * @param pathId Network path ID
     * @param bandwidth Bandwidth to allocate (Gbps)
     * @param duration Duration in seconds
     * @param qosClass Quality of service class
     * @param maxLatency Maximum acceptable latency
     */
    function allocateBandwidth(
        uint256 pathId,
        uint256 bandwidth,
        uint256 duration,
        QoSClass qosClass,
        uint256 maxLatency,
        uint256 tokenId
    ) external nonReentrant returns (uint256) {
        NetworkPath storage path = networkPaths[pathId];
        require(path.isActive, "Path not active");
        require(bandwidth <= path.availableBandwidth, "Insufficient bandwidth");
        require(duration >= 1 hours, "Duration too short");
        require(path.currentLatency <= maxLatency, "Path latency exceeds requirement");
        
        // Verify token ownership
        require(resourceToken.balanceOf(msg.sender, tokenId) > 0, "User doesn't own token");
        
        // Calculate price with congestion factor
        uint256 price = calculateBandwidthPrice(
            pathId,
            bandwidth,
            duration,
            qosClass,
            false // Not burst
        );
        
        uint256 allocationId = nextAllocationId++;
        
        allocations[allocationId] = BandwidthAllocation({
            pathId: pathId,
            user: msg.sender,
            bandwidth: bandwidth,
            startTime: block.timestamp,
            endTime: block.timestamp + duration,
            qosClass: qosClass,
            maxLatency: maxLatency,
            price: price,
            isBurst: false,
            tokenId: tokenId,
            slaViolations: 0
        });
        
        // Update path availability
        path.availableBandwidth = path.availableBandwidth.sub(bandwidth);
        pathAllocationHistory[pathId].push(allocationId);
        pathUtilization[pathId] = pathUtilization[pathId].add(bandwidth);
        
        // Update congestion if utilization is high
        updateCongestion(pathId);
        
        emit BandwidthAllocated(allocationId, pathId, msg.sender, bandwidth, duration);
        
        return allocationId;
    }
    
    /**
     * @notice Allocate burst bandwidth
     * @param pathId Network path ID
     * @param bandwidth Burst bandwidth needed
     * @param duration Burst duration (max 1 hour)
     */
    function allocateBurstBandwidth(
        uint256 pathId,
        uint256 bandwidth,
        uint256 duration
    ) external nonReentrant returns (uint256) {
        require(duration <= 1 hours, "Burst duration too long");
        
        NetworkPath storage path = networkPaths[pathId];
        require(path.isActive, "Path not active");
        
        // Burst can exceed available bandwidth up to 50%
        uint256 maxBurst = path.totalBandwidth.mul(150).div(100);
        uint256 currentUsage = path.totalBandwidth.sub(path.availableBandwidth);
        require(currentUsage.add(bandwidth) <= maxBurst, "Burst limit exceeded");
        
        // Calculate burst price
        uint256 price = calculateBandwidthPrice(
            pathId,
            bandwidth,
            duration,
            QoSClass.BestEffort,
            true // Burst
        );
        
        uint256 allocationId = nextAllocationId++;
        
        allocations[allocationId] = BandwidthAllocation({
            pathId: pathId,
            user: msg.sender,
            bandwidth: bandwidth,
            startTime: block.timestamp,
            endTime: block.timestamp + duration,
            qosClass: QoSClass.BestEffort,
            maxLatency: MAX_LATENCY,
            price: price,
            isBurst: true,
            tokenId: 0, // Burst doesn't require token
            slaViolations: 0
        });
        
        // Temporary bandwidth allocation
        if (bandwidth > path.availableBandwidth) {
            path.availableBandwidth = 0;
        } else {
            path.availableBandwidth = path.availableBandwidth.sub(bandwidth);
        }
        
        emit BandwidthAllocated(allocationId, pathId, msg.sender, bandwidth, duration);
        
        return allocationId;
    }
    
    /**
     * @notice Create dedicated circuit reservation
     * @param primaryPathId Primary path ID
     * @param backupPathIds Backup path IDs for redundancy
     * @param bandwidth Reserved bandwidth
     * @param duration Reservation duration
     * @param maxLatency Maximum latency requirement
     * @param requiredUptime Required uptime (basis points)
     */
    function createDedicatedCircuit(
        uint256 primaryPathId,
        uint256[] memory backupPathIds,
        uint256 bandwidth,
        uint256 duration,
        uint256 maxLatency,
        uint256 requiredUptime,
        uint256 tokenId
    ) external nonReentrant returns (uint256) {
        require(duration >= 7 days, "Minimum 7 days for dedicated circuit");
        require(requiredUptime >= 9900, "Minimum 99% uptime required");
        require(backupPathIds.length > 0, "At least one backup path required");
        
        // Verify token ownership
        require(resourceToken.balanceOf(msg.sender, tokenId) > 0, "User doesn't own token");
        
        // Check primary path
        NetworkPath storage primaryPath = networkPaths[primaryPathId];
        require(primaryPath.isActive, "Primary path not active");
        require(primaryPath.availableBandwidth >= bandwidth, "Insufficient bandwidth on primary");
        require(primaryPath.baseLatency <= maxLatency, "Primary path latency too high");
        
        // Check backup paths
        for (uint256 i = 0; i < backupPathIds.length; i++) {
            NetworkPath storage backupPath = networkPaths[backupPathIds[i]];
            require(backupPath.isActive, "Backup path not active");
            require(backupPath.availableBandwidth >= bandwidth, "Insufficient bandwidth on backup");
            require(
                keccak256(bytes(backupPath.sourcePOP)) == keccak256(bytes(primaryPath.sourcePOP)) &&
                keccak256(bytes(backupPath.destinationPOP)) == keccak256(bytes(primaryPath.destinationPOP)),
                "Backup path endpoints don't match"
            );
        }
        
        // Calculate price for dedicated circuit
        uint256 price = calculateDedicatedCircuitPrice(
            bandwidth,
            duration,
            backupPathIds.length,
            requiredUptime
        );
        
        uint256 circuitId = nextCircuitId++;
        
        dedicatedCircuits[circuitId] = DedicatedCircuit({
            pathId: primaryPathId,
            backupPaths: backupPathIds,
            user: msg.sender,
            bandwidth: bandwidth,
            startTime: block.timestamp,
            endTime: block.timestamp + duration,
            maxLatency: maxLatency,
            uptime: requiredUptime,
            price: price,
            isActive: true
        });
        
        // Reserve bandwidth on all paths
        primaryPath.availableBandwidth = primaryPath.availableBandwidth.sub(bandwidth);
        for (uint256 i = 0; i < backupPathIds.length; i++) {
            networkPaths[backupPathIds[i]].availableBandwidth = 
                networkPaths[backupPathIds[i]].availableBandwidth.sub(bandwidth);
        }
        
        emit DedicatedCircuitCreated(circuitId, primaryPathId, msg.sender, bandwidth, duration);
        
        return circuitId;
    }
    
    /**
     * @notice Trade latency futures
     * @param sourcePOP Source POP
     * @param destinationPOP Destination POP
     * @param maxLatency Maximum latency guarantee
     * @param deliveryDate When guarantee starts
     * @param duration Duration of guarantee
     */
    function createLatencyFuture(
        string memory sourcePOP,
        string memory destinationPOP,
        uint256 maxLatency,
        uint256 deliveryDate,
        uint256 duration
    ) external payable returns (uint256) {
        require(deliveryDate > block.timestamp, "Invalid delivery date");
        require(duration >= 1 days, "Minimum 1 day duration");
        require(maxLatency > 0 && maxLatency <= MAX_LATENCY, "Invalid latency");
        
        // Find eligible paths
        uint256[] memory eligiblePaths = popPairPaths[sourcePOP][destinationPOP];
        require(eligiblePaths.length > 0, "No paths available");
        
        // Filter paths that can meet latency requirement
        uint256[] memory qualifiedPaths = new uint256[](eligiblePaths.length);
        uint256 qualifiedCount = 0;
        
        for (uint256 i = 0; i < eligiblePaths.length; i++) {
            NetworkPath memory path = networkPaths[eligiblePaths[i]];
            if (path.isActive && path.baseLatency <= maxLatency) {
                qualifiedPaths[qualifiedCount] = eligiblePaths[i];
                qualifiedCount++;
            }
        }
        
        require(qualifiedCount > 0, "No paths can meet latency requirement");
        
        // Calculate premium based on difficulty
        uint256 premium = calculateLatencyFuturePremium(
            maxLatency,
            duration,
            qualifiedCount
        );
        
        require(msg.value >= premium, "Insufficient payment");
        
        uint256 futureId = nextFutureId++;
        
        // Store only qualified paths
        uint256[] memory finalPaths = new uint256[](qualifiedCount);
        for (uint256 i = 0; i < qualifiedCount; i++) {
            finalPaths[i] = qualifiedPaths[i];
        }
        
        latencyFutures[futureId] = LatencyFuture({
            pathIds: finalPaths,
            buyer: msg.sender,
            maxLatency: maxLatency,
            deliveryDate: deliveryDate,
            duration: duration,
            premium: premium,
            isExercised: false,
            actualLatency: 0
        });
        
        emit LatencyFutureCreated(futureId, msg.sender, maxLatency, deliveryDate);
        
        return futureId;
    }
    
    /**
     * @notice Update congestion factor for a path
     * @param pathId Path ID
     */
    function updateCongestion(uint256 pathId) public {
        NetworkPath storage path = networkPaths[pathId];
        
        if (block.timestamp < path.lastCongestionUpdate + CONGESTION_UPDATE_INTERVAL) {
            return; // Too soon to update
        }
        
        uint256 utilization = path.totalBandwidth.sub(path.availableBandwidth)
            .mul(10000).div(path.totalBandwidth);
        
        uint256 oldFactor = path.congestionFactor;
        
        // Calculate new congestion factor
        if (utilization > 9000) { // > 90%
            path.congestionFactor = 5000; // 50% price increase
        } else if (utilization > 7000) { // > 70%
            path.congestionFactor = 2000; // 20% price increase
        } else if (utilization > 5000) { // > 50%
            path.congestionFactor = 1000; // 10% price increase
        } else {
            path.congestionFactor = 0; // No congestion
        }
        
        // Update latency based on congestion
        path.currentLatency = path.baseLatency.mul(10000 + path.congestionFactor).div(10000);
        
        path.lastCongestionUpdate = block.timestamp;
        
        // Update congestion pricing
        congestionPricing[pathId] = 10000 + path.congestionFactor;
        
        emit CongestionUpdated(pathId, oldFactor, path.congestionFactor);
    }
    
    /**
     * @notice Monitor QoS and check for SLA violations
     * @param allocationId Allocation to check
     * @param actualLatency Measured latency
     * @param packetLoss Measured packet loss
     */
    function checkQoS(
        uint256 allocationId,
        uint256 actualLatency,
        uint256 packetLoss
    ) external onlyRole(QOS_MONITOR_ROLE) {
        BandwidthAllocation storage allocation = allocations[allocationId];
        require(block.timestamp >= allocation.startTime, "Allocation not started");
        require(block.timestamp <= allocation.endTime, "Allocation ended");
        
        bool violation = false;
        
        // Check latency SLA
        if (actualLatency > allocation.maxLatency) {
            violation = true;
            allocation.slaViolations = allocation.slaViolations.add(1);
            
            emit SLAViolation(allocationId, actualLatency, allocation.maxLatency);
        }
        
        // Check packet loss based on QoS class
        uint256 maxLoss = getMaxPacketLoss(allocation.qosClass);
        if (packetLoss > maxLoss) {
            violation = true;
            allocation.slaViolations = allocation.slaViolations.add(1);
        }
        
        // Apply penalty if violations exceed threshold
        if (allocation.slaViolations > 3) {
            // Refund portion of payment
            uint256 refund = allocation.price.mul(SLA_VIOLATION_PENALTY).div(10000);
            
            (bool sent, ) = allocation.user.call{value: refund}("");
            require(sent, "Refund failed");
        }
    }
    
    /**
     * @notice Release bandwidth allocation
     * @param allocationId Allocation ID
     */
    function releaseBandwidth(uint256 allocationId) external {
        BandwidthAllocation storage allocation = allocations[allocationId];
        require(
            msg.sender == allocation.user || 
            block.timestamp > allocation.endTime,
            "Not authorized to release"
        );
        
        NetworkPath storage path = networkPaths[allocation.pathId];
        path.availableBandwidth = path.availableBandwidth.add(allocation.bandwidth);
        pathUtilization[allocation.pathId] = pathUtilization[allocation.pathId].sub(allocation.bandwidth);
        
        // Update congestion after release
        updateCongestion(allocation.pathId);
        
        // Add revenue
        pathRevenue[allocation.pathId] = pathRevenue[allocation.pathId].add(allocation.price);
    }
    
    /**
     * @notice Calculate bandwidth allocation price
     * @param pathId Path ID
     * @param bandwidth Bandwidth amount
     * @param duration Duration in seconds
     * @param qosClass QoS class
     * @param isBurst Whether burst pricing
     */
    function calculateBandwidthPrice(
        uint256 pathId,
        uint256 bandwidth,
        uint256 duration,
        QoSClass qosClass,
        bool isBurst
    ) public view returns (uint256) {
        // Base price
        uint256 price = basePricePerGbpsHour
            .mul(bandwidth)
            .mul(duration)
            .div(3600);
        
        // Apply QoS multiplier
        price = price.mul(qosMultipliers[qosClass]).div(100);
        
        // Apply burst premium if applicable
        if (isBurst) {
            price = price.mul(burstPremium).div(100);
        }
        
        // Apply congestion pricing
        uint256 congestionMultiplier = congestionPricing[pathId];
        if (congestionMultiplier > 10000) {
            price = price.mul(congestionMultiplier).div(10000);
        }
        
        return price;
    }
    
    /**
     * @notice Calculate dedicated circuit price
     * @param bandwidth Bandwidth amount
     * @param duration Duration in seconds
     * @param backupCount Number of backup paths
     * @param requiredUptime Required uptime
     */
    function calculateDedicatedCircuitPrice(
        uint256 bandwidth,
        uint256 duration,
        uint256 backupCount,
        uint256 requiredUptime
    ) public view returns (uint256) {
        // Base price with dedicated premium
        uint256 price = basePricePerGbpsHour
            .mul(bandwidth)
            .mul(duration)
            .div(3600)
            .mul(dedicatedCircuitPremium)
            .div(100);
        
        // Add cost for backup paths (50% of primary cost each)
        price = price.add(price.mul(backupCount).mul(50).div(100));
        
        // Uptime premium
        if (requiredUptime >= 9999) { // 99.99%
            price = price.mul(150).div(100);
        } else if (requiredUptime >= 9990) { // 99.90%
            price = price.mul(120).div(100);
        }
        
        return price;
    }
    
    /**
     * @notice Calculate latency future premium
     * @param maxLatency Maximum latency guarantee
     * @param duration Duration of guarantee
     * @param pathCount Number of eligible paths
     */
    function calculateLatencyFuturePremium(
        uint256 maxLatency,
        uint256 duration,
        uint256 pathCount
    ) public view returns (uint256) {
        // Base premium
        uint256 premium = basePricePerGbpsHour.mul(duration).div(3600);
        
        // Difficulty multiplier (lower latency = higher premium)
        if (maxLatency < 10) { // < 10ms
            premium = premium.mul(500).div(100); // 5x
        } else if (maxLatency < 50) { // < 50ms
            premium = premium.mul(200).div(100); // 2x
        } else if (maxLatency < 100) { // < 100ms
            premium = premium.mul(150).div(100); // 1.5x
        }
        
        // Path diversity discount
        if (pathCount > 5) {
            premium = premium.mul(80).div(100); // 20% discount
        } else if (pathCount > 2) {
            premium = premium.mul(90).div(100); // 10% discount
        }
        
        return premium;
    }
    
    /**
     * @notice Get maximum packet loss for QoS class
     * @param qosClass QoS class
     */
    function getMaxPacketLoss(QoSClass qosClass) internal pure returns (uint256) {
        if (qosClass == QoSClass.Platinum) {
            return 1; // 0.01%
        } else if (qosClass == QoSClass.Gold) {
            return 10; // 0.1%
        } else if (qosClass == QoSClass.Silver) {
            return 100; // 1%
        } else if (qosClass == QoSClass.Bronze) {
            return 300; // 3%
        } else {
            return 1000; // 10% for best effort
        }
    }
    
    // Admin functions
    
    function setPricing(
        uint256 _basePricePerGbpsHour,
        uint256 _burstPremium,
        uint256 _dedicatedCircuitPremium
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        basePricePerGbpsHour = _basePricePerGbpsHour;
        burstPremium = _burstPremium;
        dedicatedCircuitPremium = _dedicatedCircuitPremium;
    }
    
    function setQoSMultiplier(
        QoSClass qosClass,
        uint256 multiplier
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        qosMultipliers[qosClass] = multiplier;
    }
    
    function pausePath(uint256 pathId) external {
        NetworkPath storage path = networkPaths[pathId];
        require(
            msg.sender == path.provider || 
            hasRole(DEFAULT_ADMIN_ROLE, msg.sender),
            "Not authorized"
        );
        path.isActive = false;
    }
    
    function resumePath(uint256 pathId) external {
        NetworkPath storage path = networkPaths[pathId];
        require(
            msg.sender == path.provider || 
            hasRole(DEFAULT_ADMIN_ROLE, msg.sender),
            "Not authorized"
        );
        path.isActive = true;
    }
} 