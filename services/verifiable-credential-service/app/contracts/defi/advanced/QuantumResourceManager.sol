// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/security/Pausable.sol";
import "@openzeppelin/contracts/utils/math/SafeMath.sol";
import "./ExtendedResourceToken.sol";

/**
 * @title QuantumResourceManager
 * @notice Manages quantum computing resources with coherence windows and entanglement
 * @dev Handles QPU allocation, coherence decay, and quantum state preparation
 */
contract QuantumResourceManager is AccessControl, ReentrancyGuard, Pausable {
    using SafeMath for uint256;
    
    // Roles
    bytes32 public constant OPERATOR_ROLE = keccak256("OPERATOR_ROLE");
    bytes32 public constant ORACLE_ROLE = keccak256("ORACLE_ROLE");
    bytes32 public constant SCHEDULER_ROLE = keccak256("SCHEDULER_ROLE");
    
    // Constants
    uint256 public constant COHERENCE_DECAY_RATE = 100; // Basis points per microsecond
    uint256 public constant MIN_COHERENCE_TIME = 10; // Minimum 10 microseconds
    uint256 public constant MAX_QUBITS = 1000; // Maximum qubits per QPU
    uint256 public constant ENTANGLEMENT_FIDELITY_THRESHOLD = 9500; // 95% minimum
    
    // Contracts
    ExtendedResourceToken public immutable resourceToken;
    
    // Quantum Processing Unit (QPU) struct
    struct QPU {
        string identifier;              // QPU unique identifier
        address provider;               // Provider address
        uint256 totalQubits;           // Total number of qubits
        uint256 availableQubits;       // Currently available qubits
        uint256 maxCoherenceTime;      // Maximum coherence time in microseconds
        uint256 gateFidelity;          // Gate fidelity (basis points)
        uint256 measurementFidelity;   // Measurement fidelity (basis points)
        bytes32 connectivityGraph;     // Hash of connectivity graph
        bool isActive;                 // Whether QPU is active
        uint256 lastCalibration;       // Last calibration timestamp
        uint256 errorRate;             // Current error rate
    }
    
    // Coherence Window struct
    struct CoherenceWindow {
        uint256 qpuId;                 // QPU identifier
        uint256 startTime;             // Window start time
        uint256 endTime;               // Window end time
        uint256 qubitAllocation;       // Number of qubits allocated
        address allocatedTo;           // User/contract allocated to
        uint256 tokenId;               // Resource token ID
        uint256 actualCoherenceTime;   // Measured coherence time
        bool isExecuted;               // Whether computation was executed
        bytes32 resultHash;            // Hash of computation result
    }
    
    // Entanglement Pair struct
    struct EntanglementPair {
        uint256 sourceQPU;             // Source QPU ID
        uint256 targetQPU;             // Target QPU ID
        uint256 pairCount;             // Number of Bell pairs
        uint256 fidelity;              // Entanglement fidelity
        uint256 creationTime;          // When pairs were created
        uint256 expiryTime;            // When pairs expire
        address owner;                 // Owner of the pairs
        bool isConsumed;               // Whether pairs have been used
    }
    
    // Quantum Algorithm Registry
    struct QuantumAlgorithm {
        string name;                   // Algorithm name
        uint256 requiredQubits;        // Minimum qubits needed
        uint256 requiredGates;         // Estimated gate count
        uint256 requiredCoherence;     // Minimum coherence time
        bytes32 circuitHash;           // Hash of quantum circuit
        bool requiresEntanglement;     // Whether needs entanglement
        uint256 successRate;           // Historical success rate
    }
    
    // State variables
    mapping(uint256 => QPU) public qpus;
    mapping(uint256 => CoherenceWindow) public coherenceWindows;
    mapping(uint256 => EntanglementPair) public entanglementPairs;
    mapping(bytes32 => QuantumAlgorithm) public algorithms;
    
    uint256 public nextQPUId;
    uint256 public nextWindowId;
    uint256 public nextPairId;
    
    // QPU performance tracking
    mapping(uint256 => uint256[]) public qpuWindowHistory; // QPU ID => window IDs
    mapping(uint256 => uint256) public qpuUtilization; // QPU ID => utilization rate
    mapping(uint256 => uint256) public qpuRevenue; // QPU ID => total revenue
    
    // Pricing parameters
    uint256 public baseQubitPrice = 1000000000000000; // 0.001 ETH per qubit-microsecond
    uint256 public coherencePremium = 150; // 1.5x for high coherence
    uint256 public entanglementPrice = 5000000000000000; // 0.005 ETH per Bell pair
    
    // Events
    event QPURegistered(
        uint256 indexed qpuId,
        string identifier,
        address indexed provider,
        uint256 qubits
    );
    
    event CoherenceWindowCreated(
        uint256 indexed windowId,
        uint256 indexed qpuId,
        uint256 startTime,
        uint256 duration,
        uint256 qubits
    );
    
    event CoherenceWindowExecuted(
        uint256 indexed windowId,
        bool success,
        uint256 actualCoherence,
        bytes32 resultHash
    );
    
    event EntanglementCreated(
        uint256 indexed pairId,
        uint256 sourceQPU,
        uint256 targetQPU,
        uint256 pairCount,
        uint256 fidelity
    );
    
    event AlgorithmRegistered(
        bytes32 indexed algorithmHash,
        string name,
        uint256 requiredQubits
    );
    
    event QPUCalibrated(
        uint256 indexed qpuId,
        uint256 newFidelity,
        uint256 newErrorRate
    );
    
    /**
     * @dev Constructor
     * @param _resourceToken ExtendedResourceToken contract address
     */
    constructor(address _resourceToken) {
        require(_resourceToken != address(0), "Invalid resource token");
        resourceToken = ExtendedResourceToken(_resourceToken);
        
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(OPERATOR_ROLE, msg.sender);
        _grantRole(ORACLE_ROLE, msg.sender);
        _grantRole(SCHEDULER_ROLE, msg.sender);
    }
    
    /**
     * @notice Register a new QPU
     * @param identifier QPU identifier
     * @param qubits Total number of qubits
     * @param maxCoherence Maximum coherence time
     * @param connectivity Connectivity graph hash
     */
    function registerQPU(
        string memory identifier,
        uint256 qubits,
        uint256 maxCoherence,
        uint256 gateFidelity,
        uint256 measurementFidelity,
        bytes32 connectivity
    ) external onlyRole(OPERATOR_ROLE) returns (uint256) {
        require(qubits > 0 && qubits <= MAX_QUBITS, "Invalid qubit count");
        require(maxCoherence >= MIN_COHERENCE_TIME, "Coherence too low");
        require(gateFidelity >= 9000, "Gate fidelity too low");
        require(measurementFidelity >= 9000, "Measurement fidelity too low");
        
        uint256 qpuId = nextQPUId++;
        
        qpus[qpuId] = QPU({
            identifier: identifier,
            provider: msg.sender,
            totalQubits: qubits,
            availableQubits: qubits,
            maxCoherenceTime: maxCoherence,
            gateFidelity: gateFidelity,
            measurementFidelity: measurementFidelity,
            connectivityGraph: connectivity,
            isActive: true,
            lastCalibration: block.timestamp,
            errorRate: 10000 - gateFidelity // Initial error rate
        });
        
        emit QPURegistered(qpuId, identifier, msg.sender, qubits);
        
        return qpuId;
    }
    
    /**
     * @notice Create a coherence window auction (Dutch auction)
     * @param qpuId QPU to auction
     * @param coherenceTime Coherence time window
     * @param qubitCount Number of qubits
     * @param startPrice Starting price
     * @param reservePrice Minimum price
     */
    function createCoherenceWindowAuction(
        uint256 qpuId,
        uint256 coherenceTime,
        uint256 qubitCount,
        uint256 startPrice,
        uint256 reservePrice,
        uint256 windowStartTime
    ) external onlyRole(SCHEDULER_ROLE) returns (uint256) {
        QPU storage qpu = qpus[qpuId];
        require(qpu.isActive, "QPU not active");
        require(qubitCount <= qpu.availableQubits, "Insufficient qubits");
        require(coherenceTime <= qpu.maxCoherenceTime, "Coherence exceeds maximum");
        require(windowStartTime > block.timestamp, "Invalid start time");
        
        uint256 windowId = nextWindowId++;
        
        // Create window
        coherenceWindows[windowId] = CoherenceWindow({
            qpuId: qpuId,
            startTime: windowStartTime,
            endTime: windowStartTime + coherenceTime,
            qubitAllocation: qubitCount,
            allocatedTo: address(0), // To be filled by auction winner
            tokenId: 0, // To be linked when allocated
            actualCoherenceTime: 0,
            isExecuted: false,
            resultHash: bytes32(0)
        });
        
        // Update QPU availability
        qpu.availableQubits = qpu.availableQubits.sub(qubitCount);
        qpuWindowHistory[qpuId].push(windowId);
        
        emit CoherenceWindowCreated(
            windowId,
            qpuId,
            windowStartTime,
            coherenceTime,
            qubitCount
        );
        
        return windowId;
    }
    
    /**
     * @notice Allocate coherence window to user
     * @param windowId Window ID
     * @param user User address
     * @param tokenId Resource token ID
     */
    function allocateCoherenceWindow(
        uint256 windowId,
        address user,
        uint256 tokenId
    ) external onlyRole(SCHEDULER_ROLE) {
        CoherenceWindow storage window = coherenceWindows[windowId];
        require(window.allocatedTo == address(0), "Already allocated");
        require(block.timestamp < window.startTime, "Window expired");
        
        // Verify token ownership and type
        require(
            resourceToken.balanceOf(user, tokenId) > 0,
            "User doesn't own token"
        );
        
        window.allocatedTo = user;
        window.tokenId = tokenId;
        
        // Update utilization
        qpuUtilization[window.qpuId] = qpuUtilization[window.qpuId].add(
            window.endTime.sub(window.startTime)
        );
    }
    
    /**
     * @notice Execute quantum computation in window
     * @param windowId Window ID
     * @param actualCoherence Actual coherence time achieved
     * @param resultHash Hash of computation result
     */
    function executeQuantumComputation(
        uint256 windowId,
        uint256 actualCoherence,
        bytes32 resultHash,
        bool success
    ) external onlyRole(ORACLE_ROLE) {
        CoherenceWindow storage window = coherenceWindows[windowId];
        require(!window.isExecuted, "Already executed");
        require(block.timestamp >= window.startTime, "Window not started");
        
        window.actualCoherenceTime = actualCoherence;
        window.resultHash = resultHash;
        window.isExecuted = true;
        
        // Release qubits back to QPU
        QPU storage qpu = qpus[window.qpuId];
        qpu.availableQubits = qpu.availableQubits.add(window.qubitAllocation);
        
        // Update QPU stats based on performance
        if (actualCoherence < (window.endTime - window.startTime).mul(90).div(100)) {
            // Coherence was less than 90% of expected
            qpu.errorRate = qpu.errorRate.add(100); // Increase error rate
        }
        
        emit CoherenceWindowExecuted(windowId, success, actualCoherence, resultHash);
    }
    
    /**
     * @notice Create entanglement pairs between QPUs
     * @param sourceQPU Source QPU ID
     * @param targetQPU Target QPU ID
     * @param pairCount Number of Bell pairs
     * @param fidelity Entanglement fidelity
     */
    function createEntanglementPairs(
        uint256 sourceQPU,
        uint256 targetQPU,
        uint256 pairCount,
        uint256 fidelity,
        address owner
    ) external onlyRole(OPERATOR_ROLE) returns (uint256) {
        require(qpus[sourceQPU].isActive, "Source QPU not active");
        require(qpus[targetQPU].isActive, "Target QPU not active");
        require(fidelity >= ENTANGLEMENT_FIDELITY_THRESHOLD, "Fidelity too low");
        require(pairCount > 0, "Invalid pair count");
        
        uint256 pairId = nextPairId++;
        
        // Entanglement pairs decay faster than coherence
        uint256 expiryTime = block.timestamp + 
            Math.min(qpus[sourceQPU].maxCoherenceTime, qpus[targetQPU].maxCoherenceTime) / 2;
        
        entanglementPairs[pairId] = EntanglementPair({
            sourceQPU: sourceQPU,
            targetQPU: targetQPU,
            pairCount: pairCount,
            fidelity: fidelity,
            creationTime: block.timestamp,
            expiryTime: expiryTime,
            owner: owner,
            isConsumed: false
        });
        
        emit EntanglementCreated(pairId, sourceQPU, targetQPU, pairCount, fidelity);
        
        return pairId;
    }
    
    /**
     * @notice Trade entanglement pairs
     * @param pairId Entanglement pair ID
     * @param newOwner New owner address
     * @param price Price in wei
     */
    function tradeEntanglementPairs(
        uint256 pairId,
        address newOwner,
        uint256 price
    ) external nonReentrant {
        EntanglementPair storage pair = entanglementPairs[pairId];
        require(pair.owner == msg.sender, "Not pair owner");
        require(!pair.isConsumed, "Pairs already consumed");
        require(block.timestamp < pair.expiryTime, "Pairs expired");
        
        // Transfer ownership
        pair.owner = newOwner;
        
        // Handle payment (simplified - in production would use escrow)
        if (price > 0) {
            (bool sent, ) = msg.sender.call{value: price}("");
            require(sent, "Payment failed");
        }
    }
    
    /**
     * @notice Register quantum algorithm
     * @param name Algorithm name
     * @param requiredQubits Minimum qubits needed
     * @param requiredGates Gate count estimate
     * @param requiredCoherence Minimum coherence time
     * @param circuitHash Hash of quantum circuit
     */
    function registerAlgorithm(
        string memory name,
        uint256 requiredQubits,
        uint256 requiredGates,
        uint256 requiredCoherence,
        bytes32 circuitHash,
        bool requiresEntanglement
    ) external onlyRole(OPERATOR_ROLE) {
        require(requiredQubits > 0 && requiredQubits <= MAX_QUBITS, "Invalid qubit requirement");
        require(requiredCoherence >= MIN_COHERENCE_TIME, "Coherence too low");
        require(circuitHash != bytes32(0), "Invalid circuit hash");
        
        algorithms[circuitHash] = QuantumAlgorithm({
            name: name,
            requiredQubits: requiredQubits,
            requiredGates: requiredGates,
            requiredCoherence: requiredCoherence,
            circuitHash: circuitHash,
            requiresEntanglement: requiresEntanglement,
            successRate: 0 // To be updated based on executions
        });
        
        emit AlgorithmRegistered(circuitHash, name, requiredQubits);
    }
    
    /**
     * @notice Reserve QPU time for specific algorithm
     * @param algorithmHash Algorithm hash
     * @param qpuId QPU to use
     * @param executionTime Desired execution time
     */
    function reserveAlgorithmExecution(
        bytes32 algorithmHash,
        uint256 qpuId,
        uint256 executionTime
    ) external returns (uint256) {
        QuantumAlgorithm memory algo = algorithms[algorithmHash];
        require(algo.requiredQubits > 0, "Algorithm not registered");
        
        QPU storage qpu = qpus[qpuId];
        require(qpu.isActive, "QPU not active");
        require(qpu.availableQubits >= algo.requiredQubits, "Insufficient qubits");
        require(qpu.maxCoherenceTime >= algo.requiredCoherence, "Insufficient coherence");
        
        // Calculate price based on algorithm complexity
        uint256 price = calculateAlgorithmPrice(
            algo.requiredQubits,
            algo.requiredGates,
            algo.requiredCoherence,
            qpu.gateFidelity
        );
        
        // Create coherence window for algorithm
        uint256 windowId = createCoherenceWindowAuction(
            qpuId,
            algo.requiredCoherence,
            algo.requiredQubits,
            price,
            price.mul(80).div(100), // 80% reserve price
            executionTime
        );
        
        return windowId;
    }
    
    /**
     * @notice Calibrate QPU performance
     * @param qpuId QPU ID
     * @param newGateFidelity New gate fidelity
     * @param newMeasurementFidelity New measurement fidelity
     * @param newErrorRate New error rate
     */
    function calibrateQPU(
        uint256 qpuId,
        uint256 newGateFidelity,
        uint256 newMeasurementFidelity,
        uint256 newErrorRate
    ) external onlyRole(ORACLE_ROLE) {
        QPU storage qpu = qpus[qpuId];
        require(qpu.isActive, "QPU not active");
        
        qpu.gateFidelity = newGateFidelity;
        qpu.measurementFidelity = newMeasurementFidelity;
        qpu.errorRate = newErrorRate;
        qpu.lastCalibration = block.timestamp;
        
        emit QPUCalibrated(qpuId, newGateFidelity, newErrorRate);
    }
    
    /**
     * @notice Calculate quantum-classical arbitrage opportunity
     * @param algorithmHash Algorithm to evaluate
     * @param classicalTime Classical execution time estimate
     * @param classicalCost Classical execution cost
     */
    function calculateQuantumAdvantage(
        bytes32 algorithmHash,
        uint256 classicalTime,
        uint256 classicalCost
    ) external view returns (bool hasAdvantage, uint256 quantumCost, uint256 speedup) {
        QuantumAlgorithm memory algo = algorithms[algorithmHash];
        require(algo.requiredQubits > 0, "Algorithm not registered");
        
        // Find best QPU for algorithm
        uint256 bestQPU = 0;
        uint256 bestPrice = type(uint256).max;
        
        for (uint256 i = 0; i < nextQPUId; i++) {
            QPU memory qpu = qpus[i];
            if (qpu.isActive && 
                qpu.availableQubits >= algo.requiredQubits &&
                qpu.maxCoherenceTime >= algo.requiredCoherence) {
                
                uint256 price = calculateAlgorithmPrice(
                    algo.requiredQubits,
                    algo.requiredGates,
                    algo.requiredCoherence,
                    qpu.gateFidelity
                );
                
                if (price < bestPrice) {
                    bestPrice = price;
                    bestQPU = i;
                }
            }
        }
        
        quantumCost = bestPrice;
        
        // Estimate quantum speedup (simplified - would use algorithm-specific models)
        speedup = classicalTime.mul(1000).div(algo.requiredCoherence); // microseconds to milliseconds
        
        hasAdvantage = quantumCost < classicalCost && speedup > 1;
    }
    
    /**
     * @notice Calculate price for algorithm execution
     * @param qubits Number of qubits
     * @param gates Number of gates
     * @param coherenceTime Required coherence time
     * @param fidelity QPU fidelity
     */
    function calculateAlgorithmPrice(
        uint256 qubits,
        uint256 gates,
        uint256 coherenceTime,
        uint256 fidelity
    ) public view returns (uint256) {
        // Base price per qubit-microsecond
        uint256 price = baseQubitPrice.mul(qubits).mul(coherenceTime);
        
        // Gate complexity factor (more gates = higher price)
        uint256 gateFactor = gates.mul(100).div(qubits); // gates per qubit
        if (gateFactor > 1000) {
            price = price.mul(gateFactor).div(1000);
        }
        
        // Fidelity premium (higher fidelity = higher price)
        if (fidelity > 9900) { // 99%+ fidelity
            price = price.mul(coherencePremium).div(100);
        }
        
        return price;
    }
    
    /**
     * @notice Get QPU availability for time window
     * @param qpuId QPU ID
     * @param startTime Start time
     * @param duration Duration needed
     */
    function getQPUAvailability(
        uint256 qpuId,
        uint256 startTime,
        uint256 duration
    ) external view returns (bool available, uint256 availableQubits) {
        QPU memory qpu = qpus[qpuId];
        if (!qpu.isActive) {
            return (false, 0);
        }
        
        // Check for overlapping windows
        uint256[] memory windows = qpuWindowHistory[qpuId];
        for (uint256 i = 0; i < windows.length; i++) {
            CoherenceWindow memory window = coherenceWindows[windows[i]];
            
            // Check if windows overlap
            if (window.startTime < startTime + duration && 
                window.endTime > startTime &&
                !window.isExecuted) {
                // Reduce available qubits
                if (qpu.availableQubits >= window.qubitAllocation) {
                    qpu.availableQubits -= window.qubitAllocation;
                } else {
                    qpu.availableQubits = 0;
                }
            }
        }
        
        available = qpu.availableQubits > 0 && duration <= qpu.maxCoherenceTime;
        availableQubits = qpu.availableQubits;
    }
    
    // Admin functions
    
    function setPricing(
        uint256 _baseQubitPrice,
        uint256 _coherencePremium,
        uint256 _entanglementPrice
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        baseQubitPrice = _baseQubitPrice;
        coherencePremium = _coherencePremium;
        entanglementPrice = _entanglementPrice;
    }
    
    function pauseQPU(uint256 qpuId) external onlyRole(OPERATOR_ROLE) {
        qpus[qpuId].isActive = false;
    }
    
    function resumeQPU(uint256 qpuId) external onlyRole(OPERATOR_ROLE) {
        qpus[qpuId].isActive = true;
    }
} 