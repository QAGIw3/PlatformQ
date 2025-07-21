// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/security/Pausable.sol";
import "@openzeppelin/contracts/utils/math/SafeMath.sol";
import "./ExtendedResourceToken.sol";

/**
 * @title AIAcceleratorRegistry
 * @notice Registry and management system for AI accelerator resources
 * @dev Handles TPU, NPU, and custom ASIC allocation with performance benchmarking
 */
contract AIAcceleratorRegistry is AccessControl, ReentrancyGuard, Pausable {
    using SafeMath for uint256;
    
    // Roles
    bytes32 public constant PROVIDER_ROLE = keccak256("PROVIDER_ROLE");
    bytes32 public constant BENCHMARKER_ROLE = keccak256("BENCHMARKER_ROLE");
    bytes32 public constant SCHEDULER_ROLE = keccak256("SCHEDULER_ROLE");
    
    // Constants
    uint256 public constant MIN_COMPUTE_CAPACITY = 10; // Minimum 10 TFLOPS
    uint256 public constant MAX_POWER_CONSUMPTION = 1000; // Maximum 1000W
    uint256 public constant BENCHMARK_VALIDITY_PERIOD = 7 days;
    uint256 public constant MIN_AVAILABILITY_WINDOW = 1 hours;
    
    // Contracts
    ExtendedResourceToken public immutable resourceToken;
    
    // Accelerator types
    enum AcceleratorType {
        TPU,
        NPU,
        ASIC
    }
    
    // AI Accelerator struct
    struct AIAccelerator {
        string model;                   // Model identifier (e.g., "TPU-v4", "A100")
        AcceleratorType acceleratorType;
        address provider;               // Provider address
        uint256 computeCapacity;       // TFLOPS
        uint256 memoryBandwidth;       // GB/s
        uint256 interconnectSpeed;     // GB/s
        uint256 powerConsumption;      // Watts
        uint256 thermalLimit;          // Celsius
        bool isActive;                 // Whether accelerator is active
        uint256 currentTemperature;    // Current temperature
        uint256 utilizationRate;       // Current utilization (basis points)
        mapping(string => bool) supportedFrameworks; // Framework support
        mapping(string => bool) supportedPrecisions; // Precision support
    }
    
    // Performance Benchmark struct
    struct PerformanceBenchmark {
        uint256 acceleratorId;         // Accelerator ID
        string benchmarkType;          // MLPerf, custom, etc.
        uint256 score;                 // Benchmark score
        uint256 timestamp;             // When benchmark was run
        bytes32 resultHash;            // Hash of detailed results
        bool isValid;                  // Whether benchmark is still valid
    }
    
    // Training Contract struct
    struct TrainingContract {
        uint256 acceleratorId;         // Allocated accelerator
        address user;                  // User address
        string modelArchitecture;      // Model being trained
        uint256 datasetSize;           // Dataset size in GB
        uint256 targetAccuracy;        // Target accuracy (basis points)
        uint256 startTime;             // Contract start time
        uint256 endTime;               // Contract end time
        uint256 actualAccuracy;        // Achieved accuracy
        uint256 price;                 // Total price
        bool isCompleted;              // Whether training completed
        uint256 tokenId;               // Resource token ID
    }
    
    // Inference Request struct
    struct InferenceRequest {
        uint256 acceleratorId;         // Allocated accelerator
        address user;                  // User address
        string modelId;                // Model to run
        uint256 batchSize;             // Batch size
        uint256 latencyRequirement;    // Max latency in ms
        uint256 requestTime;           // When request was made
        uint256 completionTime;        // When inference completed
        uint256 actualLatency;         // Actual latency achieved
        uint256 throughput;            // Inferences per second
    }
    
    // State variables
    mapping(uint256 => AIAccelerator) public accelerators;
    mapping(uint256 => PerformanceBenchmark[]) public benchmarks;
    mapping(uint256 => TrainingContract) public trainingContracts;
    mapping(uint256 => InferenceRequest) public inferenceRequests;
    
    uint256 public nextAcceleratorId;
    uint256 public nextContractId;
    uint256 public nextRequestId;
    
    // Accelerator availability tracking
    mapping(uint256 => uint256[]) public acceleratorSchedule; // Accelerator ID => contract IDs
    mapping(uint256 => uint256) public acceleratorRevenue; // Accelerator ID => total revenue
    
    // Model compatibility registry
    mapping(string => mapping(AcceleratorType => bool)) public modelCompatibility;
    mapping(string => uint256) public modelComputeRequirements; // TFLOPS required
    
    // Pricing parameters
    mapping(AcceleratorType => uint256) public baseHourlyRate; // Base price per hour
    uint256 public trainingMultiplier = 100; // 1x for training
    uint256 public inferenceMultiplier = 50; // 0.5x for inference
    uint256 public exclusiveAccessMultiplier = 200; // 2x for exclusive access
    
    // Events
    event AcceleratorRegistered(
        uint256 indexed acceleratorId,
        string model,
        AcceleratorType acceleratorType,
        address indexed provider,
        uint256 computeCapacity
    );
    
    event BenchmarkRecorded(
        uint256 indexed acceleratorId,
        string benchmarkType,
        uint256 score,
        bytes32 resultHash
    );
    
    event TrainingContractCreated(
        uint256 indexed contractId,
        uint256 indexed acceleratorId,
        address indexed user,
        string modelArchitecture,
        uint256 duration
    );
    
    event TrainingCompleted(
        uint256 indexed contractId,
        uint256 actualAccuracy,
        bool success
    );
    
    event InferenceRequestCreated(
        uint256 indexed requestId,
        uint256 indexed acceleratorId,
        address indexed user,
        string modelId
    );
    
    event InferenceCompleted(
        uint256 indexed requestId,
        uint256 actualLatency,
        uint256 throughput
    );
    
    event ModelCompatibilityUpdated(
        string modelArchitecture,
        AcceleratorType acceleratorType,
        bool isCompatible
    );
    
    /**
     * @dev Constructor
     * @param _resourceToken ExtendedResourceToken contract address
     */
    constructor(address _resourceToken) {
        require(_resourceToken != address(0), "Invalid resource token");
        resourceToken = ExtendedResourceToken(_resourceToken);
        
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(PROVIDER_ROLE, msg.sender);
        _grantRole(BENCHMARKER_ROLE, msg.sender);
        _grantRole(SCHEDULER_ROLE, msg.sender);
        
        // Initialize base hourly rates
        baseHourlyRate[AcceleratorType.TPU] = 5000000000000000000; // 5 ETH per hour
        baseHourlyRate[AcceleratorType.NPU] = 3000000000000000000; // 3 ETH per hour
        baseHourlyRate[AcceleratorType.ASIC] = 8000000000000000000; // 8 ETH per hour
    }
    
    /**
     * @notice Register a new AI accelerator
     * @param model Accelerator model
     * @param acceleratorType Type of accelerator
     * @param computeCapacity Compute capacity in TFLOPS
     * @param specs Additional specifications
     */
    function registerAccelerator(
        string memory model,
        AcceleratorType acceleratorType,
        uint256 computeCapacity,
        uint256 memoryBandwidth,
        uint256 interconnectSpeed,
        uint256 powerConsumption,
        uint256 thermalLimit
    ) external onlyRole(PROVIDER_ROLE) returns (uint256) {
        require(computeCapacity >= MIN_COMPUTE_CAPACITY, "Insufficient compute capacity");
        require(powerConsumption <= MAX_POWER_CONSUMPTION, "Power consumption too high");
        require(bytes(model).length > 0, "Invalid model name");
        
        uint256 acceleratorId = nextAcceleratorId++;
        
        AIAccelerator storage acc = accelerators[acceleratorId];
        acc.model = model;
        acc.acceleratorType = acceleratorType;
        acc.provider = msg.sender;
        acc.computeCapacity = computeCapacity;
        acc.memoryBandwidth = memoryBandwidth;
        acc.interconnectSpeed = interconnectSpeed;
        acc.powerConsumption = powerConsumption;
        acc.thermalLimit = thermalLimit;
        acc.isActive = true;
        acc.currentTemperature = 25; // Room temperature
        acc.utilizationRate = 0;
        
        emit AcceleratorRegistered(
            acceleratorId,
            model,
            acceleratorType,
            msg.sender,
            computeCapacity
        );
        
        return acceleratorId;
    }
    
    /**
     * @notice Add framework support to accelerator
     * @param acceleratorId Accelerator ID
     * @param frameworks List of supported frameworks
     * @param precisions List of supported precisions
     */
    function addFrameworkSupport(
        uint256 acceleratorId,
        string[] memory frameworks,
        string[] memory precisions
    ) external {
        AIAccelerator storage acc = accelerators[acceleratorId];
        require(acc.provider == msg.sender, "Not accelerator provider");
        require(acc.isActive, "Accelerator not active");
        
        for (uint256 i = 0; i < frameworks.length; i++) {
            acc.supportedFrameworks[frameworks[i]] = true;
        }
        
        for (uint256 i = 0; i < precisions.length; i++) {
            acc.supportedPrecisions[precisions[i]] = true;
        }
    }
    
    /**
     * @notice Record performance benchmark
     * @param acceleratorId Accelerator ID
     * @param benchmarkType Type of benchmark
     * @param score Benchmark score
     * @param resultHash Hash of detailed results
     */
    function recordBenchmark(
        uint256 acceleratorId,
        string memory benchmarkType,
        uint256 score,
        bytes32 resultHash
    ) external onlyRole(BENCHMARKER_ROLE) {
        require(accelerators[acceleratorId].isActive, "Accelerator not active");
        require(score > 0, "Invalid benchmark score");
        
        // Invalidate old benchmarks
        PerformanceBenchmark[] storage accBenchmarks = benchmarks[acceleratorId];
        for (uint256 i = 0; i < accBenchmarks.length; i++) {
            if (keccak256(bytes(accBenchmarks[i].benchmarkType)) == keccak256(bytes(benchmarkType))) {
                accBenchmarks[i].isValid = false;
            }
        }
        
        // Add new benchmark
        accBenchmarks.push(PerformanceBenchmark({
            acceleratorId: acceleratorId,
            benchmarkType: benchmarkType,
            score: score,
            timestamp: block.timestamp,
            resultHash: resultHash,
            isValid: true
        }));
        
        emit BenchmarkRecorded(acceleratorId, benchmarkType, score, resultHash);
    }
    
    /**
     * @notice Create training contract
     * @param acceleratorId Accelerator to use
     * @param modelArchitecture Model architecture
     * @param datasetSize Dataset size in GB
     * @param targetAccuracy Target accuracy
     * @param duration Training duration
     */
    function createTrainingContract(
        uint256 acceleratorId,
        string memory modelArchitecture,
        uint256 datasetSize,
        uint256 targetAccuracy,
        uint256 duration,
        uint256 tokenId
    ) external nonReentrant returns (uint256) {
        AIAccelerator storage acc = accelerators[acceleratorId];
        require(acc.isActive, "Accelerator not active");
        require(duration >= MIN_AVAILABILITY_WINDOW, "Duration too short");
        require(modelCompatibility[modelArchitecture][acc.acceleratorType], "Model not compatible");
        
        // Check accelerator availability
        require(checkAcceleratorAvailability(acceleratorId, block.timestamp, duration), "Accelerator not available");
        
        // Verify token ownership
        require(resourceToken.balanceOf(msg.sender, tokenId) > 0, "User doesn't own token");
        
        // Calculate price
        uint256 price = calculateTrainingPrice(
            acceleratorId,
            modelArchitecture,
            datasetSize,
            duration
        );
        
        uint256 contractId = nextContractId++;
        
        trainingContracts[contractId] = TrainingContract({
            acceleratorId: acceleratorId,
            user: msg.sender,
            modelArchitecture: modelArchitecture,
            datasetSize: datasetSize,
            targetAccuracy: targetAccuracy,
            startTime: block.timestamp,
            endTime: block.timestamp + duration,
            actualAccuracy: 0,
            price: price,
            isCompleted: false,
            tokenId: tokenId
        });
        
        // Update accelerator schedule
        acceleratorSchedule[acceleratorId].push(contractId);
        acc.utilizationRate = acc.utilizationRate.add(10000); // 100% utilization during training
        
        emit TrainingContractCreated(
            contractId,
            acceleratorId,
            msg.sender,
            modelArchitecture,
            duration
        );
        
        return contractId;
    }
    
    /**
     * @notice Complete training contract
     * @param contractId Contract ID
     * @param actualAccuracy Achieved accuracy
     * @param success Whether training succeeded
     */
    function completeTraining(
        uint256 contractId,
        uint256 actualAccuracy,
        bool success
    ) external onlyRole(SCHEDULER_ROLE) {
        TrainingContract storage contract_ = trainingContracts[contractId];
        require(!contract_.isCompleted, "Already completed");
        require(block.timestamp >= contract_.startTime, "Training not started");
        
        contract_.actualAccuracy = actualAccuracy;
        contract_.isCompleted = true;
        
        // Update accelerator utilization
        AIAccelerator storage acc = accelerators[contract_.acceleratorId];
        acc.utilizationRate = acc.utilizationRate.sub(10000);
        
        // Update revenue if successful
        if (success) {
            acceleratorRevenue[contract_.acceleratorId] = 
                acceleratorRevenue[contract_.acceleratorId].add(contract_.price);
        }
        
        emit TrainingCompleted(contractId, actualAccuracy, success);
    }
    
    /**
     * @notice Create spot inference request
     * @param acceleratorId Accelerator to use
     * @param modelId Model ID
     * @param batchSize Batch size
     * @param latencyRequirement Max latency in ms
     */
    function requestInference(
        uint256 acceleratorId,
        string memory modelId,
        uint256 batchSize,
        uint256 latencyRequirement
    ) external returns (uint256) {
        AIAccelerator storage acc = accelerators[acceleratorId];
        require(acc.isActive, "Accelerator not active");
        require(acc.utilizationRate < 9000, "Accelerator too busy"); // < 90% utilization
        
        uint256 requestId = nextRequestId++;
        
        inferenceRequests[requestId] = InferenceRequest({
            acceleratorId: acceleratorId,
            user: msg.sender,
            modelId: modelId,
            batchSize: batchSize,
            latencyRequirement: latencyRequirement,
            requestTime: block.timestamp,
            completionTime: 0,
            actualLatency: 0,
            throughput: 0
        });
        
        // Temporarily increase utilization
        acc.utilizationRate = acc.utilizationRate.add(500); // 5% for inference
        
        emit InferenceRequestCreated(requestId, acceleratorId, msg.sender, modelId);
        
        return requestId;
    }
    
    /**
     * @notice Complete inference request
     * @param requestId Request ID
     * @param actualLatency Actual latency achieved
     * @param throughput Inferences per second
     */
    function completeInference(
        uint256 requestId,
        uint256 actualLatency,
        uint256 throughput
    ) external onlyRole(SCHEDULER_ROLE) {
        InferenceRequest storage request = inferenceRequests[requestId];
        require(request.completionTime == 0, "Already completed");
        
        request.completionTime = block.timestamp;
        request.actualLatency = actualLatency;
        request.throughput = throughput;
        
        // Update accelerator utilization
        AIAccelerator storage acc = accelerators[request.acceleratorId];
        acc.utilizationRate = acc.utilizationRate.sub(500);
        
        // Calculate and collect payment (simplified)
        uint256 price = calculateInferencePrice(
            request.acceleratorId,
            request.batchSize,
            block.timestamp - request.requestTime
        );
        
        acceleratorRevenue[request.acceleratorId] = 
            acceleratorRevenue[request.acceleratorId].add(price);
        
        emit InferenceCompleted(requestId, actualLatency, throughput);
    }
    
    /**
     * @notice Update model compatibility
     * @param modelArchitecture Model architecture
     * @param acceleratorType Accelerator type
     * @param isCompatible Whether compatible
     * @param computeRequirement TFLOPS required
     */
    function updateModelCompatibility(
        string memory modelArchitecture,
        AcceleratorType acceleratorType,
        bool isCompatible,
        uint256 computeRequirement
    ) external onlyRole(BENCHMARKER_ROLE) {
        modelCompatibility[modelArchitecture][acceleratorType] = isCompatible;
        
        if (computeRequirement > 0) {
            modelComputeRequirements[modelArchitecture] = computeRequirement;
        }
        
        emit ModelCompatibilityUpdated(modelArchitecture, acceleratorType, isCompatible);
    }
    
    /**
     * @notice Check accelerator availability
     * @param acceleratorId Accelerator ID
     * @param startTime Start time
     * @param duration Duration needed
     */
    function checkAcceleratorAvailability(
        uint256 acceleratorId,
        uint256 startTime,
        uint256 duration
    ) public view returns (bool) {
        uint256[] memory schedule = acceleratorSchedule[acceleratorId];
        
        for (uint256 i = 0; i < schedule.length; i++) {
            TrainingContract memory contract_ = trainingContracts[schedule[i]];
            
            // Check for overlap
            if (contract_.startTime < startTime + duration && 
                contract_.endTime > startTime &&
                !contract_.isCompleted) {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * @notice Calculate training price
     * @param acceleratorId Accelerator ID
     * @param modelArchitecture Model architecture
     * @param datasetSize Dataset size
     * @param duration Duration
     */
    function calculateTrainingPrice(
        uint256 acceleratorId,
        string memory modelArchitecture,
        uint256 datasetSize,
        uint256 duration
    ) public view returns (uint256) {
        AIAccelerator storage acc = accelerators[acceleratorId];
        
        // Base price
        uint256 basePrice = baseHourlyRate[acc.acceleratorType]
            .mul(duration)
            .div(1 hours);
        
        // Apply training multiplier
        basePrice = basePrice.mul(trainingMultiplier).div(100);
        
        // Dataset size factor (larger datasets cost more)
        uint256 datasetFactor = datasetSize.mul(100).div(1000); // Per TB
        if (datasetFactor > 100) {
            basePrice = basePrice.mul(datasetFactor).div(100);
        }
        
        // Model complexity factor
        uint256 computeReq = modelComputeRequirements[modelArchitecture];
        if (computeReq > acc.computeCapacity.mul(80).div(100)) {
            // High utilization premium
            basePrice = basePrice.mul(150).div(100);
        }
        
        // Performance factor based on benchmarks
        uint256 performanceMultiplier = getPerformanceMultiplier(acceleratorId);
        basePrice = basePrice.mul(performanceMultiplier).div(100);
        
        return basePrice;
    }
    
    /**
     * @notice Calculate inference price
     * @param acceleratorId Accelerator ID
     * @param batchSize Batch size
     * @param duration Duration in seconds
     */
    function calculateInferencePrice(
        uint256 acceleratorId,
        uint256 batchSize,
        uint256 duration
    ) public view returns (uint256) {
        AIAccelerator storage acc = accelerators[acceleratorId];
        
        // Base price per second
        uint256 basePrice = baseHourlyRate[acc.acceleratorType]
            .mul(duration)
            .div(3600);
        
        // Apply inference multiplier (cheaper than training)
        basePrice = basePrice.mul(inferenceMultiplier).div(100);
        
        // Batch size factor
        uint256 batchFactor = batchSize.mul(100).div(1000); // Per 1000 samples
        if (batchFactor < 10) batchFactor = 10; // Minimum charge
        
        basePrice = basePrice.mul(batchFactor).div(100);
        
        return basePrice;
    }
    
    /**
     * @notice Get performance multiplier based on benchmarks
     * @param acceleratorId Accelerator ID
     */
    function getPerformanceMultiplier(uint256 acceleratorId) internal view returns (uint256) {
        PerformanceBenchmark[] memory accBenchmarks = benchmarks[acceleratorId];
        
        if (accBenchmarks.length == 0) {
            return 100; // Default multiplier
        }
        
        // Find most recent valid benchmark
        uint256 latestScore = 0;
        for (uint256 i = 0; i < accBenchmarks.length; i++) {
            if (accBenchmarks[i].isValid && 
                block.timestamp - accBenchmarks[i].timestamp <= BENCHMARK_VALIDITY_PERIOD) {
                if (accBenchmarks[i].score > latestScore) {
                    latestScore = accBenchmarks[i].score;
                }
            }
        }
        
        // Convert score to multiplier (higher score = higher price)
        if (latestScore > 10000) {
            return 150; // Premium performance
        } else if (latestScore > 5000) {
            return 120; // Good performance
        } else {
            return 100; // Standard performance
        }
    }
    
    /**
     * @notice Update accelerator temperature
     * @param acceleratorId Accelerator ID
     * @param temperature Current temperature
     */
    function updateTemperature(
        uint256 acceleratorId,
        uint256 temperature
    ) external onlyRole(SCHEDULER_ROLE) {
        AIAccelerator storage acc = accelerators[acceleratorId];
        acc.currentTemperature = temperature;
        
        // Thermal throttling
        if (temperature > acc.thermalLimit) {
            acc.isActive = false; // Emergency shutdown
        }
    }
    
    // Admin functions
    
    function setPricing(
        AcceleratorType acceleratorType,
        uint256 hourlyRate
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        baseHourlyRate[acceleratorType] = hourlyRate;
    }
    
    function setMultipliers(
        uint256 _trainingMultiplier,
        uint256 _inferenceMultiplier,
        uint256 _exclusiveAccessMultiplier
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        trainingMultiplier = _trainingMultiplier;
        inferenceMultiplier = _inferenceMultiplier;
        exclusiveAccessMultiplier = _exclusiveAccessMultiplier;
    }
    
    function pauseAccelerator(uint256 acceleratorId) external {
        AIAccelerator storage acc = accelerators[acceleratorId];
        require(msg.sender == acc.provider || hasRole(DEFAULT_ADMIN_ROLE, msg.sender), "Not authorized");
        acc.isActive = false;
    }
    
    function resumeAccelerator(uint256 acceleratorId) external {
        AIAccelerator storage acc = accelerators[acceleratorId];
        require(msg.sender == acc.provider || hasRole(DEFAULT_ADMIN_ROLE, msg.sender), "Not authorized");
        require(acc.currentTemperature <= acc.thermalLimit, "Temperature too high");
        acc.isActive = true;
    }
} 