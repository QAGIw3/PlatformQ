// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "../ResourceToken.sol";
import "@openzeppelin/contracts/utils/math/SafeMath.sol";

/**
 * @title ExtendedResourceToken
 * @notice Extends ResourceToken to support advanced compute resources
 * @dev Adds quantum computing, AI accelerators, and network bandwidth resources
 */
contract ExtendedResourceToken is ResourceToken {
    using SafeMath for uint256;
    
    // Extended resource types (continuing from base ResourceToken)
    // Base types: CPU_HOURS(0), GPU_HOURS(1), STORAGE_GB_HOURS(2), BANDWIDTH_TB(3), MEMORY_GB_HOURS(4)
    enum ExtendedResourceType {
        QPU_MINUTES,              // 5: Quantum Processing Unit time
        QUANTUM_MEMORY_MB,        // 6: Quantum memory in megabits
        ENTANGLEMENT_PAIRS,       // 7: Bell pairs for quantum communication
        TPU_HOURS,                // 8: Tensor Processing Unit
        NPU_HOURS,                // 9: Neural Processing Unit
        ASIC_HOURS,               // 10: Custom ASIC time
        BANDWIDTH_DEDICATED_GBPS, // 11: Dedicated bandwidth
        LATENCY_GUARANTEED_MS,    // 12: Latency-guaranteed paths
        ROUTING_PRIORITY          // 13: Network routing priority
    }
    
    // Quantum-specific resource attributes
    struct QuantumResourceSpec {
        uint256 qubitCount;           // Number of qubits
        uint256 coherenceTime;        // Coherence time in microseconds
        uint256 gateFidelity;         // Gate fidelity (basis points, e.g., 9995 = 99.95%)
        uint256 measurementFidelity;  // Measurement fidelity (basis points)
        bytes32 connectivityGraph;    // Hash of qubit connectivity graph
        string quantumProcessor;      // QPU identifier (e.g., "IBMQ_Mumbai", "IonQ_Harmony")
        uint256 errorRate;            // Error rate per operation (basis points)
        bool supportsErrorCorrection; // Whether error correction is available
    }
    
    // AI Accelerator attributes
    struct AIAcceleratorSpec {
        string acceleratorModel;      // Model identifier (e.g., "TPU-v4", "A100", "H100")
        uint256 computeCapacity;      // TFLOPS capacity
        uint256 memoryBandwidth;      // GB/s memory bandwidth
        uint256 interconnectSpeed;    // GB/s interconnect speed
        string[] supportedFrameworks; // Supported ML frameworks
        string[] supportedPrecisions; // fp32, fp16, int8, etc.
        uint256 powerConsumption;     // Watts
        uint256 thermalLimit;         // Celsius
    }
    
    // Network bandwidth attributes
    struct NetworkBandwidthSpec {
        string sourcePOP;             // Source point of presence
        string destinationPOP;        // Destination point of presence
        uint256 guaranteedBandwidth;  // Gbps guaranteed
        uint256 burstBandwidth;       // Gbps burst capacity
        uint256 latencyP50;           // 50th percentile latency in ms
        uint256 latencyP99;           // 99th percentile latency in ms
        uint256 packetLossRate;       // Basis points (e.g., 10 = 0.1%)
        uint256 pathDiversity;        // Number of diverse paths
        string qosClass;              // Quality of service class
    }
    
    // Storage mappings for extended specs
    mapping(uint256 => QuantumResourceSpec) public quantumSpecs;
    mapping(uint256 => AIAcceleratorSpec) public aiAcceleratorSpecs;
    mapping(uint256 => NetworkBandwidthSpec) public networkSpecs;
    
    // Quality metrics tracking
    mapping(uint256 => uint256) public lastQualityUpdate;
    mapping(uint256 => uint256) public qualityScore; // 0-10000 basis points
    
    // Events
    event QuantumResourceMinted(
        uint256 indexed tokenId,
        address indexed provider,
        uint256 qubitCount,
        uint256 coherenceTime,
        string quantumProcessor
    );
    
    event AIAcceleratorMinted(
        uint256 indexed tokenId,
        address indexed provider,
        string acceleratorModel,
        uint256 computeCapacity
    );
    
    event NetworkBandwidthMinted(
        uint256 indexed tokenId,
        address indexed provider,
        string sourcePOP,
        string destinationPOP,
        uint256 guaranteedBandwidth
    );
    
    event QualityScoreUpdated(
        uint256 indexed tokenId,
        uint256 oldScore,
        uint256 newScore
    );
    
    /**
     * @notice Mint quantum computing resources
     * @param provider Resource provider address
     * @param qpuMinutes QPU time in minutes
     * @param spec Quantum resource specifications
     */
    function mintQuantumResource(
        address provider,
        uint256 qpuMinutes,
        QuantumResourceSpec memory spec,
        uint256 validFrom,
        uint256 validUntil,
        string memory region,
        ServiceTier tier,
        bytes32 slaHash
    ) external onlyRole(MINTER_ROLE) nonReentrant whenNotPaused returns (uint256) {
        require(spec.qubitCount > 0, "Invalid qubit count");
        require(spec.coherenceTime > 0, "Invalid coherence time");
        require(spec.gateFidelity >= 9000, "Gate fidelity too low"); // Min 90%
        
        // Mint base resource token
        uint256 tokenId = mintResource(
            provider,
            ResourceType(uint256(ExtendedResourceType.QPU_MINUTES)),
            qpuMinutes,
            validFrom,
            validUntil,
            region,
            tier,
            slaHash
        );
        
        // Store quantum-specific attributes
        quantumSpecs[tokenId] = spec;
        
        // Set initial quality score based on fidelity
        qualityScore[tokenId] = (spec.gateFidelity + spec.measurementFidelity) / 2;
        lastQualityUpdate[tokenId] = block.timestamp;
        
        emit QuantumResourceMinted(
            tokenId,
            provider,
            spec.qubitCount,
            spec.coherenceTime,
            spec.quantumProcessor
        );
        
        return tokenId;
    }
    
    /**
     * @notice Mint AI accelerator resources
     * @param provider Resource provider address
     * @param hours Accelerator hours
     * @param acceleratorType Type of accelerator (TPU, NPU, ASIC)
     * @param spec AI accelerator specifications
     */
    function mintAIAcceleratorResource(
        address provider,
        ExtendedResourceType acceleratorType,
        uint256 hours,
        AIAcceleratorSpec memory spec,
        uint256 validFrom,
        uint256 validUntil,
        string memory region,
        ServiceTier tier,
        bytes32 slaHash
    ) external onlyRole(MINTER_ROLE) nonReentrant whenNotPaused returns (uint256) {
        require(
            acceleratorType == ExtendedResourceType.TPU_HOURS ||
            acceleratorType == ExtendedResourceType.NPU_HOURS ||
            acceleratorType == ExtendedResourceType.ASIC_HOURS,
            "Invalid accelerator type"
        );
        require(spec.computeCapacity > 0, "Invalid compute capacity");
        require(bytes(spec.acceleratorModel).length > 0, "Invalid model");
        
        // Mint base resource token
        uint256 tokenId = mintResource(
            provider,
            ResourceType(uint256(acceleratorType)),
            hours,
            validFrom,
            validUntil,
            region,
            tier,
            slaHash
        );
        
        // Store AI accelerator-specific attributes
        aiAcceleratorSpecs[tokenId] = spec;
        
        // Set quality score based on compute capacity and efficiency
        uint256 efficiencyScore = spec.computeCapacity.mul(10000).div(spec.powerConsumption);
        qualityScore[tokenId] = efficiencyScore > 10000 ? 10000 : efficiencyScore;
        lastQualityUpdate[tokenId] = block.timestamp;
        
        emit AIAcceleratorMinted(
            tokenId,
            provider,
            spec.acceleratorModel,
            spec.computeCapacity
        );
        
        return tokenId;
    }
    
    /**
     * @notice Mint network bandwidth resources
     * @param provider Resource provider address
     * @param bandwidthType Type of bandwidth resource
     * @param amount Amount (Gbps for dedicated, ms for latency, priority level for routing)
     * @param spec Network bandwidth specifications
     */
    function mintNetworkBandwidthResource(
        address provider,
        ExtendedResourceType bandwidthType,
        uint256 amount,
        NetworkBandwidthSpec memory spec,
        uint256 validFrom,
        uint256 validUntil,
        string memory region,
        ServiceTier tier,
        bytes32 slaHash
    ) external onlyRole(MINTER_ROLE) nonReentrant whenNotPaused returns (uint256) {
        require(
            bandwidthType == ExtendedResourceType.BANDWIDTH_DEDICATED_GBPS ||
            bandwidthType == ExtendedResourceType.LATENCY_GUARANTEED_MS ||
            bandwidthType == ExtendedResourceType.ROUTING_PRIORITY,
            "Invalid bandwidth type"
        );
        require(bytes(spec.sourcePOP).length > 0, "Invalid source POP");
        require(bytes(spec.destinationPOP).length > 0, "Invalid destination POP");
        
        // Mint base resource token
        uint256 tokenId = mintResource(
            provider,
            ResourceType(uint256(bandwidthType)),
            amount,
            validFrom,
            validUntil,
            region,
            tier,
            slaHash
        );
        
        // Store network-specific attributes
        networkSpecs[tokenId] = spec;
        
        // Set quality score based on latency and packet loss
        uint256 latencyScore = 10000 - spec.latencyP99; // Lower latency = higher score
        uint256 lossScore = 10000 - spec.packetLossRate;
        qualityScore[tokenId] = (latencyScore + lossScore) / 2;
        lastQualityUpdate[tokenId] = block.timestamp;
        
        emit NetworkBandwidthMinted(
            tokenId,
            provider,
            spec.sourcePOP,
            spec.destinationPOP,
            spec.guaranteedBandwidth
        );
        
        return tokenId;
    }
    
    /**
     * @notice Update quality score for a resource
     * @param tokenId Token ID
     * @param newScore New quality score (0-10000)
     */
    function updateQualityScore(
        uint256 tokenId,
        uint256 newScore
    ) external onlyRole(ORACLE_ROLE) {
        require(resourceSpecs[tokenId].isActive, "Resource not active");
        require(newScore <= 10000, "Invalid score");
        
        uint256 oldScore = qualityScore[tokenId];
        qualityScore[tokenId] = newScore;
        lastQualityUpdate[tokenId] = block.timestamp;
        
        emit QualityScoreUpdated(tokenId, oldScore, newScore);
        
        // Apply slashing if quality drops significantly
        if (newScore < oldScore.mul(80).div(100)) { // 20% drop
            _applyQualitySlashing(tokenId, oldScore, newScore);
        }
    }
    
    /**
     * @notice Get quantum resource specifications
     * @param tokenId Token ID
     * @return Quantum resource specifications
     */
    function getQuantumSpec(uint256 tokenId) external view returns (QuantumResourceSpec memory) {
        require(
            resourceSpecs[tokenId].resourceType == ResourceType(uint256(ExtendedResourceType.QPU_MINUTES)),
            "Not a quantum resource"
        );
        return quantumSpecs[tokenId];
    }
    
    /**
     * @notice Get AI accelerator specifications
     * @param tokenId Token ID
     * @return AI accelerator specifications
     */
    function getAIAcceleratorSpec(uint256 tokenId) external view returns (AIAcceleratorSpec memory) {
        ResourceType rType = resourceSpecs[tokenId].resourceType;
        require(
            rType == ResourceType(uint256(ExtendedResourceType.TPU_HOURS)) ||
            rType == ResourceType(uint256(ExtendedResourceType.NPU_HOURS)) ||
            rType == ResourceType(uint256(ExtendedResourceType.ASIC_HOURS)),
            "Not an AI accelerator resource"
        );
        return aiAcceleratorSpecs[tokenId];
    }
    
    /**
     * @notice Get network bandwidth specifications
     * @param tokenId Token ID
     * @return Network bandwidth specifications
     */
    function getNetworkSpec(uint256 tokenId) external view returns (NetworkBandwidthSpec memory) {
        ResourceType rType = resourceSpecs[tokenId].resourceType;
        require(
            rType == ResourceType(uint256(ExtendedResourceType.BANDWIDTH_DEDICATED_GBPS)) ||
            rType == ResourceType(uint256(ExtendedResourceType.LATENCY_GUARANTEED_MS)) ||
            rType == ResourceType(uint256(ExtendedResourceType.ROUTING_PRIORITY)),
            "Not a network resource"
        );
        return networkSpecs[tokenId];
    }
    
    /**
     * @notice Calculate time-decayed value for quantum resources
     * @param tokenId Token ID
     * @return decayedValue Time-decayed value considering coherence
     */
    function calculateQuantumDecayedValue(uint256 tokenId) external view returns (uint256) {
        require(
            resourceSpecs[tokenId].resourceType == ResourceType(uint256(ExtendedResourceType.QPU_MINUTES)),
            "Not a quantum resource"
        );
        
        ResourceSpec memory spec = resourceSpecs[tokenId];
        QuantumResourceSpec memory qSpec = quantumSpecs[tokenId];
        
        // Calculate time until expiry
        uint256 timeRemaining = spec.validUntil > block.timestamp ? 
            spec.validUntil - block.timestamp : 0;
            
        // Apply coherence-based decay
        uint256 coherenceDecayFactor = timeRemaining.mul(10000).div(qSpec.coherenceTime);
        if (coherenceDecayFactor > 10000) coherenceDecayFactor = 10000;
        
        // Apply quality-based adjustment
        uint256 qualityFactor = qualityScore[tokenId];
        
        // Calculate final value
        uint256 baseValue = spec.amount.mul(spec.tier == ServiceTier.GUARANTEED ? 200 : 
                                           spec.tier == ServiceTier.PREMIUM ? 150 : 100);
        
        return baseValue.mul(coherenceDecayFactor).mul(qualityFactor).div(100000000);
    }
    
    /**
     * @notice Check if resources can be bundled together
     * @param tokenIds Array of token IDs to bundle
     * @return canBundle Whether the resources can be bundled
     */
    function canBundleResources(uint256[] calldata tokenIds) external view returns (bool) {
        if (tokenIds.length < 2) return false;
        
        // Check all resources have same provider and validity period
        ResourceSpec memory firstSpec = resourceSpecs[tokenIds[0]];
        
        for (uint256 i = 1; i < tokenIds.length; i++) {
            ResourceSpec memory spec = resourceSpecs[tokenIds[i]];
            if (spec.provider != firstSpec.provider) return false;
            if (spec.validFrom != firstSpec.validFrom) return false;
            if (spec.validUntil != firstSpec.validUntil) return false;
            if (!spec.isActive) return false;
        }
        
        return true;
    }
    
    // Internal functions
    
    function _applyQualitySlashing(
        uint256 tokenId,
        uint256 oldScore,
        uint256 newScore
    ) internal {
        ResourceSpec storage spec = resourceSpecs[tokenId];
        
        // Calculate slashing amount based on quality drop
        uint256 dropPercentage = (oldScore - newScore).mul(100).div(oldScore);
        uint256 slashAmount = spec.amount.mul(dropPercentage).div(100);
        
        // Apply slashing
        spec.slashedAmount = spec.slashedAmount.add(slashAmount);
        
        // Emit slashing event
        emit ResourceSlashed(tokenId, spec.provider, slashAmount, "Quality degradation");
        
        // Update provider reputation
        providers[spec.provider].totalSlashed = 
            providers[spec.provider].totalSlashed.add(slashAmount);
        providers[spec.provider].slashingEvents = 
            providers[spec.provider].slashingEvents.add(1);
            
        // Reduce reputation score
        if (providers[spec.provider].reputationScore > dropPercentage) {
            providers[spec.provider].reputationScore = 
                providers[spec.provider].reputationScore.sub(dropPercentage);
        } else {
            providers[spec.provider].reputationScore = 0;
        }
    }
} 