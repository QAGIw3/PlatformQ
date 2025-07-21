# AI Compute Market Service

## Overview

The AI Compute Market Service manages the marketplace for specialized AI accelerators (TPUs, NPUs, and custom ASICs) within PlatformQ. It provides dynamic pricing, performance-based allocation, and seamless integration with the ML Platform Service for training and inference workloads.

## Architecture

### Key Components

1. **Accelerator Registry**: Tracks available AI accelerators and their specifications
2. **Performance Benchmarker**: Validates and tracks accelerator performance
3. **Workload Scheduler**: Matches AI workloads to optimal accelerators
4. **Pricing Engine**: Dynamic pricing based on performance and demand
5. **Training Manager**: Handles long-running training contracts
6. **Inference Router**: Routes inference requests to available accelerators
7. **Thermal Monitor**: Tracks temperature and manages throttling

### Technology Stack

- **FastAPI**: REST API framework
- **Apache Ignite**: Distributed state for accelerator registry
- **Apache Pulsar**: Event streaming for workload events
- **Apache Flink**: Real-time performance analytics
- **Blockchain**: Smart contracts for resource tokenization

## Features

### Accelerator Types
- **TPUs**: Tensor Processing Units for TensorFlow workloads
- **NPUs**: Neural Processing Units for edge AI
- **ASICs**: Custom accelerators for specific models
- **FPGAs**: Field-programmable arrays for flexible acceleration

### Market Mechanisms
- **Spot Instances**: Pay-as-you-go for inference
- **Reserved Instances**: Long-term contracts with discounts
- **Training Contracts**: Guaranteed resources for model training
- **Batch Aggregation**: Combine small jobs for efficiency

### Performance Features
- **Benchmark Registry**: MLPerf and custom benchmarks
- **Performance Guarantees**: SLA-based pricing
- **Model Compatibility**: Framework and precision matching
- **Auto-scaling**: Dynamic resource allocation

## API Endpoints

### Accelerator Management
- `POST /api/v1/accelerators/register` - Register new accelerator
- `GET /api/v1/accelerators` - List available accelerators
- `GET /api/v1/accelerators/{id}` - Get accelerator details
- `PUT /api/v1/accelerators/{id}/benchmark` - Update benchmark results

### Training Contracts
- `POST /api/v1/training/contracts` - Create training contract
- `GET /api/v1/training/contracts/{id}` - Get contract status
- `PUT /api/v1/training/contracts/{id}/checkpoint` - Update training progress
- `POST /api/v1/training/contracts/{id}/complete` - Mark training complete

### Inference Requests
- `POST /api/v1/inference/request` - Submit inference request
- `GET /api/v1/inference/batch` - Get batch inference status
- `POST /api/v1/inference/stream` - Start streaming inference
- `GET /api/v1/inference/metrics` - Get inference metrics

### Pricing and Markets
- `GET /api/v1/pricing/spot` - Get spot prices by accelerator type
- `POST /api/v1/pricing/quote` - Get training/inference quote
- `GET /api/v1/markets/availability` - Check accelerator availability
- `GET /api/v1/markets/trends` - Market trends and forecasts

### Performance Monitoring
- `GET /api/v1/performance/benchmarks` - List benchmark results
- `POST /api/v1/performance/report` - Submit performance report
- `GET /api/v1/performance/leaderboard` - Performance leaderboard
- `GET /api/v1/performance/compatibility` - Model compatibility matrix

## Accelerator Specifications

### TPU Specification
```python
{
    "accelerator_id": "tpu_v4_pod",
    "type": "TPU",
    "model": "v4",
    "provider": "provider_address",
    "specs": {
        "compute_capacity": 275,  # TFLOPS
        "memory_bandwidth": 1200,  # GB/s
        "interconnect_speed": 400,  # GB/s
        "memory_size": 32,  # GB HBM
        "power_consumption": 450,  # Watts
        "thermal_limit": 85  # Celsius
    },
    "supported_frameworks": ["tensorflow", "jax", "pytorch_xla"],
    "supported_precisions": ["bf16", "fp32", "int8"],
    "location": "us-central1",
    "price_per_hour": "5.0"  # Base price
}
```

### Training Contract
```python
{
    "contract_id": "tc_123456",
    "accelerator_id": "tpu_v4_pod",
    "user": "user_address",
    "model_architecture": "transformer_large",
    "dataset_size": 1000,  # GB
    "target_accuracy": 0.95,
    "start_time": "2024-01-15T10:00:00Z",
    "duration": 86400,  # seconds
    "price": "120.0",  # Total price
    "status": "running",
    "checkpoints": [
        {
            "epoch": 10,
            "accuracy": 0.89,
            "loss": 0.23,
            "timestamp": "2024-01-15T12:00:00Z"
        }
    ]
}
```

### Inference Request
```python
{
    "request_id": "ir_789012",
    "accelerator_id": "npu_edge_01",
    "model_id": "mobilenet_v3",
    "batch_size": 32,
    "input_shape": [224, 224, 3],
    "latency_requirement": 10,  # ms
    "precision": "int8",
    "status": "completed",
    "metrics": {
        "actual_latency": 8.5,
        "throughput": 3764,  # inferences/second
        "accuracy": 0.92
    }
}
```

## Pricing Models

### Training Pricing
```python
price = base_hourly_rate * duration * performance_multiplier * utilization_factor
```

Where:
- `base_hourly_rate`: Base price for accelerator type
- `performance_multiplier`: Based on benchmark scores
- `utilization_factor`: Higher for exclusive access

### Inference Pricing
```python
price = base_rate_per_1k * (batch_size / 1000) * latency_premium * precision_discount
```

Where:
- `base_rate_per_1k`: Base price per 1000 inferences
- `latency_premium`: Premium for low-latency requirements
- `precision_discount`: Discount for lower precision

## Workload Scheduling

### Training Priority
1. Reserved instances (highest priority)
2. Long-term contracts
3. Spot training jobs
4. Research/academic (discounted)

### Inference Routing
1. Latency requirements
2. Model compatibility
3. Geographic proximity
4. Cost optimization

## Performance Benchmarking

### Supported Benchmarks
- **MLPerf Training**: Industry-standard training benchmarks
- **MLPerf Inference**: Inference performance benchmarks
- **Custom Benchmarks**: User-defined performance tests
- **Energy Efficiency**: Performance per watt metrics

### Benchmark Categories
- Image Classification (ResNet, EfficientNet)
- Object Detection (YOLO, SSD)
- Language Models (BERT, GPT)
- Recommendation (DLRM)
- Reinforcement Learning (various)

## Model Compatibility

### Framework Support Matrix
| Accelerator | TensorFlow | PyTorch | JAX | ONNX | Custom |
|-------------|------------|---------|-----|------|---------|
| TPU v4      | Native     | XLA     | Native | Limited | No |
| NPU Edge    | TFLite     | Limited | No  | Yes    | Yes |
| ASIC ML     | No         | No      | No  | No     | Yes |

### Precision Support
- **FP32**: Full precision (all accelerators)
- **FP16**: Half precision (most accelerators)
- **BF16**: Brain float (TPUs, some GPUs)
- **INT8**: Quantized (edge devices)
- **INT4**: Ultra-low precision (specialized)

## Integration

### With ML Platform Service
- Automatic accelerator selection
- Training job orchestration
- Model deployment pipeline
- Performance tracking

### With DeFi Protocols
- Collateralized compute loans
- Training completion futures
- Performance-based staking
- Inference capacity options

## Monitoring

### Metrics
- `ai_accelerator_utilization`
- `ai_training_jobs_active`
- `ai_inference_requests_per_second`
- `ai_model_accuracy_achieved`
- `ai_thermal_throttling_events`
- `ai_benchmark_scores`

### Alerts
- Thermal limit approaching
- Training stall detection
- Inference SLA violations
- Accelerator failure

## Configuration

```python
# AI Compute Market Configuration
MIN_COMPUTE_CAPACITY = 10  # TFLOPS
MAX_POWER_CONSUMPTION = 1000  # Watts
BENCHMARK_VALIDITY_PERIOD = 604800  # 7 days

# Pricing
BASE_TPU_HOURLY = 5.0  # ETH
BASE_NPU_HOURLY = 3.0  # ETH
BASE_ASIC_HOURLY = 8.0  # ETH

# Training
MIN_TRAINING_DURATION = 3600  # 1 hour
MAX_TRAINING_DURATION = 2592000  # 30 days
CHECKPOINT_INTERVAL = 3600  # 1 hour

# Inference
MAX_BATCH_SIZE = 1024
MIN_LATENCY_MS = 1
MAX_LATENCY_MS = 1000
```

## Security

### Access Control
- Provider verification
- Model certification
- Benchmark validation
- Anti-gaming measures

### Resource Protection
- DDoS protection
- Rate limiting
- Compute quotas
- Thermal protection

## Future Enhancements

1. **Federated Training**
   - Multi-accelerator training
   - Gradient aggregation
   - Privacy-preserving training

2. **Model Marketplace**
   - Pre-trained model trading
   - Fine-tuning services
   - Model compression

3. **Edge Computing**
   - Mobile accelerators
   - IoT device integration
   - Distributed inference

4. **Specialized Accelerators**
   - Vision Processing Units
   - Graph Neural Network accelerators
   - Neuromorphic chips 