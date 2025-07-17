# Federated Learning Service

The Federated Learning Service enables privacy-preserving collaborative machine learning across multiple tenants without sharing raw data, now enhanced with homomorphic encryption and advanced privacy techniques.

## 🎯 Overview

This service orchestrates federated learning sessions where multiple participants can collaboratively train ML models while keeping their data private. It integrates with:
- **Apache Spark** for distributed model training
- **Apache Ignite** for high-performance in-memory aggregation
- **Verifiable Credentials** for access control and result certification
- **Differential Privacy** with adaptive clipping and RDP accounting
- **Homomorphic Encryption** (CKKS & Paillier) for secure aggregation
- **Zero-Knowledge Proofs** for model validation
- **Secure Multi-Party Computation** for Byzantine robustness

## 🏗️ Architecture

### Key Components

1. **Session Coordinator**
   - Manages federated learning sessions
   - Verifies participant credentials
   - Orchestrates training rounds
   - Triggers model aggregation

2. **Privacy-Preserving Training**
   - Differential privacy with configurable epsilon/delta
   - Secure aggregation with encryption
   - Zero-knowledge proofs for training correctness
   - No raw data sharing between participants

3. **Model Aggregation**
   - FedAvg (Federated Averaging) algorithm
   - Weighted aggregation based on sample size
   - Convergence tracking
   - In-memory processing with Ignite

4. **Access Control**
   - Verifiable credential requirements
   - Reputation score thresholds
   - Tenant-based restrictions
   - OIDC authentication via auth-service

## 📡 API Endpoints

### Session Management
- `POST /api/v1/sessions` - Create a new federated learning session
- `GET /api/v1/sessions/{session_id}/status` - Get session status
- `POST /api/v1/sessions/{session_id}/join` - Join a session as participant
- `GET /api/v1/sessions/{session_id}/model` - Get aggregated model

### Model Updates
- `POST /api/v1/sessions/{session_id}/updates` - Submit model update
- `GET /api/v1/sessions/{session_id}/updates/{round}` - Get round updates

### Homomorphic Encryption
- `POST /api/v1/sessions/{session_id}/encrypt` - Encrypt model weights
- `POST /api/v1/sessions/{session_id}/secure_aggregate` - Trigger secure aggregation

### Privacy & Verification
- `POST /api/v1/sessions/{session_id}/generate_zkp` - Generate zero-knowledge proof
- `POST /api/v1/verify_zkp` - Verify zero-knowledge proof
- `GET /api/v1/sessions/{session_id}/privacy_report` - Get privacy budget report

## 🔐 Privacy Features

### Homomorphic Encryption
- **CKKS Scheme**: Approximate arithmetic on encrypted real numbers
- **Paillier Scheme**: Additive homomorphic encryption
- **Encrypted Aggregation**: Aggregate without decrypting individual updates
- **Noise Budget Management**: Track encryption quality throughout computation

### Advanced Differential Privacy
- **Adaptive Clipping**: Dynamic gradient clipping based on quantiles
- **RDP Accounting**: Tight privacy bounds using Rényi Differential Privacy
- **Multiple Mechanisms**: Gaussian, Laplace, Exponential, Geometric noise
- **Per-layer Sensitivity**: Compute optimal noise for each model layer

### Secure Multi-Party Computation
- **Secret Sharing**: Distribute model updates across participants
- **Beaver Triples**: Efficient multiplication of encrypted values
- **Byzantine Tolerance**: Handle up to 20% malicious participants
- **Dropout Resilience**: Continue training despite participant failures

### Zero-Knowledge Proofs
- **Training Correctness**: Prove proper training without revealing data
- **Norm Bounds**: Verify gradient norms stay within limits
- **Range Proofs**: Validate parameters are in acceptable ranges
- **Verifiable Aggregation**: Prove aggregation was done correctly

## 🚀 Usage Example

### 1. Create a Federated Learning Session

```python
import requests

# Create session for collaborative fraud detection
response = requests.post(
    "http://federated-learning-service/api/v1/sessions",
    headers={"Authorization": "Bearer <token>"},
    json={
        "model_type": "CLASSIFICATION",
        "algorithm": "LogisticRegression",
        "dataset_requirements": {
            "min_samples": 1000,
            "features": ["transaction_amount", "merchant_category", "time_of_day"],
            "label_column": "is_fraud"
        },
        "privacy_parameters": {
            "differential_privacy_enabled": True,
            "epsilon": 1.0,
            "delta": 1e-5,
            "secure_aggregation": True,
            "homomorphic_encryption": True,
            "encryption_scheme": "CKKS",
            "aggregation_strategy": "SECURE_AGG",
            "adaptive_clipping": True,
            "byzantine_tolerance": 0.2
        },
        "training_parameters": {
            "rounds": 10,
            "min_participants": 3,
            "max_participants": 10,
            "model_hyperparameters": {
                "maxIter": 100,
                "regParam": 0.1
            }
        },
        "participation_criteria": {
            "required_credentials": [
                {
                    "credential_type": "DataProviderCredential",
                    "min_trust_score": 0.8
                }
            ],
            "min_reputation_score": 50
        }
    }
)

session_id = response.json()["session_id"]
```

### 2. Join as a Participant

```python
# Join the session with your private dataset
response = requests.post(
    f"http://federated-learning-service/api/v1/sessions/{session_id}/join",
    headers={"Authorization": "Bearer <participant_token>"},
    json={
        "session_id": session_id,
        "dataset_stats": {
            "num_samples": 5000,
            "num_features": 3,
            "data_hash": "sha256:abcd1234...",
            "class_distribution": {"0": 4800, "1": 200}
        },
        "compute_capabilities": {
            "spark_executors": 4,
            "memory_gb": 16.0,
            "gpu_available": False
        },
        "public_key": "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgk..."
    }
)
```

### 3. Encrypt Model Updates (with HE enabled)

```python
# Encrypt your model weights before submission
response = requests.post(
    f"http://federated-learning-service/api/v1/sessions/{session_id}/encrypt",
    headers={"Authorization": "Bearer <participant_token>"},
    json={
        "layer1_weights": model_weights["layer1"].tolist(),
        "layer1_bias": model_bias["layer1"].tolist(),
        "layer2_weights": model_weights["layer2"].tolist(),
        "layer2_bias": model_bias["layer2"].tolist()
    }
)

encrypted_uri = response.json()["encryption_uri"]
```

### 4. Training Happens Automatically

The service will:
1. Wait for minimum participants
2. Start training rounds
3. Participants train locally with differential privacy
4. Encrypt model updates using homomorphic encryption
5. Submit encrypted updates (never decrypted by server)
6. Aggregate encrypted updates homomorphically
7. Apply Byzantine-robust filtering if needed
8. Repeat for configured rounds

### 5. Get the Final Model

```python
# Get aggregated model after training
response = requests.get(
    f"http://federated-learning-service/api/v1/sessions/{session_id}/model",
    headers={"Authorization": "Bearer <token>"}
)

model_info = response.json()
# {
#     "model_uri": "s3://federated-learning/aggregated/...",
#     "num_participants": 5,
#     "total_samples": 25000,
#     "verifiable_credential_id": "urn:uuid:...",
#     "convergence_score": 0.95
# }
```

### 6. Check Privacy Budget

```python
# Monitor privacy budget consumption
response = requests.get(
    f"http://federated-learning-service/api/v1/sessions/{session_id}/privacy_report",
    headers={"Authorization": "Bearer <token>"}
)

privacy_report = response.json()
# {
#     "differential_privacy_enabled": true,
#     "epsilon": 1.0,
#     "delta": 1e-5,
#     "initial_budget": 10.0,
#     "remaining_budget": 8.5,
#     "rounds_completed": 3,
#     "homomorphic_encryption_enabled": true,
#     "encryption_scheme": "CKKS"
# }
```

## 🔧 Configuration

### Environment Variables
- `PULSAR_URL`: Apache Pulsar broker URL
- `VC_SERVICE_URL`: Verifiable Credential service URL
- `AUTH_SERVICE_URL`: Authentication service URL
- `SPARK_MASTER_URL`: Spark master URL
- `IGNITE_NODES`: Comma-separated Ignite nodes

### Privacy Settings
Configure in session creation:
- `epsilon`: Privacy budget (lower = more private)
- `delta`: Privacy parameter
- `secure_aggregation`: Enable encryption
- `homomorphic_encryption`: Future enhancement

## 📊 Monitoring

### Metrics
- Session creation rate
- Participant join rate
- Round completion time
- Model convergence metrics
- Privacy budget usage

### Events Published
- `federated_learning_initiated`
- `federated_participant_joined`
- `federated_model_update`
- `federated_round_completed`

## 🏢 Multi-Tenant Support

- Complete tenant isolation
- No cross-tenant data access
- Per-tenant session management
- Tenant-specific access controls

## 🔍 Supported Algorithms

### Classification
- Logistic Regression (with DP)
- Random Forest
- Neural Networks (coming soon)

### Regression
- Linear Regression (with DP)
- Gradient Boosted Trees

### Custom Models
- Bring your own Spark ML pipeline
- Must support weight extraction

## 🛡️ Security Considerations

1. **Data Privacy**: No raw data leaves participant's environment
2. **Model Privacy**: Updates encrypted during transmission
3. **Access Control**: VC-based participation requirements
4. **Audit Trail**: All actions logged with tenant context
5. **Secure Communication**: TLS for all API calls

## 🚧 Limitations

- Maximum 100 participants per session (configurable)
- Models must fit Spark ML pipeline format
- CKKS multiplication depth limited to ~10 levels
- GPU acceleration for HE operations planned
- Full MPC protocols still in development

## 🚀 Future Enhancements

- **Trusted Execution Environments**: Intel SGX integration
- **Functional Encryption**: More flexible computation on encrypted data
- **Threshold Cryptography**: Distributed key generation
- **Quantum-Safe Cryptography**: Post-quantum secure protocols
- **Cross-Silo Federation**: Support for institutional deployments 