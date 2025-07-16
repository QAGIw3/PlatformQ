# Federated Learning Service

The Federated Learning Service enables privacy-preserving collaborative machine learning across multiple tenants without sharing raw data.

## 🎯 Overview

This service orchestrates federated learning sessions where multiple participants can collaboratively train ML models while keeping their data private. It integrates with:
- **Apache Spark** for distributed model training
- **Apache Ignite** for high-performance in-memory aggregation
- **Verifiable Credentials** for access control and result certification
- **Differential Privacy** for privacy guarantees

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

## 🔐 Privacy Features

### Differential Privacy
- Configurable privacy budget (epsilon, delta)
- Laplace noise addition to gradients
- Per-participant privacy accounting

### Secure Aggregation
- RSA encryption of model updates
- Homomorphic properties for aggregation
- Ephemeral key generation

### Zero-Knowledge Proofs
- Proof of correct training execution
- No data revelation
- Verifiable by coordinator

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
            "secure_aggregation": True
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

### 3. Training Happens Automatically

The service will:
1. Wait for minimum participants
2. Start training rounds
3. Participants train locally with differential privacy
4. Submit encrypted model updates
5. Aggregate updates securely
6. Repeat for configured rounds

### 4. Get the Final Model

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

- Maximum 100 participants per session
- Models must fit Spark ML pipeline format
- Homomorphic encryption in development
- GPU acceleration planned for future 