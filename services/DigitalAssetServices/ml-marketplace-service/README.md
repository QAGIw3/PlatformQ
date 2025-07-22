# ML Marketplace Service

Decentralized marketplace for discovering, sharing, and monetizing ML models and datasets.

## Overview

The ML Marketplace Service provides a platform for data scientists and ML engineers to:
- **Publish Models**: Share trained models with the community or organization
- **Discover Models**: Search and find pre-trained models for various tasks
- **Monetize Work**: Set prices and licensing terms for your models
- **Rate & Review**: Build reputation through community feedback

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  ML Marketplace Service                      │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │   Model     │  │    Search    │  │   Recommendation │  │
│  │  Registry   │  │    Engine    │  │     Engine       │  │
│  └──────┬──────┘  └──────┬───────┘  └────────┬─────────┘  │
│         │                 │                    │             │
│  ┌──────┴─────────────────┴───────────────────┴─────────┐  │
│  │              Apache Ignite (Storage)                  │  │
│  └───────────────────────┬───────────────────────────────┘  │
│                          │                                   │
│  ┌───────────────────────┴───────────────────────────────┐  │
│  │              Apache Pulsar (Events)                   │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## Features

### Model Publishing
- Upload trained models with metadata
- Set visibility (private, organization, public)
- Define licensing terms (MIT, Apache, proprietary, etc.)
- Set pricing for commercial models
- Version management

### Model Discovery
- Full-text search across names, descriptions, and tags
- Filter by category, framework, license, price
- Sort by relevance, rating, downloads, price
- Browse trending and recommended models

### Marketplace Features
- **Ratings & Reviews**: 5-star rating system with text reviews
- **Download Tracking**: Track model popularity
- **Revenue Sharing**: Built-in payment handling (future)
- **Recommendations**: Personalized model suggestions
- **Categories**: Computer Vision, NLP, Time Series, etc.

### Security & Privacy
- Private models for internal use
- Organization-level sharing
- Access control for downloads
- Secure model storage

## API Endpoints

### Model Publishing

#### Publish Model
```http
POST /api/v1/models/publish?publisher_id={user_id}
Content-Type: application/json

{
  "model_id": "model_123",
  "name": "BERT Fine-tuned for Sentiment Analysis",
  "description": "BERT model fine-tuned on product reviews",
  "category": "nlp",
  "visibility": "public",
  "license": "apache_2.0",
  "price": 0.0,
  "tags": ["nlp", "sentiment", "bert", "transformers"],
  "framework": "pytorch",
  "version": "1.0.0",
  "metrics": {
    "accuracy": 0.94,
    "f1_score": 0.93,
    "dataset": "Amazon Product Reviews"
  },
  "requirements": {
    "python": ">=3.8",
    "torch": ">=1.9.0",
    "transformers": ">=4.0.0"
  }
}
```

### Model Discovery

#### Search Models
```http
POST /api/v1/models/search
Content-Type: application/json

{
  "query": "sentiment analysis",
  "category": "nlp",
  "tags": ["bert"],
  "min_rating": 4.0,
  "max_price": 100.0,
  "framework": "pytorch",
  "sort_by": "relevance",
  "limit": 20,
  "offset": 0
}
```

#### Get Model Details
```http
GET /api/v1/models/{marketplace_id}
```

### Model Usage

#### Download Model
```http
POST /api/v1/models/{marketplace_id}/download?user_id={user_id}
```

Response:
```json
{
  "status": "success",
  "model_id": "model_123",
  "download_url": "/models/model_123/download",
  "expires_in": 3600
}
```

#### Rate Model
```http
POST /api/v1/models/{marketplace_id}/rate?user_id={user_id}
Content-Type: application/json

{
  "rating": 5,
  "review": "Excellent model! Works great for my use case."
}
```

### Discovery Features

#### Get Trending Models
```http
GET /api/v1/trending?limit=10
```

#### Get Recommendations
```http
GET /api/v1/recommendations?user_id={user_id}&limit=10
```

#### List Categories
```http
GET /api/v1/categories
```

#### List Licenses
```http
GET /api/v1/licenses
```

### Analytics

#### Marketplace Statistics
```http
GET /api/v1/stats
```

## Configuration

### Environment Variables

```bash
# Apache Ignite
IGNITE_HOST=ignite
IGNITE_PORT=10800

# Apache Pulsar
PULSAR_URL=pulsar://pulsar:6650

# Service Configuration
LOG_LEVEL=INFO
```

## Deployment

### Docker

```bash
# Build image
docker build -t ml-marketplace-service:latest .

# Run container
docker run -d \
  --name ml-marketplace-service \
  -p 8000:8000 \
  -e IGNITE_HOST=ignite \
  -e PULSAR_URL=pulsar://pulsar:6650 \
  ml-marketplace-service:latest
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ml-marketplace-service
spec:
  replicas: 3
  selector:
    matchLabels:
      app: ml-marketplace-service
  template:
    metadata:
      labels:
        app: ml-marketplace-service
    spec:
      containers:
      - name: ml-marketplace-service
        image: ml-marketplace-service:latest
        ports:
        - containerPort: 8000
        env:
        - name: IGNITE_HOST
          value: "ignite"
        - name: PULSAR_URL
          value: "pulsar://pulsar:6650"
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
```

## Usage Examples

### Python Client

```python
import httpx
import asyncio

class MarketplaceClient:
    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url
        self.client = httpx.AsyncClient()
    
    async def publish_model(self, model_data, publisher_id):
        response = await self.client.post(
            f"{self.base_url}/api/v1/models/publish",
            params={"publisher_id": publisher_id},
            json=model_data
        )
        return response.json()
    
    async def search_models(self, query=None, category=None, tags=None):
        response = await self.client.post(
            f"{self.base_url}/api/v1/models/search",
            json={
                "query": query,
                "category": category,
                "tags": tags,
                "sort_by": "relevance"
            }
        )
        return response.json()
    
    async def download_model(self, marketplace_id, user_id):
        response = await self.client.post(
            f"{self.base_url}/api/v1/models/{marketplace_id}/download",
            params={"user_id": user_id}
        )
        return response.json()

# Example usage
async def main():
    client = MarketplaceClient()
    
    # Publish a model
    model_data = {
        "model_id": "my_model_123",
        "name": "Customer Churn Predictor",
        "description": "XGBoost model for predicting customer churn",
        "category": "tabular",
        "visibility": "public",
        "license": "mit",
        "tags": ["churn", "classification", "xgboost"],
        "framework": "scikit-learn",
        "metrics": {
            "auc": 0.89,
            "precision": 0.85,
            "recall": 0.82
        }
    }
    
    result = await client.publish_model(model_data, "user_123")
    print(f"Published model: {result['marketplace_id']}")
    
    # Search for models
    results = await client.search_models(
        query="churn prediction",
        category="tabular"
    )
    print(f"Found {results['total']} models")
    
    # Download a model
    if results['models']:
        model = results['models'][0]
        download = await client.download_model(
            model['marketplace_id'],
            "user_456"
        )
        print(f"Download URL: {download['download_url']}")

asyncio.run(main())
```

## Model Categories

- **Computer Vision**: Image classification, object detection, segmentation
- **NLP**: Text classification, NER, sentiment analysis, translation
- **Tabular**: Classification, regression, clustering
- **Time Series**: Forecasting, anomaly detection
- **Reinforcement Learning**: Game agents, control systems
- **Generative**: GANs, VAEs, diffusion models
- **Anomaly Detection**: Outlier detection, fraud detection

## License Types

- **MIT**: Permissive open-source license
- **Apache 2.0**: Permissive with patent protection
- **GPL 3.0**: Copyleft open-source license
- **BSD 3-Clause**: Permissive with attribution
- **Proprietary**: Custom commercial license
- **Custom**: User-defined license terms

## Best Practices

### For Publishers
1. **Clear Documentation**: Provide detailed model descriptions
2. **Performance Metrics**: Include validation metrics
3. **Requirements**: List all dependencies
4. **Examples**: Provide usage examples
5. **Versioning**: Use semantic versioning

### For Consumers
1. **Check Ratings**: Review community feedback
2. **Verify Requirements**: Ensure compatibility
3. **Test First**: Use demo/trial when available
4. **Leave Reviews**: Help the community

## Monitoring

### Metrics
- Total models published
- Downloads per model
- Search queries
- User engagement
- Revenue metrics

### Health Checks
- Ignite connectivity
- Pulsar connectivity
- Cache performance
- Search latency

## Development

### Local Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Run locally
python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Testing

```bash
# Run unit tests
pytest tests/

# Run with coverage
pytest --cov=app tests/

# Run integration tests
pytest tests/integration/
```

## Troubleshooting

### Common Issues

1. **Model not found**
   - Check marketplace_id is correct
   - Verify model is active
   - Check visibility settings

2. **Search returns no results**
   - Broaden search criteria
   - Check spelling
   - Try different categories

3. **Download fails**
   - Verify user permissions
   - Check if payment required
   - Ensure model is active

4. **Slow search**
   - Check Ignite cache
   - Monitor network latency
   - Review search complexity

## Future Enhancements

- Blockchain integration for decentralized payments
- IPFS storage for distributed model hosting
- Advanced recommendation algorithms
- Model performance benchmarking
- Automated model validation
- Dataset marketplace
- Model composition and pipelines

## License

This service is part of the PlatformQ project and follows the project's licensing terms. 