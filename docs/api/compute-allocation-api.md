# Compute Allocation Service API

The Compute Allocation Service provides a unified API for allocating compute resources across multiple cloud providers and on-premise infrastructure.

## Base URL

```
https://compute-allocation.platformq.io/api/v1
```

## Authentication

All API requests require a valid JWT token in the Authorization header:

```
Authorization: Bearer <jwt-token>
```

## API Endpoints

### Allocate Resources

Allocate compute resources based on requirements and strategy.

**POST** `/allocations`

#### Request Body

```json
{
  "workload_type": "ml-training",
  "workload_id": "model-123",
  "requirements": {
    "cpu_cores": 8,
    "memory_gb": 32,
    "storage_gb": 100,
    "gpu_count": 1,
    "gpu_type": "nvidia-v100",
    "network_bandwidth_gbps": 10,
    "regions": ["us-east-1", "us-west-2"],
    "availability_zones": [],
    "compliance_requirements": []
  },
  "strategy": "COST_OPTIMIZED",
  "duration_hours": 24,
  "pricing_preferences": ["SPOT", "ON_DEMAND"],
  "tags": {
    "project": "ml-research",
    "team": "data-science"
  }
}
```

#### Parameters

- `workload_type` (string, required): Type of workload (e.g., ml-training, simulation, batch-processing)
- `workload_id` (string, required): Unique identifier for the workload
- `requirements` (object, required): Resource requirements
  - `cpu_cores` (integer): Number of CPU cores needed
  - `memory_gb` (integer): Memory in GB
  - `storage_gb` (integer): Storage in GB
  - `gpu_count` (integer): Number of GPUs
  - `gpu_type` (string): Type of GPU (e.g., nvidia-v100, nvidia-a100)
  - `network_bandwidth_gbps` (float): Network bandwidth requirements
  - `regions` (array): Preferred regions
  - `availability_zones` (array): Specific AZs if needed
  - `compliance_requirements` (array): Compliance requirements (e.g., HIPAA, SOC2)
- `strategy` (string): Allocation strategy - COST_OPTIMIZED, PERFORMANCE_OPTIMIZED, or BALANCED
- `duration_hours` (float): Expected duration in hours
- `pricing_preferences` (array): Pricing models in order of preference - ON_DEMAND, SPOT, RESERVED
- `tags` (object): Key-value tags for the allocation

#### Response

```json
{
  "success": true,
  "allocation": {
    "allocation_id": "alloc-uuid-123456",
    "tenant_id": "tenant-123",
    "workload_id": "model-123",
    "workload_type": "ml-training",
    "provider": "AWS",
    "region": "us-east-1",
    "instance_id": "i-0a1b2c3d4e5f",
    "instance_type": "p3.2xlarge",
    "status": "ACTIVE",
    "cpu_cores": 8,
    "memory_gb": 61,
    "storage_gb": 100,
    "gpu_count": 1,
    "gpu_type": "nvidia-v100",
    "cost_per_hour": 3.06,
    "pricing_model": "SPOT",
    "created_at": "2024-01-15T10:30:00Z",
    "activated_at": "2024-01-15T10:31:30Z",
    "expires_at": "2024-01-16T10:30:00Z",
    "access_details": {
      "instance_id": "i-0a1b2c3d4e5f",
      "public_ip": "54.123.45.67",
      "private_ip": "172.16.0.10",
      "dns_name": "ec2-54-123-45-67.compute-1.amazonaws.com",
      "ssh_key": "platformq-ml-training"
    },
    "tags": {
      "project": "ml-research",
      "team": "data-science"
    }
  },
  "message": "Resources allocated successfully"
}
```

### Get Allocation Details

Get details of a specific allocation.

**GET** `/allocations/{allocation_id}`

#### Response

Returns the same allocation object as the create response.

### List Allocations

List all allocations for the authenticated tenant.

**GET** `/allocations`

#### Query Parameters

- `workload_type` (string): Filter by workload type
- `workload_id` (string): Filter by workload ID
- `status` (string): Filter by status (PENDING, PROVISIONING, ACTIVE, TERMINATED, FAILED)
- `provider` (string): Filter by provider
- `page` (integer): Page number (default: 1)
- `limit` (integer): Items per page (default: 20, max: 100)

#### Response

```json
{
  "allocations": [
    {
      "allocation_id": "alloc-uuid-123456",
      "workload_id": "model-123",
      "workload_type": "ml-training",
      "provider": "AWS",
      "region": "us-east-1",
      "status": "ACTIVE",
      "cost_per_hour": 3.06,
      "created_at": "2024-01-15T10:30:00Z"
    }
  ],
  "total": 42,
  "page": 1,
  "limit": 20
}
```

### Modify Allocation

Modify an existing allocation (extend duration or scale resources).

**PUT** `/allocations/{allocation_id}`

#### Request Body

```json
{
  "extend_hours": 12,
  "scale_to": {
    "cpu_cores": 16,
    "memory_gb": 64,
    "storage_gb": 200
  }
}
```

#### Response

```json
{
  "status": "modified",
  "allocation_id": "alloc-uuid-123456"
}
```

### Release Allocation

Release allocated resources.

**DELETE** `/allocations/{allocation_id}`

#### Response

```json
{
  "status": "deallocated",
  "allocation_id": "alloc-uuid-123456"
}
```

### Estimate Costs

Get cost estimates for given requirements.

**GET** `/costs/estimate`

#### Query Parameters

- `cpu_cores` (integer): Number of CPU cores
- `memory_gb` (integer): Memory in GB
- `storage_gb` (integer): Storage in GB
- `gpu_count` (integer): Number of GPUs
- `gpu_type` (string): Type of GPU
- `duration_hours` (float): Duration in hours
- `pricing_model` (string): Pricing model (ON_DEMAND, SPOT, RESERVED)

#### Response

```json
{
  "estimates": {
    "AWS": {
      "cpu_cost": 0.20,
      "memory_cost": 0.16,
      "storage_cost": 0.01,
      "gpu_cost": 2.70,
      "total_hourly_cost": 3.07,
      "total_cost": 73.68,
      "provider": "AWS",
      "region": "us-east-1",
      "pricing_model": "ON_DEMAND"
    },
    "CLOUDSTACK": {
      "cpu_cost": 0.10,
      "memory_cost": 0.08,
      "storage_cost": 0.01,
      "gpu_cost": 1.35,
      "total_hourly_cost": 1.54,
      "total_cost": 36.96,
      "provider": "CLOUDSTACK",
      "region": "us-east-1",
      "pricing_model": "ON_DEMAND"
    }
  },
  "duration_hours": 24,
  "pricing_model": "ON_DEMAND"
}
```

### Get Provider Capabilities

Get capabilities of all registered providers.

**GET** `/providers/capabilities`

#### Response

```json
{
  "aws": {
    "provider_type": "AWS",
    "regions": ["us-east-1", "us-west-2", "eu-west-1"],
    "instance_types": ["t3.micro", "t3.small", "m5.large", "p3.2xlarge"],
    "gpu_types": ["nvidia-v100", "nvidia-a100"],
    "pricing_models": ["ON_DEMAND", "SPOT", "RESERVED"],
    "features": {
      "spot_instances": true,
      "dedicated_hosts": true,
      "auto_scaling": true,
      "load_balancing": true
    },
    "sla_guarantees": {
      "availability": 0.999,
      "network": 0.999
    }
  },
  "cloudstack": {
    "provider_type": "CLOUDSTACK",
    "regions": ["default"],
    "instance_types": ["small", "medium", "large", "xlarge"],
    "gpu_types": ["nvidia-v100"],
    "pricing_models": ["ON_DEMAND"],
    "features": {
      "spot_instances": false,
      "dedicated_hosts": true,
      "auto_scaling": true,
      "load_balancing": true
    },
    "sla_guarantees": {
      "availability": 0.99,
      "network": 0.999
    }
  }
}
```

### Get Allocation Metrics

Get aggregated metrics for all allocations.

**GET** `/metrics/allocations`

#### Response

```json
{
  "total_allocations": 156,
  "active_allocations": 42,
  "total_cost_usd": 523.45,
  "by_provider": {
    "AWS": {
      "count": 25,
      "cost": 380.50,
      "cpu_cores": 200,
      "memory_gb": 800,
      "gpu_count": 10
    },
    "CLOUDSTACK": {
      "count": 17,
      "cost": 142.95,
      "cpu_cores": 136,
      "memory_gb": 544,
      "gpu_count": 4
    }
  },
  "timestamp": "2024-01-15T12:00:00Z"
}
```

## Error Responses

All endpoints return consistent error responses:

```json
{
  "detail": "Error message describing what went wrong"
}
```

### Common HTTP Status Codes

- `200` - Success
- `400` - Bad Request (invalid parameters)
- `401` - Unauthorized (missing or invalid token)
- `403` - Forbidden (insufficient permissions)
- `404` - Not Found
- `409` - Conflict (resource already exists)
- `429` - Too Many Requests (rate limited)
- `500` - Internal Server Error

## Rate Limiting

API requests are rate limited to:
- 100 requests per minute for allocation creation
- 1000 requests per minute for read operations

Rate limit headers are included in responses:
- `X-RateLimit-Limit`: Maximum requests per window
- `X-RateLimit-Remaining`: Requests remaining in current window
- `X-RateLimit-Reset`: Unix timestamp when window resets

## Webhooks

The service can send webhooks for allocation events. Configure webhook endpoints in your tenant settings.

### Webhook Events

- `allocation.created` - New allocation created
- `allocation.active` - Allocation became active
- `allocation.failed` - Allocation failed
- `allocation.terminated` - Allocation terminated
- `allocation.expiring` - Allocation expiring soon (1 hour warning)

### Webhook Payload

```json
{
  "event": "allocation.active",
  "timestamp": "2024-01-15T10:31:30Z",
  "allocation": {
    "allocation_id": "alloc-uuid-123456",
    "workload_id": "model-123",
    "status": "ACTIVE",
    "provider": "AWS",
    "region": "us-east-1"
  }
}
``` 