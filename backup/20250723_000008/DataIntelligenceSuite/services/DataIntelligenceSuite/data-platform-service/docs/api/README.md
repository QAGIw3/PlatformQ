# Data Platform Service API Documentation

## Base URL

```
https://api.platform.com/data-platform-service
```

## Authentication

All API requests require authentication via JWT token:

```
Authorization: Bearer <token>
```

## API Versioning

The API is versioned through the URL path:
- v1: `/api/v1` - Stable API
- v2: `/api/v2` - Latest features (may have breaking changes)

## Common Responses

### Success Response

```json
{
  "success": true,
  "data": {...},
  "message": "Operation successful"
}
```

### Error Response

```json
{
  "success": false,
  "errors": [
    {
      "code": "ERROR_CODE",
      "message": "Error description",
      "field": "field_name"  // Optional
    }
  ]
}
```

## Endpoints

### Health Check

```
GET /health
```

Returns service health status.

### Ready Check

```
GET /ready
```

Returns service readiness status including dependency checks.

See service-specific endpoint documentation.

## Rate Limiting

API requests are rate limited:
- Anonymous: 100 requests/hour
- Authenticated: 1000 requests/hour
- Enterprise: Custom limits

## Webhooks

The service supports webhooks for real-time notifications.

### Webhook Events

- `resource.created`
- `resource.updated`
- `resource.deleted`

## SDKs

Official SDKs are available for:
- Python
- JavaScript/TypeScript
- Go
- Java

## Support

For API support, contact the platform team.
