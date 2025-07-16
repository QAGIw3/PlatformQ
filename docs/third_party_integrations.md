# PlatformQ Third-Party Integrations

This document provides comprehensive documentation for all third-party integrations in PlatformQ, including Nextcloud, Zulip, and OpenProject.

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Client Libraries](#client-libraries)
   - [Nextcloud Client](#nextcloud-client)
   - [Zulip Client](#zulip-client)
   - [OpenProject Client](#openproject-client)
4. [Resilience Features](#resilience-features)
5. [Webhook Integration](#webhook-integration)
6. [Service Implementations](#service-implementations)
7. [Configuration](#configuration)
8. [Examples](#examples)
9. [Testing](#testing)
10. [Troubleshooting](#troubleshooting)

## Overview

PlatformQ integrates with three major collaboration platforms to provide a comprehensive project management and collaboration environment:

- **Nextcloud**: File storage, sharing, and collaboration
- **Zulip**: Team communication and messaging
- **OpenProject**: Project management and issue tracking

Each integration is built with production-grade features including:
- Comprehensive API coverage
- Resilience patterns (circuit breakers, rate limiting, retries)
- Webhook support for real-time events
- Connection pooling for performance
- Extensive error handling

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         PlatformQ Services                        │
├─────────────────────────────────────────────────────────────────┤
│  ┌───────────────┐  ┌────────────────┐  ┌──────────────────┐   │
│  │ Projects Svc  │  │ Provisioning   │  │ Notification     │   │
│  │               │  │ Service        │  │ Service          │   │
│  └───────┬───────┘  └────────┬───────┘  └────────┬─────────┘   │
│          │                    │                    │              │
│          └────────────────────┴────────────────────┘              │
│                               │                                   │
│  ┌────────────────────────────┴─────────────────────────────┐   │
│  │                    Shared Library                         │   │
│  ├───────────────────────────────────────────────────────────┤   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌────────────────┐ │   │
│  │  │ Nextcloud    │  │ Zulip        │  │ OpenProject    │ │   │
│  │  │ Client       │  │ Client       │  │ Client         │ │   │
│  │  └──────┬───────┘  └──────┬───────┘  └────────┬───────┘ │   │
│  │         │                  │                    │         │   │
│  │  ┌──────┴──────────────────┴────────────────────┴──────┐ │   │
│  │  │              Resilience Module                       │ │   │
│  │  │  (Circuit Breakers, Rate Limiting, Retries)         │ │   │
│  │  └──────────────────────────────────────────────────────┘ │   │
│  └────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    External Services                             │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────┐        │
│  │  Nextcloud   │  │    Zulip     │  │  OpenProject   │        │
│  └──────────────┘  └──────────────┘  └────────────────┘        │
└─────────────────────────────────────────────────────────────────┘
```

## Client Libraries

### Nextcloud Client

The Nextcloud client (`libs/shared/platformq_shared/nextcloud_client.py`) provides comprehensive access to Nextcloud's APIs.

#### Features

- **User Management**: Create, update, delete users; manage quotas and groups
- **File Operations**: Upload, download, delete, move, copy files and folders
- **Sharing**: Create shares with various permission levels
- **WebDAV**: Direct WebDAV protocol support
- **Groups**: Create and manage groups
- **Search**: Search for files and folders

#### Basic Usage

```python
from platformq_shared.nextcloud_client import NextcloudClient

# Initialize client
client = NextcloudClient(
    nextcloud_url="https://nextcloud.example.com",
    admin_user="admin",
    admin_pass="password",
    use_connection_pool=True,
    rate_limit=10.0  # 10 requests per second
)

# Create a user
client.create_user(
    username="john.doe",
    password="secure_password",
    email="john@example.com",
    display_name="John Doe",
    groups=["users", "project-team"],
    quota="5GB"
)

# Upload a file
with open("document.pdf", "rb") as f:
    client.upload_file(
        local_file=f,
        remote_path="/Projects/Q1/document.pdf"
    )

# Create a share
share_info = client.create_share(
    path="/Projects/Q1",
    share_type=0,  # User share
    share_with="jane.doe",
    permissions=31  # All permissions
)
```

### Zulip Client

The Zulip client (`libs/shared/platformq_shared/zulip_client.py`) provides full access to Zulip's messaging APIs.

#### Features

- **Messaging**: Send messages to streams and users, edit/delete messages
- **Streams**: Create, update, delete streams; manage subscriptions
- **Users**: Create and manage users, update profiles
- **File Uploads**: Upload files with proper MIME type handling
- **Reactions**: Add/remove reactions to messages
- **Presence**: Track user presence and availability

#### Basic Usage

```python
from platformq_shared.zulip_client import ZulipClient

# Initialize client
client = ZulipClient(
    zulip_site="https://zulip.example.com",
    zulip_email="bot@example.com",
    zulip_api_key="your-api-key",
    use_connection_pool=True,
    rate_limit=20.0  # 20 requests per second
)

# Send a message
message_id = client.send_message(
    content="Welcome to the project!",
    message_type="stream",
    to="general",
    topic="Announcements"
)

# Create a stream
client.create_stream(
    name="project-alpha",
    description="Discussion for Project Alpha",
    invite_only=False,
    stream_post_policy=1  # Anyone can post
)

# Subscribe users
client.subscribe_users(
    streams=["project-alpha"],
    principals=["john@example.com", "jane@example.com"]
)
```

### OpenProject Client

The OpenProject client (`libs/shared/platformq_shared/openproject_client.py`) provides comprehensive project management capabilities.

#### Features

- **Projects**: Create, update, delete projects with hierarchy support
- **Work Packages**: Full CRUD operations, comments, custom fields
- **Users**: User management and membership assignment
- **Time Tracking**: Log time entries and activities
- **Attachments**: Upload and manage attachments
- **Queries**: Create and execute saved queries

#### Basic Usage

```python
from platformq_shared.openproject_client import OpenProjectClient

# Initialize client
client = OpenProjectClient(
    openproject_url="https://openproject.example.com",
    api_key="your-api-key",
    use_connection_pool=True,
    rate_limit=15.0  # 15 requests per second
)

# Create a project
project = client.create_project(
    name="Project Alpha",
    identifier="project-alpha",
    description="Revolutionary new project",
    public=False,
    active=True
)

# Create a work package
work_package = client.create_work_package(
    project_id=project["id"],
    subject="Design system architecture",
    type_id=1,  # Task
    description="Design the overall system architecture",
    assignee_id=123,
    start_date="2024-01-15",
    due_date="2024-01-30"
)

# Add a comment
client.add_work_package_comment(
    work_package["id"],
    "Started working on the architecture diagrams"
)
```

## Resilience Features

All clients include advanced resilience patterns through the `resilience.py` module:

### Circuit Breaker

Prevents cascading failures by stopping requests to failing services:

```python
# Circuit breaker opens after 5 consecutive failures
# Stays open for 60 seconds before attempting recovery
```

### Rate Limiting

Prevents overwhelming external services:

```python
# Each client has configurable rate limits
# Uses token bucket algorithm for smooth traffic shaping
```

### Retry Logic

Automatic retries with exponential backoff:

```python
# 3 retry attempts by default
# Exponential backoff with jitter
# Only retries on retryable errors (network, 5xx)
```

### Connection Pooling

Reuses HTTP connections for better performance:

```python
# Shared connection pool across all clients
# Configurable pool size and timeout
```

### Usage Example

```python
from platformq_shared.resilience import with_resilience

@with_resilience(
    service_name="my-service",
    circuit_breaker_threshold=5,
    circuit_breaker_timeout=60,
    rate_limit=10.0,
    max_retries=3
)
def my_api_call():
    # Your API call here
    pass
```

## Webhook Integration

The webhook module (`libs/shared/platformq_shared/webhooks.py`) provides a unified interface for handling webhooks from all three platforms.

### Features

- **Security**: Signature verification for all platforms
- **Event Models**: Typed event models with validation
- **Background Processing**: Non-blocking webhook processing
- **Custom Handlers**: Register custom handlers for specific events
- **Metrics**: Built-in metrics collection

### Setup

```python
from platformq_shared.webhooks import setup_webhooks

# In your FastAPI app startup
setup_webhooks(app, config_loader, event_publisher)
```

### Custom Handler Example

```python
from platformq_shared.webhooks import WebhookManager

# Get webhook manager
webhook_manager = app.state.webhook_manager

# Register custom handler
async def handle_file_created(event):
    file_path = event.data.get('file', {}).get('path')
    print(f"New file created: {file_path}")
    # Process the file...

webhook_manager.register_handler(
    'nextcloud', 
    'file_created', 
    handle_file_created
)
```

### Webhook Endpoints

After setup, the following endpoints are available:

- `/webhooks/nextcloud` - Nextcloud webhooks
- `/webhooks/zulip` - Zulip webhooks  
- `/webhooks/openproject` - OpenProject webhooks

## Service Implementations

### Projects Service

The projects service orchestrates resources across all three platforms:

```python
# Creates synchronized resources:
# 1. Nextcloud folder structure
# 2. OpenProject project
# 3. Zulip stream
# 4. Cross-platform permissions
```

### Provisioning Service

Handles user provisioning with comprehensive setup:

```python
# For each new user:
# 1. Creates Nextcloud account
# 2. Sets up personal folders
# 3. Assigns groups and quotas
# 4. Creates welcome content
```

### Notification Service

Sends rich notifications via Zulip:

```python
# Features:
# - Formatted messages with markdown
# - Priority levels
# - User mentions
# - Quiet hours support
# - Activity aggregation
```

## Configuration

### Environment Variables

```bash
# Nextcloud
NEXTCLOUD_URL=https://nextcloud.example.com
NEXTCLOUD_ADMIN_USER=admin
NEXTCLOUD_ADMIN_PASS=secure_password

# Zulip
ZULIP_SITE=https://zulip.example.com
ZULIP_EMAIL=bot@example.com
ZULIP_API_KEY=your-api-key

# OpenProject
OPENPROJECT_URL=https://openproject.example.com
OPENPROJECT_API_KEY=your-api-key

# Resilience settings
CIRCUIT_BREAKER_THRESHOLD=5
CIRCUIT_BREAKER_TIMEOUT=60
HTTP_TIMEOUT=30
MAX_RETRIES=3
```

### Secrets Management

Store sensitive data in HashiCorp Vault:

```python
# Vault paths
platformq/nextcloud/admin_user
platformq/nextcloud/admin_pass
platformq/nextcloud/webhook_secret
platformq/zulip/email
platformq/zulip/api_key
platformq/zulip/webhook_secret
platformq/openproject/api_key
platformq/openproject/webhook_secret
```

## Examples

### Complete Project Setup

```python
async def setup_collaborative_project(name: str, members: List[str]):
    """Set up a complete collaborative project"""
    
    # Initialize clients
    nc = get_nextcloud_client()
    zulip = get_zulip_client()
    op = get_openproject_client()
    
    # 1. Create Nextcloud folder
    folder = f"Projects/{name}"
    nc.create_folder(folder)
    nc.create_folder(f"{folder}/Documents")
    nc.create_folder(f"{folder}/Resources")
    
    # 2. Create OpenProject project
    project = op.create_project(
        name=name,
        identifier=name.lower().replace(" ", "-"),
        description=f"Collaborative workspace for {name}"
    )
    
    # 3. Create Zulip stream
    zulip.create_stream(
        name=name,
        description=f"Discussion for {name} project"
    )
    
    # 4. Add members to all platforms
    for member in members:
        # Share Nextcloud folder
        nc.create_share(
            path=folder,
            share_type=0,
            share_with=member,
            permissions=31
        )
        
        # Add to OpenProject
        # (would need user ID lookup)
        
        # Subscribe to Zulip stream
        zulip.subscribe_users(
            streams=[name],
            principals=[f"{member}@example.com"]
        )
    
    # 5. Send welcome message
    zulip.send_message(
        content=f"Welcome to {name}! Check out the project folder and OpenProject workspace.",
        message_type="stream",
        to=name,
        topic="Welcome"
    )
    
    return {
        "nextcloud_folder": folder,
        "openproject_id": project["id"],
        "zulip_stream": name
    }
```

### Bulk User Provisioning

```python
async def provision_team(team_name: str, users: List[Dict]):
    """Provision an entire team"""
    
    provisioner = NextcloudProvisioner(
        admin_user=config.NEXTCLOUD_ADMIN_USER,
        admin_pass=config.NEXTCLOUD_ADMIN_PASS,
        nextcloud_url=config.NEXTCLOUD_URL,
        default_groups=["users", f"team-{team_name}"]
    )
    
    # Start provisioner
    provisioner.start()
    
    # Queue all users
    provisioner.bulk_provision_users(
        tenant_id=team_name,
        users=users,
        batch_size=10,
        delay=0.5
    )
    
    # Wait for completion (in practice, this would be async)
    import time
    time.sleep(len(users) * 2)
    
    # Get stats
    stats = provisioner.get_stats()
    print(f"Provisioned {stats['users_created']} users")
    print(f"Failed: {stats['users_failed']}")
    print(f"Skipped: {stats['users_skipped']}")
```

### Activity Monitoring

```python
async def monitor_project_activity(project_name: str):
    """Monitor all activity for a project"""
    
    # Set up webhook handlers
    activity_log = []
    
    async def log_nextcloud_activity(event):
        activity_log.append({
            "platform": "nextcloud",
            "type": event.event_type,
            "user": event.user,
            "timestamp": event.timestamp
        })
    
    async def log_zulip_activity(event):
        activity_log.append({
            "platform": "zulip",
            "type": event.event_type,
            "user": event.user_email,
            "timestamp": event.timestamp
        })
    
    async def log_openproject_activity(event):
        activity_log.append({
            "platform": "openproject",
            "type": event.event_type,
            "user": event.user.get("name") if event.user else None,
            "timestamp": event.timestamp
        })
    
    # Register handlers
    webhook_manager.register_handler("nextcloud", "file_created", log_nextcloud_activity)
    webhook_manager.register_handler("zulip", "message", log_zulip_activity)
    webhook_manager.register_handler("openproject", "work_package:updated", log_openproject_activity)
    
    # Return activity summary
    return activity_log
```

## Testing

### Unit Tests

Run unit tests for all clients:

```bash
cd libs/shared
pytest tests/integration/test_nextcloud_client.py
pytest tests/integration/test_zulip_client.py  
pytest tests/integration/test_openproject_client.py
pytest tests/integration/test_resilience.py
```

### Integration Tests

The test suite includes mock servers for all three platforms:

```python
# conftest.py provides mock servers
def test_nextcloud_operations(nextcloud_mock_server):
    client = NextcloudClient(
        nextcloud_url=nextcloud_mock_server.url,
        admin_user="test",
        admin_pass="test"
    )
    
    # Test operations against mock server
    assert client.create_user(username="test", ...)
```

### Manual Testing

Use the provided scripts for manual testing:

```bash
# Test Nextcloud
python scripts/test_nextcloud_integration.py

# Test Zulip
python scripts/test_zulip_integration.py

# Test OpenProject
python scripts/test_openproject_integration.py
```

## Troubleshooting

### Common Issues

#### Connection Errors

```python
# Check service availability
try:
    client.test_connection()
except Exception as e:
    print(f"Service unavailable: {e}")
```

#### Rate Limiting

```python
# Adjust rate limits if seeing 429 errors
client = NextcloudClient(
    ...,
    rate_limit=5.0  # Reduce to 5 requests/second
)
```

#### Circuit Breaker Open

```python
# Check circuit breaker status
from platformq_shared.resilience import get_circuit_breakers

breakers = get_circuit_breakers()
for name, breaker in breakers.items():
    print(f"{name}: {'OPEN' if breaker.is_open else 'CLOSED'}")
```

### Debug Logging

Enable debug logging for detailed information:

```python
import logging

# Enable debug logs
logging.basicConfig(level=logging.DEBUG)

# Or for specific module
logging.getLogger('platformq_shared.nextcloud_client').setLevel(logging.DEBUG)
```

### Metrics

Access metrics for monitoring:

```python
from platformq_shared.resilience import get_metrics_collector

metrics = get_metrics_collector()
stats = metrics.get_stats()

print(f"Total requests: {stats['total_requests']}")
print(f"Failed requests: {stats['failed_requests']}")
print(f"Average latency: {stats['avg_latency_ms']}ms")
```

## Best Practices

1. **Always use connection pooling** for production deployments
2. **Set appropriate rate limits** based on service capabilities
3. **Handle errors gracefully** - services may be temporarily unavailable
4. **Use background tasks** for non-critical operations
5. **Monitor metrics** to detect issues early
6. **Test webhooks** with tools like ngrok during development
7. **Secure webhook endpoints** with signature verification
8. **Use structured logging** for better observability

## Migration Guide

If upgrading from the basic implementations:

1. Update imports:
   ```python
   # Old
   from shared_lib.nextcloud_client import NextcloudClient
   
   # New
   from platformq_shared.nextcloud_client import NextcloudClient
   ```

2. Add resilience configuration:
   ```python
   client = NextcloudClient(
       ...,
       use_connection_pool=True,
       rate_limit=10.0
   )
   ```

3. Update error handling:
   ```python
   from platformq_shared.nextcloud_client import NextcloudError
   from platformq_shared.resilience import CircuitBreakerError
   
   try:
       client.create_user(...)
   except CircuitBreakerError:
       # Service temporarily unavailable
   except NextcloudError as e:
       # Handle specific Nextcloud errors
   ```

4. Migrate webhook endpoints to use new handlers

## Support

For issues or questions:

1. Check the [troubleshooting](#troubleshooting) section
2. Review test cases for usage examples
3. Enable debug logging for detailed error information
4. Contact the platform team for assistance 