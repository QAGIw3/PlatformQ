
# Provisioning Service

This service is responsible for orchestrating the provisioning of all necessary resources for a new tenant. This includes creating Cassandra keyspaces, MinIO buckets, Pulsar namespaces, and OpenProject projects.

## Development

To install dependencies, run:

```bash
pip install -r requirements.txt
```

To run the service locally, use:

```bash
uvicorn app.main:app --reload
``` 