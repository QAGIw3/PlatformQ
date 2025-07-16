# Kong API Gateway Configuration

This directory contains the declarative configuration for the Kong API Gateway. The configuration is split into multiple files to improve readability and maintainability.

## File Structure

- `kong.yaml`: The main entrypoint for the Kong configuration. It references the other files in this directory.
- `_services.yaml`: Defines all the upstream services that Kong will proxy to.
- `_routes.yaml`: Defines all the public-facing routes that will be exposed by the API Gateway.
- `_plugins.yaml`: Defines global plugins that will be applied to all services and routes.
- `_consumers.yaml`: Defines all the consumers of the API, such as end-users and internal services.

## Usage

The configuration is applied to the Kong Gateway using the `deck` CLI tool.

```bash
deck sync -s kong.yaml
``` 