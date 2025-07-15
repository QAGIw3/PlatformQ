# `platformqctl`: The PlatformQ Control Tool

`platformqctl` is a command-line interface for managing and scaffolding services on the platformQ ecosystem. It is the heart of our "Golden Path" for developers.

## Installation

To install the CLI in an editable mode for development:
```bash
pip install -e .
```

## Commands

### `platformqctl init`

Initializes the current directory with a `.platformqctl.yaml` configuration file. This file allows you to customize the tool for your specific enterprise environment (e.g., custom template repositories, different container registries).

### `platformqctl create service <service-name>`

Scaffolds a new microservice in the `services/` directory using the configured template repository.

**Options**:
- `--deployable`: If this flag is set, the tool will automatically attempt to update the platform's umbrella Helm chart and CI/CD workflow to make the new service instantly deployable. 