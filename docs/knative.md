# Knative Installation

This document describes how to install Knative Serving into the Kubernetes cluster.

## Installation

To install Knative Serving, run the following command:

```bash
make install-knative
```

This will apply the necessary Kubernetes manifests to the cluster.

## Uninstallation

To uninstall Knative Serving, run the following command:

```bash
make uninstall-knative
```

This will remove the Knative Serving manifests from the cluster. 