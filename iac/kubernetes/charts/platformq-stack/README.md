# platformQ Stack Helm Chart

This Helm chart deploys the entire platformQ stack to a Kubernetes cluster. It is an "umbrella" chart that manages all other component charts as dependencies.

## Prerequisites

1.  A running Kubernetes cluster (e.g., the GKE cluster from our Terraform scripts).
2.  `kubectl` configured to connect to your cluster.
3.  `helm` v3 installed.

## Deployment Steps

1.  **Add Required Helm Repositories**:
    Some of our dependencies come from third-party chart repositories. You must add them to your local Helm client:
    ```bash
    helm repo add hashicorp https://helm.releases.hashicorp.com
    helm repo add kong https://charts.konghq.com
    helm repo add apache https://pulsar.apache.org/charts
    helm repo add bitnami https://charts.bitnami.com/bitnami
    helm repo update
    ```

2.  **Navigate to the Chart Directory**:
    ```bash
    cd iac/kubernetes/charts/platformq-stack
    ```

3.  **Fetch Chart Dependencies**:
    This command downloads the third-party charts listed in `Chart.yaml` into a `charts/` subdirectory.
    ```bash
    helm dependency update
    ```

4.  **Install the Stack**:
    This command will deploy the entire platform into a new `platformq` namespace.
    ```bash
    helm install platformq . --namespace platformq --create-namespace
    ```

After a few minutes, all services should be up and running. You can check the status with `kubectl get pods -n platformq`. 