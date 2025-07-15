# Load Testing with k6

This directory contains scripts for load testing the platformQ services.

## Prerequisites

1.  **A Running Platform Stack**: You must have the entire stack deployed (e.g., via `helm install` to your GKE cluster).
2.  **`k6` CLI**: You need the `k6` CLI installed. You can find installation instructions [here](https://k6.io/docs/getting-started/installation/).

## Running the Test

1.  **Find your Kong Gateway IP**:
    After deploying the stack to Kubernetes, find the external IP address of the Kong proxy service.
    ```bash
    kubectl get svc -n platformq platformq-kong-proxy -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
    ```

2.  **Update the Test Script**:
    Open the `k6-test.js` file and replace the `<KONG_IP_ADDRESS>` placeholder in the `BASE_URL` constant with the IP address you found in the previous step.

3.  **Run k6**:
    Execute the following command from this directory:
    ```bash
    k6 run k6-test.js
    ```

## Observing Autoscaling

While the test is running, you can watch the Horizontal Pod Autoscaler in action in a separate terminal:

```bash
# Watch the HPA resource and see its target vs. current metrics
kubectl get hpa -n platformq --watch

# Watch the auth-service pods being created or terminated
kubectl get pods -n platformq --watch
```

You can also view the CPU utilization and pod counts in your Grafana dashboard. 