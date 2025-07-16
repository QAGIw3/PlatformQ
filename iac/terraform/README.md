# Terraform for platformQ GKE Cluster

This directory contains the Terraform code to provision the core Kubernetes infrastructure for the platform on Google Cloud Platform (GCP).

## Prerequisites

1.  **A GCP Project**: You must have a GCP project created.
2.  **Terraform CLI**: You need the `terraform` CLI installed on your machine.
3.  **GCP Authentication**: You must be authenticated to GCP in your terminal. The easiest way is to run:
    ```bash
    gcloud auth application-default login
    ```

## Usage

1.  **Navigate to this directory**:
    ```bash
    cd iac/terraform
    ```

2.  **Create a `terraform.tfvars` file**:
    Create a file named `terraform.tfvars` in this directory and add the following content, replacing `<YOUR_GCP_PROJECT_ID>` with your actual project ID:
    ```tfvars
    gcp_project_id = "<YOUR_GCP_PROJECT_ID>"
    ```

3.  **Initialize Terraform**:
    ```bash
    terraform init
    ```

4.  **Plan the deployment**:
    This will show you what resources Terraform will create.
    ```bash
    terraform plan
    ```

5.  **Apply the configuration**:
    This will create the GKE cluster and associated networking.
    ```bash
    terraform apply
    ```

After applying, Terraform will output the necessary information to connect to your new cluster with `kubectl`.

## Vault Configuration

To configure the Vault server running in the local Docker Compose environment, navigate to the `iac/terraform/vault` directory and run the following commands:

```bash
cd iac/terraform/vault
terraform init
terraform apply
```

This will configure Vault with the necessary policies, roles, and secrets for the platform to function.

### Production Deployment

The Terraform configuration in this directory also includes a conceptual setup for a production-grade Vault cluster using the Raft storage backend and Google Cloud KMS for auto-unsealing. To deploy this configuration, you will need to:

1.  **Authenticate with GCP**: Ensure your local environment is authenticated with GCP with permissions to manage KMS resources.
2.  **Provide GCP Variables**: Create a `terraform.tfvars` file in this directory with your GCP project ID:
    ```tfvars
    gcp_project_id = "<YOUR_GCP_PROJECT_ID>"
    ```
3.  **Deploy the Infrastructure**: Run `terraform apply`. This will provision the KMS resources in your GCP project. The `vault_config` resource is conceptual and would need to be adapted to your chosen deployment method (e.g., Helm, Kubernetes Operator). 