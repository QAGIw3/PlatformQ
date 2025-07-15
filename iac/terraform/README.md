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