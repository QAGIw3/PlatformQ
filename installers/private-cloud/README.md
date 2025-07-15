# platformQ Private Cloud Edition - Air-Gapped Installation Guide

This guide explains how to install the platformQ Private Cloud Edition into an existing Kubernetes cluster that **does not have internet access**.

## Prerequisites

1.  An existing, conformant Kubernetes cluster (v1.21+).
2.  A private container registry (e.g., Harbor, Artifactory) that is accessible from your Kubernetes nodes.
3.  The `kots` CLI installed on your local, internet-connected machine.
4.  `kubectl` configured to connect to your target cluster.

## Installation Steps

### Step 1: Pushing Images to a Private Registry (Online Machine)

1.  Unpack the provided `platformq-airgap.tar.gz` archive.
2.  This will reveal a directory containing all the application's Docker images.
3.  You must write a script to re-tag and push all of these images to your organization's private container registry.

### Step 2: Install the KOTS Admin Console (Offline Machine)

1.  On a machine that can access your Kubernetes cluster, run the following command to install the KOTS admin console in air-gapped mode:
    ```bash
    kubectl kots install platformq --airgap
    ```
    This command will provide you with a password and a URL for the admin console.

### Step 3: Upload License and Airgap Bundle

1.  Navigate to the provided URL in your browser and log in with the password.
2.  You will be prompted to upload your **license file**.
3.  Next, upload the `platformq-airgap.tar.gz` file.

### Step 4: Configure Your Application

1.  The admin console will now show the configuration screen.
2.  Crucially, you must provide the location of your **private container registry** so that KOTS knows where to pull the images from.
3.  Fill out the other required fields (admin passwords, hostname, etc.).
4.  Click "Deploy".

### Step 5: Monitor the Deployment

The admin console will show the status of the deployment. Once all pods are in a `Running` state, the installation is complete.

This completes the air-gapped installation process. 