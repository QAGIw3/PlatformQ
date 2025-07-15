# Configuring Nextcloud for OIDC Single Sign-On

This guide explains how to configure your newly deployed Nextcloud instance to use our platform's `auth-service` as its identity provider for Single Sign-On (SSO).

## Prerequisites

1.  The entire platform stack must be deployed to Kubernetes via the `platformq-stack` Helm chart.
2.  The `bootstrap-oidc-clients` script must have been run to register the Nextcloud client with the `auth-service`.

## Step 1: Access Your Nextcloud Instance

1.  **Find your Nextcloud IP address**:
    ```bash
    kubectl get svc -n platformq platformq-nextcloud -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
    ```
2.  Navigate to this IP address in your browser.
3.  Log in with the admin credentials you defined in the `values.yaml` file (default is `nc-admin` / `strongpassword`).

## Step 2: Install and Configure the "Social Login" App

1.  In the Nextcloud UI, click the user icon in the top-right and go to **Apps**.
2.  Find the **Social Login** app and click **Download and enable**.
3.  After it's enabled, navigate to **Settings** (under the user icon again) and find **Social Login** in the left-hand administration sidebar.

## Step 3: Configure the Custom OpenID Connect Provider

1.  Click **Add provider** and select **Custom OpenID Connect**.
2.  Fill in the form with the following details:
    *   **Internal name**: `platformq` (must be lowercase)
    *   **Title**: `Login with platformQ`
    *   **Authorize URL**: `http://<KONG_IP>/auth/api/v1/authorize`
    *   **Token URL**: `http://<KONG_IP>/auth/api/v1/token`
    *   **Userinfo URL**: `http://<KONG_IP>/auth/api/v1/userinfo`
    *   **Client ID**: `nextcloud`
    *   **Client Secret**: `nextcloud-secret`
    *   **Scope**: `openid profile email`
    *   **Groups claim (optional)**: `roles`

    *Note: You must replace `<KONG_IP>` with the external IP address of your Kong proxy service (`kubectl get svc -n platformq platformq-kong-proxy`).*

3.  Click **Save**.

## Step 4: Finalize and Test

1.  Disable all other login methods in the Social Login settings page if desired (e.g., "Disable username/password login for users when they have a social login connected").
2.  Log out of your admin account.
3.  You should now see a "Login with platformQ" button on the login page. Clicking this should redirect you through our `auth-service`'s authorization flow.

This completes the SSO integration. 