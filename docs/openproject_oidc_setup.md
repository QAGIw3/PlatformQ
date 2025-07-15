# Configuring OpenProject for OIDC Single Sign-On

This guide explains how to configure your newly deployed OpenProject instance to use our platform's `auth-service` as its identity provider for Single Sign-On (SSO).

## Prerequisites

1.  The entire platform stack must be deployed to Kubernetes via the `platformq-stack` Helm chart.
2.  The `bootstrap-oidc-clients` script must have been run to register the OpenProject client with the `auth-service`.

## Step 1: Access Your OpenProject Instance

1.  **Find your OpenProject IP address**:
    ```bash
    kubectl get svc -n platformq platformq-openproject -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
    ```
2.  Navigate to this IP address in your browser.
3.  Log in with the admin credentials you defined in the `values.yaml` file (default is `admin` / `strongpassword`). You will be prompted to change this password on first login.

## Step 2: Configure OpenID Connect Authentication

1.  In the OpenProject UI, click the user icon in the top-right and go to **Administration**.
2.  In the left sidebar, navigate to **Authentication** -> **Authentication Settings**.
3.  Under **Single-Sign-On**, click **Add a new provider**.
4.  Select **OpenID Connect**.
5.  On the next screen, give it a name like `platformq`.
6.  Fill in the form with the following details:
    *   **Host**: `http://<KONG_IP>/auth`
    *   **Client ID**: `openproject`
    *   **Client Secret**: `openproject-secret`
    *   **Identifier**: This will usually be `sub` (for the user ID).
    *   **First name attribute**: `given_name`
    *   **Last name attribute**: `family_name`
    *   **Email attribute**: `email`

    *Note: You must replace `<KONG_IP>` with the external IP address of your Kong proxy service (`kubectl get svc -n platformq platformq-kong-proxy`). OpenProject will use the standard OIDC discovery endpoint (`.well-known/openid-configuration`) to find the rest of the URLs.*

7.  Click **Create**.

## Step 3: Finalize and Test

1.  You can now enable this provider and optionally disable password-based login.
2.  Log out, and you should see the option to log in with the "platformq" provider.

This completes the SSO integration for OpenProject. 