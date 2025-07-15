# Configuring Zulip for OIDC Single Sign-On

This guide explains how to configure your newly deployed Zulip instance to use our platform's `auth-service` as its identity provider for Single Sign-On (SSO).

## Prerequisites

1.  The entire platform stack must be deployed to Kubernetes via the `platformq-stack` Helm chart.
2.  The `bootstrap-oidc-clients` script must have been run to register the Zulip client with the `auth-service`.

## Step 1: Access Your Zulip Instance

1.  **Find your Zulip IP address**:
    ```bash
    kubectl get svc -n platformq platformq-zulip -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
    ```
2.  Navigate to this IP address in your browser.
3.  Log in with the admin credentials you defined in the `values.yaml` file (default is `admin@platformq.local` / `strongpassword`).

## Step 2: Configure OpenID Connect Authentication

1.  In the Zulip UI, click the gear icon in the top-right and go to **Organization settings**.
2.  In the left sidebar, click on **Authentication methods**.
3.  Check the box for **OpenID Connect** and click "Save changes".
4.  A new section "OpenID Connect" will appear. Click "Configure".
5.  Fill in the form with the following details:
    *   **OIDC provider's discovery URL**: `http://<KONG_IP>/auth/api/v1/.well-known/openid-configuration`
    *   **Client ID**: `zulip`
    *   **Client Secret**: `zulip-secret`
    *   **Attribute to use for username**: `email`

    *Note: You must replace `<KONG_IP>` with the external IP address of your Kong proxy service (`kubectl get svc -n platformq platformq-kong-proxy`).*

6.  Click **Save changes**.

## Step 3: Finalize and Test

1.  In the **Authentication methods** page, you can optionally limit which login methods are shown to users.
2.  Log out of your admin account.
3.  You should now see a "Login with OpenID Connect" button on the login page. This will redirect you through our `auth-service` for authentication.

This completes the SSO integration for Zulip. 