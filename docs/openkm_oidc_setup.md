# Configuring OpenKM for OIDC Single Sign-On

This guide explains how to configure your newly deployed OpenKM instance to use our platform's `auth-service` as its identity provider for Single Sign-On (SSO).

**Note:** OpenKM's OIDC integration is a feature of its professional edition. This guide outlines the theoretical steps. For the Community Edition, a custom integration or alternative authentication method might be required.

## Prerequisites

1.  The platform stack must be running.
2.  The `bootstrap-oidc-clients` script must have been run to register OpenKM.

## Step 1: Access Your OpenKM Instance

1.  Navigate to `http://localhost:8081` in your browser.
2.  Log in with the default admin credentials (`okmAdmin` / `admin`).

## Step 2: Configure OpenID Connect

1.  Navigate to the **Administration** section.
2.  Find the **Authentication** or **SSO** settings.
3.  Add a new OpenID Connect provider with the following details:
    *   **Discovery URL**: `http://<KONG_IP>/auth/api/v1/.well-known/openid-configuration`
    *   **Client ID**: `openkm`
    *   **Client Secret**: `openkm-secret`
    *   **Username mapping**: `email`

    *Note: You must replace `<KONG_IP>` with the external IP address of your Kong proxy service.*

4.  Save the configuration and enable the OIDC login method.

This completes the conceptual SSO integration for OpenKM. 