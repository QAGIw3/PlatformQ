-- Create the table for tenants, the root of our data model
CREATE TABLE IF NOT EXISTS tenants (
    id UUID,
    name TEXT,
    created_at TIMESTAMP,
    PRIMARY KEY (id)
);

-- Create the table for users, partitioned by tenant
CREATE TABLE IF NOT EXISTS users (
    tenant_id UUID,
    id UUID,
    email TEXT,
    full_name TEXT,
    status TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    PRIMARY KEY ((tenant_id), id)
);
CREATE INDEX IF NOT EXISTS ON users (email);

-- User credentials must be unique globally, but we store tenant_id for context
CREATE TABLE IF NOT EXISTS user_credentials (
    provider TEXT,
    provider_user_id TEXT,
    tenant_id UUID,
    user_id UUID,
    PRIMARY KEY (provider, provider_user_id)
);

-- Roles are now scoped to a user within a tenant
CREATE TABLE IF NOT EXISTS user_roles (
    tenant_id UUID,
    user_id UUID,
    role TEXT,
    PRIMARY KEY ((tenant_id), user_id, role)
);

-- Subscriptions are tied to a user within a tenant
CREATE TABLE IF NOT EXISTS subscriptions (
    tenant_id UUID,
    id UUID,
    user_id UUID,
    tier TEXT,
    status TEXT,
    billing_cycle_start TIMESTAMP,
    billing_cycle_end TIMESTAMP,
    PRIMARY KEY ((tenant_id), id)
);
CREATE INDEX IF NOT EXISTS ON subscriptions (user_id);

-- Quotas are tied to a subscription within a tenant
CREATE TABLE IF NOT EXISTS usage_quotas (
    tenant_id UUID,
    subscription_id UUID,
    feature TEXT,
    quota BIGINT,
    used BIGINT,
    PRIMARY KEY ((tenant_id), subscription_id, feature)
);

-- Audit logs are partitioned by tenant
CREATE TABLE IF NOT EXISTS audit_log (
    tenant_id UUID,
    id UUID,
    user_id UUID,
    event_type TEXT,
    details TEXT,
    event_timestamp TIMESTAMP,
    PRIMARY KEY ((tenant_id), id)
);

-- API Keys are now issued to a user within a tenant
CREATE TABLE IF NOT EXISTS api_keys (
    tenant_id UUID,
    key_prefix TEXT,
    hashed_key TEXT,
    user_id UUID,
    created_at TIMESTAMP,
    expires_at TIMESTAMP,
    last_used_at TIMESTAMP,
    is_active BOOLEAN,
    PRIMARY KEY ((tenant_id), key_prefix)
);
CREATE INDEX IF NOT EXISTS ON api_keys (user_id);

-- Refresh tokens must also be tenant-scoped
CREATE TABLE IF NOT EXISTS refresh_tokens (
    tenant_id UUID,
    token_hash TEXT,
    user_id UUID,
    created_at TIMESTAMP,
    expires_at TIMESTAMP,
    is_revoked BOOLEAN,
    PRIMARY KEY ((tenant_id), token_hash)
);
CREATE INDEX IF NOT EXISTS ON refresh_tokens (user_id);

-- Invitations are issued within a specific tenant context
CREATE TABLE IF NOT EXISTS invitations (
    tenant_id UUID,
    token_hash TEXT,
    email_invited TEXT,
    invited_by_user_id UUID,
    created_at TIMESTAMP,
    expires_at TIMESTAMP,
    is_accepted BOOLEAN,
    PRIMARY KEY ((tenant_id), token_hash)
);
CREATE INDEX IF NOT EXISTS ON invitations (email_invited);

-- OIDC clients are now associated with a tenant
CREATE TABLE IF NOT EXISTS oidc_clients (
    tenant_id UUID,
    client_id TEXT,
    client_secret_hash TEXT,
    client_name TEXT,
    redirect_uris LIST<TEXT>,
    allowed_scopes LIST<TEXT>,
    PRIMARY KEY ((tenant_id), client_id)
);

-- Auth codes are issued within a tenant
CREATE TABLE IF NOT EXISTS authorization_codes (
    tenant_id UUID,
    code_hash TEXT,
    client_id TEXT,
    user_id UUID,
    redirect_uri TEXT,
    scope TEXT,
    expires_at TIMESTAMP,
    is_used BOOLEAN,
    PRIMARY KEY ((tenant_id), code_hash)
);

-- Passwordless tokens are also tenant-scoped
CREATE TABLE IF NOT EXISTS passwordless_tokens (
    tenant_id UUID,
    email TEXT,
    token_hash TEXT,
    expires_at TIMESTAMP,
    PRIMARY KEY ((tenant_id), email, token_hash)
);

-- A tenant-partitioned, time-series table for the unified activity stream
CREATE TABLE IF NOT EXISTS activity_stream (
    tenant_id UUID,
    event_timestamp TIMESTAMP,
    event_id UUID,
    user_id UUID,
    event_source TEXT,
    event_type TEXT,
    primary_object_id TEXT,
    primary_object_type TEXT,
    details MAP<TEXT, TEXT>,
    PRIMARY KEY ((tenant_id), event_timestamp, event_id)
) WITH CLUSTERING ORDER BY (event_timestamp DESC); 

-- A table to store metadata for custom collaborative proposals
CREATE TABLE IF NOT EXISTS proposals (
    tenant_id UUID,
    proposal_id UUID,
    status TEXT, -- e.g., 'draft', 'in_review', 'approved', 'rejected'
    customer_name TEXT,
    document_path TEXT, -- The path to the actual file in Nextcloud
    created_by_user_id UUID,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    PRIMARY KEY ((tenant_id), proposal_id)
); 

-- A table to store single-use nonces for SIWE
CREATE TABLE IF NOT EXISTS siwe_nonces (
    nonce TEXT,
    created_at TIMESTAMP,
    PRIMARY KEY (nonce)
); 

-- A table to link together resources from different applications for a single project
CREATE TABLE IF NOT EXISTS projects (
    tenant_id UUID,
    project_id UUID,
    project_name TEXT,
    status TEXT, -- e.g., 'active', 'archived'
    openproject_id TEXT,
    nextcloud_folder_path TEXT,
    zulip_stream_name TEXT,
    created_by_user_id UUID,
    created_at TIMESTAMP,
    PRIMARY KEY ((tenant_id), project_id)
); 