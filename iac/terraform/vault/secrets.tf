resource "vault_kv_secret_v2" "minio" {
  mount = "secret"
  name  = "platformq/minio"
  data_json = jsonencode({
    access_key = "minioadmin"
    secret_key = "minioadmin"
  })
}

resource "vault_kv_secret_v2" "postgres" {
  mount = "secret"
  name  = "platformq/postgres"
  data_json = jsonencode({
    password = "password"
  })
}

resource "vault_kv_secret_v2" "cassandra" {
  mount = "secret"
  name  = "platformq/cassandra"
  data_json = jsonencode({
    password = "cassandra"
    config   = "{\"hosts\": \"cassandra\", \"port\": \"9042\"}"
  })
}

resource "vault_kv_secret_v2" "jwt" {
  mount = "secret"
  name  = "platformq/jwt"
  data_json = jsonencode({
    s2s_secret_key = "a-very-secret-key"
  })
}

resource "vault_kv_secret_v2" "ethereum" {
  mount = "secret"
  name  = "platformq/ethereum"
  data_json = jsonencode({
    private_key = "0x0000000000000000000000000000000000000000000000000000000000000000"
  })
}

resource "vault_kv_secret_v2" "polygon" {
  mount = "secret"
  name  = "platformq/polygon"
  data_json = jsonencode({
    private_key = "0x0000000000000000000000000000000000000000000000000000000000000000"
  })
}

resource "vault_kv_secret_v2" "credentials" {
  mount = "secret"
  name  = "platformq/credentials"
  data_json = jsonencode({
    encryption_key = "a-very-secret-key"
  })
}

resource "vault_kv_secret_v2" "bridge" {
  mount = "secret"
  name  = "platformq/bridge"
  data_json = jsonencode({
    private_key = "0x0000000000000000000000000000000000000000000000000000000000000000"
  })
}

resource "vault_kv_secret_v2" "reputation" {
  mount = "secret"
  name  = "platformq/reputation"
  data_json = jsonencode({
    operator_private_key = "0x0000000000000000000000000000000000000000000000000000000000000000"
  })
}

resource "vault_kv_secret_v2" "zulip" {
  mount = "secret"
  name  = "platformq/zulip"
  data_json = jsonencode({
    api_key = "a-zulip-api-key"
  })
}

resource "vault_kv_secret_v2" "openproject" {
  mount = "secret"
  name  = "platformq/openproject"
  data_json = jsonencode({
    api_key = "an-openproject-api-key"
  })
}

resource "vault_kv_secret_v2" "auth_service" {
  mount = "secret"
  name = "platformq/auth-service"
  data_json = jsonencode({
    jwt_secret_key = "a-super-secret-jwt-key"
  })
}

resource "vault_kv_secret_v2" "digital_asset_service" {
  mount = "secret"
  name = "platformq/digital-asset-service"
  data_json = jsonencode({
    database_url = "postgresql://user:password@postgres:5432/das"
    minio_access_key = "minioadmin"
    minio_secret_key = "minioadmin"
  })
}

resource "vault_kv_secret_v2" "proposals_service" {
  mount = "secret"
  name = "platformq/proposals-service"
  data_json = jsonencode({
    nextcloud_url = "http://platformq-nextcloud"
    nextcloud_user = "nc-admin"
    nextcloud_password = "strongpassword"
  })
}

resource "vault_kv_secret_v2" "storage_proxy_service" {
  mount = "secret"
  name = "platformq/storage-proxy-service"
  data_json = jsonencode({
    auth_service_url = "http://auth-service:8000"
    minio_endpoint = "minio:9000"
    minio_access_key = "minioadmin"
    minio_secret_key = "minioadmin"
  })
}

resource "vault_kv_secret_v2" "functions_service" {
  mount = "secret"
  name = "platformq/functions-service"
  data_json = jsonencode({
    connector_service_grpc_target = "connector-service:50051"
  })
}

resource "vault_kv_secret_v2" "projects_service" {
  mount = "secret"
  name = "platformq/projects-service"
  data_json = jsonencode({
    nextcloud_url = "http://platformq-nextcloud"
    nextcloud_user = "nc-admin"
    nextcloud_password = "strongpassword"
    openproject_url = "http://platformq-openproject"
    openproject_api_key = "an-openproject-api-key"
    zulip_site = "http://platformq-zulip"
    zulip_email = "bot@platformq.io"
    zulip_api_key = "a-zulip-api-key"
    platform_url = "http://localhost:8000"
  })
}

resource "vault_kv_secret_v2" "neuromorphic_service" {
  mount = "secret"
  name = "platformq/neuromorphic-service"
  data_json = jsonencode({
    quantum_optimization_url = "http://quantum-optimization-service:8001"
    ignite_host = "ignite"
    ignite_port = 10800
  })
}

resource "vault_kv_secret_v2" "graph_intelligence_service" {
  mount = "secret"
  name = "platformq/graph-intelligence-service"
  data_json = jsonencode({
    gremlin_server_url = "ws://janusgraph:8182/gremlin"
  })
} 