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