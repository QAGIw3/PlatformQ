resource "vault_policy" "service_policy" {
  name = "service-policy"
  policy = <<EOT
path "secret/data/platformq/*" {
  capabilities = ["read"]
}
EOT
} 