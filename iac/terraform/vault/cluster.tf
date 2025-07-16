# This is a conceptual resource, as the actual deployment of a Vault cluster
# would be handled by a more complex set of resources, likely using a
# combination of Kubernetes operators, Helm charts, or virtual machine images.
# This resource is included to illustrate how the configuration would look.

resource "vault_config" "production_cluster" {
  storage "raft" {
    path    = "/vault/data"
    node_id = "node1" # This would be unique for each node in the cluster
  }

  seal "gcpckms" {
    project     = var.gcp_project_id
    region      = var.gcp_region
    key_ring    = google_kms_key_ring.vault_unseal.name
    crypto_key  = google_kms_crypto_key.vault_unseal.name
  }

  listener "tcp" {
    address     = "0.0.0.0:8200"
    tls_disable = "true" # In a real production setup, this would be false
  }
} 