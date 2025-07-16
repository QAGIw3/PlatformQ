resource "google_kms_key_ring" "vault_unseal" {
  name     = "vault-unseal-keyring"
  location = var.gcp_region
}

resource "google_kms_crypto_key" "vault_unseal" {
  name            = "vault-unseal-key"
  key_ring        = google_kms_key_ring.vault_unseal.id
  rotation_period = "100000s" # Example rotation period

  lifecycle {
    prevent_destroy = true
  }
} 