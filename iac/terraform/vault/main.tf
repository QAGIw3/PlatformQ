terraform {
  required_providers {
    vault = {
      source  = "hashicorp/vault"
      version = "3.25.0"
    }
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "vault" {
  address = "http://127.0.0.1:8200"
  token   = "dev-root-token"
}

provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
} 