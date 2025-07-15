variable "gcp_project_id" {
  description = "The GCP project ID to deploy to."
  type        = string
}

variable "regions" {
  description = "A list of GCP regions to deploy clusters into."
  type        = list(string)
  default     = ["us-central1", "europe-west1"]
}

variable "cluster_name_prefix" {
  description = "A prefix for the GKE cluster names."
  type        = string
  default     = "platformq"
} 