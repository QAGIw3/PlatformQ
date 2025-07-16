variable "gcp_project_id" {
  description = "The GCP project ID to use."
  type        = string
}

variable "gcp_region" {
  description = "The GCP region to use."
  type        = string
  default     = "us-central1"
} 