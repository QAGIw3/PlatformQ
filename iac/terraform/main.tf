terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.40.0"
    }
  }
}

provider "google" {
  project = var.gcp_project_id
  region  = var.region
}

# A VPC network for the GKE cluster
resource "google_compute_network" "vpc_network" {
  name                    = "${var.cluster_name}-vpc"
  auto_create_subnetworks = false
}

# A subnet for the GKE cluster
resource "google_compute_subnetwork" "vpc_subnet" {
  name          = "${var.cluster_name}-subnet"
  ip_cidr_range = "10.10.0.0/24"
  network       = google_compute_network.vpc_network.id
}

# The GKE cluster
resource "google_container_cluster" "primary" {
  name     = var.cluster_name
  location = var.region
  network    = google_compute_network.vpc_network.name
  subnetwork = google_compute_subnetwork.vpc_subnet.name

  # Enable Istio (Anthos Service Mesh) on the cluster
  addons_config {
    http_load_balancing {
      disabled = false
    }
    gcp_filestore_csi_driver_config {
      enabled = true
    }
    gke_backup_agent_config {
      enabled = true
    }
    asm_istio {
      # This enables the managed Istio control plane
      enabled = true
    }
  }

  initial_node_count = 1
  remove_default_node_pool = true

  # Enable Vertical Pod Autoscaling
  vertical_pod_autoscaling {
    enabled = true
  }

  # Cluster-level autoscaling configuration
  cluster_autoscaling {
    enabled = true
    autoscaling_profile = "OPTIMIZE_UTILIZATION"
    
    # This enables Node Auto-Provisioning
    auto_provisioning_defaults {
      oauth_scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
      ]
    }
  }

  # We can add more specific configurations for node pools, etc. later.
}

# A container registry to store our service images
resource "google_artifact_registry_repository" "docker_repo" {
  location      = var.region
  repository_id = "platformq-services"
  format        = "DOCKER"
  description   = "Docker repository for platformQ services"
}

resource "google_container_node_pool" "primary_nodes" {
  name       = "${google_container_cluster.primary.name}-initial-pool"
  cluster    = google_container_cluster.primary.name
  location   = var.region
  
  # Enable autoscaling for this specific node pool
  autoscaling {
    min_node_count = 1
    max_node_count = 5
  }
  
  node_count = 1 # Initial count
  
  node_config {
    preemptible  = true
    machine_type = "e2-medium"
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform",
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/monitoring",
      "https://www.googleapis.com/auth/servicecontrol",
      "https://www.googleapis.com/auth/service.management.readonly",
      "https://www.googleapis.com/auth/trace.append",
    ]
  }
} 

resource "google_container_node_pool" "wasm_nodes" {
  name       = "${google_container_cluster.primary.name}-wasm-pool"
  cluster    = google_container_cluster.primary.name
  location   = var.region
  node_count = 1

  node_config {
    machine_type = "e2-medium"
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform",
    ]
    # Add a taint to ensure only WASM workloads run here
    taint {
      key    = "workload.gke.io/type"
      value  = "wasm"
      effect = "NO_SCHEDULE"
    }
    # Startup script to install WasmEdge
    metadata = {
      gce-container-declaration = <<-EOT
        spec:
          containers:
          - name: wasmedge-installer
            image: wasmedge/wasmedge:0.13.5
            command: ["/bin/sh", "-c", "curl -sSf https://raw.githubusercontent.com/WasmEdge/WasmEdge/master/utils/install.sh | bash -s -- -p /usr/local && apt-get update && apt-get install -y crun"]
            volumeMounts:
            - name: usr-local
              mountPath: /usr/local
      EOT
      google-logging-enabled = "true"
    }
  }
} 