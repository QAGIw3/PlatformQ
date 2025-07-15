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

# --- Networking ---
# A single shared Virtual Private Cloud (VPC) for all our clusters,
# enabling them to communicate privately and securely.
resource "google_compute_network" "vpc_network" {
  name                    = "${var.cluster_name_prefix}-vpc"
  auto_create_subnetworks = false
}

# Use for_each to create a subnet in each specified region
resource "google_compute_subnetwork" "vpc_subnets" {
  for_each      = toset(var.regions)
  name          = "${var.cluster_name_prefix}-subnet-${each.key}"
  ip_cidr_range = "10.${index(var.regions, each.key)}.0.0/24" # Assign a unique CIDR range per region
  region        = each.key
  network       = google_compute_network.vpc_network.id
}

# --- Kubernetes Engine ---
# This resource uses a for_each loop to create identical GKE clusters
# in each of the specified regions. This is the core of our multi-region setup.
resource "google_container_cluster" "primary_clusters" {
  for_each   = toset(var.regions)
  name       = "${var.cluster_name_prefix}-${each.key}"
  location   = each.key
  network    = google_compute_network.vpc_network.name
  subnetwork = google_compute_subnetwork.vpc_subnets[each.key].name

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

# Use for_each to create a node pool for each cluster
resource "google_container_node_pool" "primary_nodes" {
  for_each   = toset(var.regions)
  name       = "${google_container_cluster.primary_clusters[each.key].name}-default-pool"
  cluster    = google_container_cluster.primary_clusters[each.key].name
  location   = each.key
  
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

# Use for_each to create a WASM node pool for each cluster
resource "google_container_node_pool" "wasm_nodes" {
  for_each   = toset(var.regions)
  name       = "${google_container_cluster.primary_clusters[each.key].name}-wasm-pool"
  cluster    = google_container_cluster.primary_clusters[each.key].name
  location   = each.key
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

# --- Global Traffic Management ---
# This Global External HTTPS Load Balancer provides a single, anycast IP
# address to route traffic to the nearest, healthiest GKE cluster.
# It uses container-native load balancing via Network Endpoint Groups (NEGs).
resource "google_compute_global_address" "default" {
  name = "${var.cluster_name_prefix}-global-ip"
}

resource "google_compute_backend_service" "default" {
  name        = "${var.cluster_name_prefix}-backend-service"
  protocol    = "HTTP"
  port_name   = "http"
  timeout_sec = 10
  
  # Add the Istio Ingress from each cluster as a backend
  # This uses Network Endpoint Groups (NEGs) for container-native load balancing
  # Note: The NEG resources themselves would be created and managed by the GKE Ingress controller
}

resource "google_compute_url_map" "default" {
  name            = "${var.cluster_name_prefix}-url-map"
  default_service = google_compute_backend_service.default.id
}

resource "google_compute_target_https_proxy" "default" {
  name    = "${var.cluster_name_prefix}-https-proxy"
  url_map = google_compute_url_map.default.id
  # In production, you would add a managed SSL certificate here
}

resource "google_compute_global_forwarding_rule" "default" {
  name       = "${var.cluster_name_prefix}-forwarding-rule"
  target     = google_compute_target_https_proxy.default.id
  ip_address = google_compute_global_address.default.address
  port_range = "443"
} 