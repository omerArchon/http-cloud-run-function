# This tells Terraform we are using the Google Cloud provider.
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

# This configures the provider with your project, region, and credentials.
# It reads the key file to authenticate.
provider "google" {
  project     = var.gcp_project_id
  region      = var.gcp_region
  credentials = file(var.service_account_key_path)
}