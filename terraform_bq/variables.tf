variable "gcp_project_id" {
  type        = string
  description = "The Google Cloud project ID to deploy resources into."
  default     = "omer-playground-440310"
}

variable "gcp_region" {
  type        = string
  description = "The GCP region for resources like Cloud Functions (if used later)."
  default     = "us-central1"
}

variable "dataset_id" {
  type        = string
  description = "The ID for the BigQuery dataset."
  default     = "events_analytics_api"
}

variable "dataset_location" {
  type        = string
  description = "The multi-region location for the BigQuery dataset (e.g., US, EU)."
  default     = "US"
}

variable "service_account_key_path" {
  type        = string
  description = "Path to the GCP service account JSON key file."
  default     = "./gcp-credentials.json"
}