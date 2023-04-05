locals {
  data_lake_bucket = "airbnb-gcs-bucket"
}

variable "service_account" {
  description = "Google Service Account IAM & Admin > Service Accounts"
  default     = "GOOGLE_APPLICATION_CREDENTIALS.json" # CHANGE THIS TO YOUR CREDENTIALS
  type        = string
}

variable "project" {
  description = "Name of the Project in Google Cloud"
  default     = "dtc-de-2023"
  type        = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/compute/docs/regions-zones"
  default     = "asia-southeast1"
  type        = string
}

variable "zone" {
  description = "A zone is a deployment area within a region. Choose as per your location: https://cloud.google.com/compute/docs/regions-zones"
  default     = "asia-southeast1-a"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "sample_airbnb"
}
