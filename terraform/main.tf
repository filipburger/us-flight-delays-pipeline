# terraform/main.tf
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  # Credentials only needs to be set if you do not have the GOOGLE_APPLICATION_CREDENTIALS set
  credentials = var.credentials_path
  project     = var.project_id
  region      = var.region
}

resource "google_storage_bucket" "data_lake" {
  name                        = var.bucket_name
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition { age = 90 }
    action    { type = "Delete" }
  }
}

# Raw — one dataset per source system, keeps provenance explicit
resource "google_bigquery_dataset" "raw_bts" {
  dataset_id  = "raw_bts"
  project     = var.project_id
  location    = var.region
  description = "Raw BTS TranStats extracts, loaded as-is from GCS"
}

resource "google_bigquery_dataset" "staging" {
  dataset_id  = "flights_staging"
  project     = var.project_id
  location    = var.region
  description = "dbt staging models — cleaned, typed, conformed"
}

resource "google_bigquery_dataset" "core" {
  dataset_id  = "flights_core"
  project     = var.project_id
  location    = var.region
  description = "dbt core models — star schema facts and dimensions"
}

resource "google_bigquery_dataset" "marts" {
  dataset_id  = "flights_marts"
  project     = var.project_id
  location    = var.region
  description = "dbt marts — dashboard-facing aggregates"
}