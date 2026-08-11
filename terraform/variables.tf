# terraform/variables.tf
variable "project_id"       { default = "de-zoomcamp-499310" }
variable "region"           { default = "us-central1" }
variable "bucket_name"      { default = "filipburger-flight-data-lake" }
variable "credentials_path" { default = "keys/terraform-runner.json" }