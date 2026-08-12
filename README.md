# us-flight-delays-pipeline

End-to-end data pipeline for US flight delay data — Airflow ingestion, dbt transformations
into a star schema on BigQuery, served via a Data Studio dashboard.

## Overview

Ingests monthly [BTS Reporting Carrier On-Time Performance](https://www.transtats.bts.gov/)
extracts, lands them as partitioned Parquet in a GCS data lake, loads them into BigQuery,
and models them into a star schema with dbt for delay analysis by carrier, airport, and season.

**Stack:** Airflow · dbt · BigQuery · GCS · Terraform · Data Studio

## Prerequisites

- Docker Desktop
- Terraform
- `gcloud` CLI, authenticated
- A GCP project with billing enabled

## Setup

### 1. Clone

```bash
git clone git@github.com:filipburger/us-flight-delays-pipeline.git
cd us-flight-delays-pipeline
```

### 2. Provision GCP infrastructure (Terraform)

Create a service account for Terraform with these roles:

roles/storage.admin
roles/bigquery.admin

Generate its key:

```bash
export PROJECT_ID=<your-project-id>

gcloud iam service-accounts create terraform-runner \
  --project=$PROJECT_ID \
  --display-name="Terraform infrastructure runner"

gcloud iam service-accounts keys create ~/keys/terraform-runner.json \
  --iam-account=terraform-runner@${PROJECT_ID}.iam.gserviceaccount.com
```

Then apply:

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

This creates the GCS data lake bucket and the `raw_bts`, `flights_staging`,
`flights_core`, and `flights_marts` BigQuery datasets.

### 3. Create the Airflow service account

Scoped to least privilege — it writes objects and loads tables, but cannot
create or delete buckets or datasets.

```bash
export PROJECT_ID=<your-project-id>
export SA="airflow-runner@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud iam service-accounts create airflow-runner \
  --project=$PROJECT_ID \
  --display-name="Airflow pipeline runner"

for ROLE in roles/storage.objectAdmin roles/bigquery.dataEditor roles/bigquery.jobUser; do
  gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SA}" --role="$ROLE"
done

gcloud iam service-accounts keys create airflow/config/gcp-credentials.json \
  --iam-account=$SA
```

> `airflow/config/gcp-credentials.json` is gitignored and must be generated locally.

### 4. Start Airflow

```bash
cd airflow
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
mkdir -p ./dags ./logs ./plugins ./config

# Local environment file (gitignored)
echo "AIRFLOW_UID=$(id -u)" > .env
echo "FERNET_KEY=$(python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')" >> .env

docker compose up airflow-init
docker compose up -d
```

First boot runs database migrations and takes a minute or two.

```bash
docker compose ps
```

The UI is at http://localhost:8080 (default login `airflow` / `airflow`).

> Generating the Fernet key requires the `cryptography` package
> (`pip install cryptography` or `uv add cryptography`). It encrypts stored
> connection credentials and is not needed at runtime by the DAGs themselves.

### 5. Configure the Airflow → GCP connection

In the Airflow UI: **Admin → Connections → +**

| Field | Value |
|---|---|
| Connection Id | `google_cloud_default` |
| Connection Type | Google Cloud |
| Keyfile Path | `/opt/airflow/config/gcp-credentials.json` |
| Project Id | your project ID |

## Project structure
airflow/ Local Airflow stack (Docker Compose) and DAGs
terraform/ GCS bucket and BigQuery dataset definitions
dbt/ Staging, core, and mart models
notebooks/ Exploratory analysis of the source data
docs/ Field dictionary and data quality notes

```
.
├── airflow/          # Local Airflow stack (Docker Compose) and DAGs
│   ├── dags/         # Pipeline definitions
│   └── config/       # GCP credentials (gitignored)
├── terraform/        # GCS bucket and BigQuery dataset definitions
├── dbt/              # Staging, core, and mart models
│   └── models/
│       ├── staging/
│       ├── core/
│       └── marts/
├── notebooks/        # Exploratory analysis of the source data
└── docs/             # Field dictionary and data quality notes
```