from datetime import datetime, timedelta
import io
import logging
import zipfile

import pandas as pd
import requests
import urllib3
from airflow.sdk import dag, task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from pendulum import DateTime

from include.bts_ontime_reporting_schema import BTS_ONTIME_REPORTING_DTYPES

# try:
#     resp = requests.get(url, timeout=300)
#     resp.raise_for_status()
# except requests.exceptions.SSLError:
#     log.warning("TLS verification failed for BTS, retrying without verification")
#     resp = requests.get(url, verify=False, timeout=300)
#     resp.raise_for_status()

# BTS has a known-broken cert chain; we disale verification and silence warning for know
# proper solution is to verify if it is fixed or not
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger(__name__)


# to check whats available manually go on https://transtats.bts.gov/PREZIP/ and look up dataset name
BASE_URL = (
    "https://transtats.bts.gov/PREZIP/"
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
)

# Verified stable across 2015-2026 110 columns - 1 phantom column caused by trailng comma in source file
EXPECTED_COLUMN_COUNT = 109
SCHEMA_COLUMNS_COUNT = len(BTS_ONTIME_REPORTING_DTYPES)

if SCHEMA_COLUMNS_COUNT != EXPECTED_COLUMN_COUNT:
    raise ValueError(
        f"Expected {EXPECTED_COLUMN_COUNT} columns, schema defines {SCHEMA_COLUMNS_COUNT}"
    )

GCS_BUCKET = "filipburger-flight-data-lake"
GCS_PREFIX = "raw/bts_ontime_reporting"


def bts_request(method: str, url: str, **kwargs) -> requests.Response:
    """Request BTS with TLS verification, falling back to unverified on the known cert issue"""
    try:
        return requests.request(method, url, **kwargs)
    except requests.exceptions.SSLError:
        log.warning("TLS verification failed for %s, retrying unverified", url)
        # in case user pass verify true, we need override
        kwargs["verify"] = False
        return requests.request(method, url, **kwargs)


def build_url(year: int, month: int) -> str:
    return BASE_URL.format(year=year, month=month)


def validate_and_clean_columns(df: pd.DataFrame, year, month):
    """Non judgmental clean up of phantom column caused by trailing comma in source file
    + validate columns agains expected schema"""
    df = df.drop(columns=[c for c in df.columns if c.startswith("Unnamed:")])
    missing = set(BTS_ONTIME_REPORTING_DTYPES) - set(df.columns)
    unexpected_columns = set(df.columns) - set(BTS_ONTIME_REPORTING_DTYPES)
    if missing or unexpected_columns:
        raise ValueError(
            f"Schema columns missalignment in {year}-{month} file:"
            f"Missing columns: {sorted(missing)}"
            f"Unexpected columns: {sorted(unexpected_columns)}"
        )
    return df


def download_and_extract_csv(year: int, month: int) -> pd.DataFrame:
    """Download the monthly zip and return its CSV as DataFrame"""
    url = build_url(year, month)
    log.info("Downloading %s", url)

    resp = bts_request("GET", url, timeout=600)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        csv_files = [n for n in z.namelist() if n.lower().endswith(".csv")]
        # we expect exactly one file, we need ingestion fail loudely otherwise
        if len(csv_files) != 1:
            raise ValueError(
                f"Expected exatly one CSV file in {url}, found {len(csv_files)}: {csv_files}"
            )

        csv_name = csv_files[0]
        log.info("Extracting %s", csv_name)
        with z.open(csv_name) as f:
            df = pd.read_csv(f, dtype=BTS_ONTIME_REPORTING_DTYPES)  # type: ignore[arg-type]
            df = validate_and_clean_columns(df, year, month)
            return df


def annotate_columns(df: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    """Add meta data for easier debugging"""
    df["_source_file"] = build_url(year, month).rsplit("/", 1)[-1]
    df["_ingested_at"] = pd.Timestamp.now(tz="UTC")
    df["_logical_date"] = f"{year}-{month:02d}"
    return df


@dag(
    dag_id="bts_ontime_reporting",
    schedule="@monthly",
    start_date=datetime(2026, 5, 1),
    end_date=datetime(2026, 8, 1),
    catchup=True,
    max_active_runs=1,
    tags=["bts_ontime_reporting"],
    default_args={"retries": 1},
)
def bts_ontime_reporting():
    @task.short_circuit
    def check_availability(logical_date: DateTime = None) -> bool:  # type: ignore[arg-type]
        """Return False (skipping everything downstream) if BTS hasn't published this month yet."""
        url = BASE_URL.format(year=logical_date.year, month=logical_date.month)

        resp = bts_request("HEAD", url, timeout=60)
        available = resp.status_code == 200

        log.info(
            "%s-%02d → HTTP %s (%s)",
            logical_date.year,
            logical_date.month,
            resp.status_code,
            "available" if available else "not published yet",
        )
        return available

    @task
    def ingest_month(logical_date: DateTime = None) -> str:  # type: ignore[arg-type]
        """Download the month's zip, convert to parquet in memory, upload to GCS.

        Kept as a single task with an in-memory buffer rather than writing to
        local disk: under CeleryExecutor separate tasks may run on different
        workers, so a file written by one isn't guaranteed visible to the next.
        """
        year, month = logical_date.year, logical_date.month

        # Hive-style partitioning (key=value). BigQuery parses these directory
        # names into `year`/`month` columns and prunes partitions on them, so a
        # filtered query reads one file instead of the whole prefix. Zero-padded
        # so listings sort chronologically rather than 1, 10, 11, 2.
        # added source_ prefix to distuinguish from flight's year and month
        # available in data set, should ve 100%, but comparing these two
        # will help us test data quality
        object_name = f"{GCS_PREFIX}/source_year={year}/source_month={month:02d}/data.parquet"
        hook = GCSHook(gcp_conn_id="google_cloud_default")

        # Idempotency: backfills get re-run, don't re-download/upload what's already there
        if hook.exists(bucket_name=GCS_BUCKET, object_name=object_name):
            log.info("Already in GCS, skipping upload %s", object_name)
            return object_name

        # download, apply schema, clean up, annotate
        df = download_and_extract_csv(year, month)
        df = annotate_columns(df, year, month)

        # write to buffer memory
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)
        log.info("Wrote %s rows x %s cols to buffer memory", len(df), len(df.columns))

        hook.upload(
            bucket_name=GCS_BUCKET,
            object_name=object_name,
            data=buffer.getvalue(),
            timeout=600,
        )
        log.info("Uploaded → gs://%s/%s", GCS_BUCKET, object_name)
        return object_name

    check_availability() >> ingest_month()


bts_ontime_reporting()
