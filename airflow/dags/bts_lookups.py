import codecs
import io
import logging
from datetime import datetime
import gc

import pandas as pd
import requests
import urllib3
from airflow.sdk import dag, task
from airflow.providers.google.cloud.hooks.gcs import GCSHook

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
log = logging.getLogger(__name__)

GCS_BUCKET ="filipburger-flight-data-lake"
GCS_PREFIX = "raw/bts_lookups"

# Lookup tables that resolve doded columns in the on-time fact table.
# Most are simpel Code/Descroption pairs.
LOOKUP_TABLES = [
    # Time
    "L_QUARTERS",             # Quarter
    "L_MONTHS",               # Month
    "L_WEEKDAYS",             # DayOfWeek
    # Carrier
    "L_UNIQUE_CARRIERS",      # Reporting_Airline
    "L_AIRLINE_ID",           # DOT_ID_Reporting_Airline
    "L_CARRIER_HISTORY",      # IATA_CODE_Reporting_Airline
    # Geography
    "L_AIRPORT",              # Origin / Dest
    "L_AIRPORT_ID",           # OriginAirportID / DestAirportID
    "L_AIRPORT_SEQ_ID",       # OriginAirportSeqID / DestAirportSeqID
    "L_CITY_MARKET_ID",       # OriginCityMarketID / DestCityMarketID
    "L_STATE_ABR_AVIATION",   # OriginState / DestState
    "L_STATE_FIPS",           # OriginStateFips / DestStateFips
    "L_WORLD_AREA_CODES",     # OriginWac / DestWac
    # Coded measures
    "L_YESNO_RESP",           # DepDel15, ArrDel15, Cancelled, Diverted  
    "L_ONTIME_DELAY_GROUPS",  # DepartureDelayGroups / ArrivalDelayGroups
    "L_DEPARRBLK",            # DepTimeBlk / ArrTimeBlk
    "L_CANCELLATION",         # CancellationCode
    "L_DISTANCE_GROUP_250",   # DistanceGroup
    "L_DIVERSIONS",           # DivAirportLandings
]

# BTS obfuscates URL parameters by rotating 13 positions through a combined
# digit+uppercase+lowercase alphabet. Not standard ROT13: characters near the
# end of one range wrap into the next, so N→a and 2→F.
_BTS_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
_BTS_ENCODE = str.maketrans(_BTS_ALPHABET, _BTS_ALPHABET[13:] + _BTS_ALPHABET[:13])
_BTS_DECODE = str.maketrans(_BTS_ALPHABET[13:] + _BTS_ALPHABET[:13], _BTS_ALPHABET)


def bts_encode(text: str) -> str:
    """Encode a table name for a BTS URL parameter."""
    return text.translate(_BTS_ENCODE)


def bts_decode(text: str) -> str:
    """Decode a BTS URL parameter back to readable text."""
    return text.translate(_BTS_DECODE)


def lookup_url(table: str) -> str:
    return f"https://transtats.bts.gov/Download_Lookup.asp?Y11x72={bts_encode(table)}"

@dag(
    dag_id="bts_lookups",
    schedule="@monthly", # reference data changes rarely, but does change
    start_date=datetime(2026, 8, 1), # no history to backfill, just current state snapshot
    catchup=False,
    tags=["bts", "monthly", "reference"],
    default_args={"retries": 2},
    )
def bts_lookups():

    @task
    def fetch_lookup(table_name: str) -> str:
        """Download one lookup table and overwirite it in gcs as parquet"""
        url = lookup_url(table_name)
        log.info("Fetching %s from %s", table_name, url)

        resp = requests.get(url, verify=False, timeout=120)
        resp.raise_for_status()

        # All columns as strings, these are identifiers and labels.
        # Avoids ID beiong read as int in one table and as str in another.
        # Final casting happens in dbt.
        df = pd.read_csv(io.BytesIO(resp.content), dtype='string')

        if df.empty:
            raise ValueError(f"{table_name} returned no rows - check table name")

        df["_ingested_at"] = pd.Timestamp.now(tz="UTC")

        buffer = io.BytesIO()
        df.to_parquet(buffer,engine="pyarrow", compression="snappy", index=False)
        row_cnt=len(df) # to save it for logs before deletion

        del df
        gc.collect()

        object_name =f"{GCS_PREFIX}/{table_name.lower()}/data.parquet"
        GCSHook(gcp_conn_id="google_cloud_default").upload(
            bucket_name=GCS_BUCKET,
            object_name=object_name,
            data=buffer.getvalue(),
            timeout=120
        )

        log.info("%s: %s rows → gs://%s/%s", table_name, row_cnt, GCS_BUCKET, object_name)
        return object_name

    fetch_lookup.expand(table_name=LOOKUP_TABLES)


bts_lookups()
        