from datetime import datetime, timedelta
import logging

import requests
import urllib3
from airflow.sdk import dag, task
from pendulum import DateTime

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

def bts_request(method: str, url: str, **kwargs) -> requests.Response:
    """Request BTS with TLS verification, falling back to unverified on the known cert issue"""
    try:
        return requests.request(method, url, **kwargs)
    except requests.exceptions.SSLError:
        log.warning("TLS verification failed for %s, retrying unverified", url)
        # in case user pass verify true, we need override
        kwargs["verify"] = False
        return requests.request(method, url, **kwargs)


@dag(
    dag_id="bts_ontime_reporting",
    schedule="@monthly",
    start_date=datetime(2026,5, 1),
    end_date=datetime(2026, 8, 1),
    catchup=True,
    max_active_runs=1,
    tags=["bts"],
    default_args={"retries":1},
)
def bts_ontime_reporting():

    # @task
    # def show_target_month(logical_date=None):
    #     """Prove we understand which month each run is responsible for."""
    #     year, month = logical_date.year, logical_date.month
    #     log.info("This DAG run is resposible for %s-%02d", year, month)
    #     return f"{year}-{month:02d}"

    # show_target_month()

    @task.short_circuit
    def check_availability(logical_date: DateTime = None) -> bool:
        """Return False (skipping everything downstream) if BTS hasn't published this month yet."""
        url = BASE_URL.format(year=logical_date.year, month=logical_date.month)

        resp = bts_request('HEAD', url, timeout=60)
        available = resp.status_code == 200

        log.info(
            "%s-%02d → HTTP %s (%s)",
            logical_date.year,
            logical_date.month,
            resp.status_code,
            "available" if available else "not published yet",
        )
        return available

    check_availability()



bts_ontime_reporting()
