"""Explicit dtypes for BTS On-Time Reporting Carrier extracts.

Specifying dtypes at read time (rather than letting pandas infer per file)
guarantees every monthly parquet has an identical schema — important because
the Div1-5 columns are 96-100% null and would otherwise infer differently
across months, breaking the BigQuery load.

Nullable dtypes (Int8/Int16/Int32) are used throughout rather than their
non-nullable equivalents, so a single unexpected null can't fail a backfill.
Booleans and dates stay as raw types here and are cast in dbt.
"""

BTS_ONTIME_REPORTING_DTYPES = {
    # Date / calendar
    "Year": "Int16",
    "Quarter": "Int8",
    "Month": "Int8",
    "DayofMonth": "Int8",
    "DayOfWeek": "Int8",
    "FlightDate": "string",  # cast to date in dbt — avoids tz/format assumptions at ingestion

    # Carrier identifiers
    "Reporting_Airline": "string",
    "DOT_ID_Reporting_Airline": "Int32",
    "IATA_CODE_Reporting_Airline": "string",
    "Tail_Number": "string",
    "Flight_Number_Reporting_Airline": "Int32",

    # Origin
    "OriginAirportID": "Int32",
    "OriginAirportSeqID": "Int32",
    "OriginCityMarketID": "Int32",
    "Origin": "string",
    "OriginCityName": "string",
    "OriginState": "string",
    "OriginStateFips": "string",
    "OriginStateName": "string",
    "OriginWac": "Int16",

    # Destination — same shape as Origin
    "DestAirportID": "Int32",
    "DestAirportSeqID": "Int32",
    "DestCityMarketID": "Int32",
    "Dest": "string",
    "DestCityName": "string",
    "DestState": "string",
    "DestStateFips": "string",
    "DestStateName": "string",
    "DestWac": "Int16",

    # Departure performance — HHMM kept as int; real timestamp math is dbt's job
    "CRSDepTime": "Int32",
    "DepTime": "Int32",
    "DepDelay": "float32",
    "DepDelayMinutes": "float32",
    "DepDel15": "Int8",  # 0/1 flag, cast to boolean in dbt
    "DepartureDelayGroups": "Int8",
    "DepTimeBlk": "string",
    "TaxiOut": "float32",
    "WheelsOff": "Int32",

    # Arrival performance
    "WheelsOn": "Int32",
    "TaxiIn": "float32",
    "CRSArrTime": "Int32",
    "ArrTime": "Int32",
    "ArrDelay": "float32",
    "ArrDelayMinutes": "float32",
    "ArrDel15": "Int8",
    "ArrivalDelayGroups": "Int8",
    "ArrTimeBlk": "string",

    # Cancellation / diversion (summary) — 0/1 in source, cast to boolean in dbt
    "Cancelled": "Int8",
    "CancellationCode": "string",
    "Diverted": "Int8",

    # Duration & distance
    "CRSElapsedTime": "float32",
    "ActualElapsedTime": "float32",
    "AirTime": "float32",
    "Flights": "Int8",
    "Distance": "float32",
    "DistanceGroup": "Int8",

    # Delay cause breakdown — only populated when delay >= 15 min, ~77% null by design
    "CarrierDelay": "float32",
    "WeatherDelay": "float32",
    "NASDelay": "float32",
    "SecurityDelay": "float32",
    "LateAircraftDelay": "float32",

    # Gate return / cancelled flight ground time
    "FirstDepTime": "Int32",
    "TotalAddGTime": "float32",
    "LongestAddGTime": "float32",

    # Diversion detail (summary)
    "DivAirportLandings": "Int8",
    "DivReachedDest": "Int8",
    "DivActualElapsedTime": "float32",
    "DivArrDelay": "float32",
    "DivDistance": "float32",
}

# Div1-5: identical 8-field block per diverted airport. Collapsed to a
# was_diverted flag + normalised detail table in dbt; kept raw here.
for _n in range(1, 6):
    BTS_ONTIME_REPORTING_DTYPES.update(
        {
            f"Div{_n}Airport": "string",
            f"Div{_n}AirportID": "Int32",
            f"Div{_n}AirportSeqID": "Int32",
            f"Div{_n}WheelsOn": "Int32",
            f"Div{_n}TotalGTime": "float32",
            f"Div{_n}LongestGTime": "float32",
            f"Div{_n}WheelsOff": "Int32",
            f"Div{_n}TailNum": "string",
        }
    )
    