-- One row per operated flight leg. Sparse delay-cause columns and the
-- Div1-5 diversion detail are intentionally excluded here — see
-- stg_diversion_detail for the normalised diversion table, and
-- docs/data_quality_notes.md for why the diversion detail was collapsed.
--
-- Casts below are only applied where a real transform happens (STRING to
-- DATE, 0/1 flag to BOOL). Columns already typed correctly at ingestion
-- are simply renamed, since BigQuery has one integer type and string type

with source as (
    select * from {{ source('raw_bts', 'ontime_reporting') }}
),

renamed_and_typed as (
    select
    -- identifiers
    Flight_Number_Reporting_Airline as flight_number,
    Tail_Number as tail_number,
    Reporting_Airline as carrier_code,

    -- date - the source of truth for all date logic, not source_year/source_month
    parse_date('%Y-%m-%d', FlightDate) as flight_date,
    Year as flight_year,
    Quarter as flight_quarter,
    Month as flight_month,
    DayofMonth as day_of_month,
    DayOfWeek as day_of_week,

    -- origin
    Origin as origin_airport,
    OriginAirportID as origin_airport_id,
    OriginCityMarketID as origin_city_market_id,
    OriginState as origin_state,

    -- destination
    Dest as destination_airport,
    DestAirportID as destination_airport_id,
    DestCityMarketID as destination_city_market_id,
    DestState as destination_state,

    -- scheduled vs actual — kept as raw HHMM ints; real timestamps need
    -- each airport's timezone, built once dim_airport exists
    CRSDepTime as scheduled_departure_time,
    DepTime as actual_departure_time,
    CRSArrTime as scheduled_arrival_time,
    ArrTime as actual_arrival_time,

    -- delay measures
    DepDelay as departure_delay_minutes,
    DepDel15 = 1 as departure_delayed_15,
    ArrDelay as arrival_delay_minutes,
    ArrDel15 = 1 as arrival_delayed_15,

    -- delay cause breakdown — populated only when arrival_delay_minutes >= 15,
    -- and only from June 2003 onward (see data_quality_notes.md)
    CarrierDelay as carrier_delay_minutes,
    WeatherDelay as weather_delay_minutes,
    NASDelay as nas_delay_minutes,
    SecurityDelay as security_delay_minutes,
    LateAircraftDelay as late_aircraft_delay_minutes,

    -- cancellation / diversion (summary)
    Cancelled = 1 as was_cancelled,
    CancellationCode as cancellation_code,
    Diverted = 1 as was_diverted,

    -- flight characteristics
    CRSElapsedTime as scheduled_elapsed_minutes,
    ActualElapsedTime as actual_elapsed_minutes,
    AirTime as air_time_minutes,
    Distance as distance_miles,
    DistanceGroup as distance_group,

    -- last non-null Div*Airport if diverted, else the scheduled destination
    coalesce(
        Div5Airport, Div4Airport, Div3Airport, Div2Airport, Div1Airport,
        Dest
    ) as final_destination_airport,
    DivAirportLandings as diversion_landing_count,

    -- provenance — which file this row came from, not what it describes.
    -- See "Provenance vs. source columns" in data_quality_notes.md.
    source_year,
    source_month,
    _source_file,
    _ingested_at

    from source

)

select * from renamed_and_typed