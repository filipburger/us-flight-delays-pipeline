-- One row per operated flight leg. Sparse delay-cause columns and the
-- Div1-5 diversion detail are intentionally excluded here — see
-- stg_diversion_detail for the normalised diversion table, and
-- docs/data_quality_notes.md for why the diversion detail was collapsed.
--
-- Casts below are only applied where a real transform happens (STRING to
-- DATE, 0/1 flag to BOOL). Columns already typed correctly at ingestion
-- are simply renamed, since BigQuery has one integer type and string
-- type.
--
-- flight_outcome distinguishes cancellation timing, discovered by
-- cross-tabulating Cancelled/Diverted/DepTime/WheelsOff/
-- DivAirportLandings. DivAirportLandings=9 is a BTS sentinel (not a
-- real count) meaning "cancelled after departure, diversion columns
-- populated but this flight was never actually diverted". See
-- "Cancellation timing" in docs/data_quality_notes.md.

with source as (

    select * from {{ source('raw_bts', 'ontime_reporting') }}

),

renamed_and_typed as (

    select
        -- identifiers
        {{ dbt_utils.generate_surrogate_key([
            'source_year', 'source_month', 'Reporting_Airline',
            'Flight_Number_Reporting_Airline', 'FlightDate',
            'Origin', 'Dest', 'CRSDepTime'
        ]) }} as flight_id,
        Flight_Number_Reporting_Airline as flight_number,
        Tail_Number as tail_number,
        Reporting_Airline as carrier_code,

        -- date - the source of truth for all date logic, not
        -- source_year/source_month
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

        -- scheduled vs actual — kept as raw HHMM ints; real timestamps
        -- need each airport's timezone, built once dim_airport exists
        CRSDepTime as scheduled_departure_time,
        DepTime as actual_departure_time,
        CRSArrTime as scheduled_arrival_time,
        ArrTime as actual_arrival_time,

        -- delay measures
        DepDelay as departure_delay_minutes,
        DepDel15 = 1 as departure_delayed_15,
        ArrDelay as arrival_delay_minutes,
        ArrDel15 = 1 as arrival_delayed_15,

        -- delay cause breakdown — populated only when
        -- arrival_delay_minutes >= 15, and only from June 2003 onward
        -- (see data_quality_notes.md)
        CarrierDelay as carrier_delay_minutes,
        WeatherDelay as weather_delay_minutes,
        NASDelay as nas_delay_minutes,
        SecurityDelay as security_delay_minutes,
        LateAircraftDelay as late_aircraft_delay_minutes,

        -- cancellation / diversion (summary)
        Cancelled = 1 as was_cancelled,
        CancellationCode as cancellation_code,
        Diverted = 1 as was_diverted,

        case
            when Diverted = 1
                then 'diverted'
            when Cancelled = 1 and DepTime is null
                then 'cancelled_before_pushback'
            when Cancelled = 1 and WheelsOff is null
                then 'cancelled_after_pushback'
            when Cancelled = 1 and DivAirportLandings = 9
                then 'cancelled_after_departure'
            when Cancelled = 1
                then 'cancelled_other'
            else 'completed'
        end as flight_outcome,

        -- flight characteristics
        CRSElapsedTime as scheduled_elapsed_minutes,
        ActualElapsedTime as actual_elapsed_minutes,
        AirTime as air_time_minutes,
        Distance as distance_miles,
        DistanceGroup as distance_group,

        -- final_diversion_airport only meaningful for genuine
        -- diversions (Diverted = 1); DivAirportLandings=9 rows have
        -- Div* columns populated too, but that's ground-return activity
        -- on a cancelled flight, handled in stg_diversion_detail
        case
            when Diverted = 1 then coalesce(
                Div5Airport, Div4Airport, Div3Airport,
                Div2Airport, Div1Airport
            )
        end as final_diversion_airport,

        -- provenance — which file this row came from, not what it
        -- describes. See "Provenance vs. source columns" in
        -- docs/data_quality_notes.md.
        source_year,
        source_month,
        _source_file,
        _ingested_at

    from source

)

select * from renamed_and_typed
