-- Diversions modelled as a separate table rather than a nested/repeated
-- BigQuery STRUCT column, despite the latter being more BigQuery-idiomatic
-- and avoiding the join entirely. Three reasons:
--   1. Grain clarity — fct_flights stays purely flight-grain; diversion
--      detail is its own grain (one row per diversion leg), kept explicit
--      rather than nested inside a flight row.
--   2. BI tool compatibility — Data Studio and similar tools handle flat
--      joins more reliably than nested/repeated fields.
--   3. Legibility — the modelling decision (unpivoting BTS's wide Div1-5
--      repeated group) is visible in the model list, not buried in a
--      struct definition.
--   4. Expected usage — diversion detail is for drilling into a specific
--      flight (e.g. "what happened to this one diverted flight"), which
--      is naturally a row-per-leg table view. The wide Div1-5 source
--      shape is effectively undisplayable as-is; multiple short rows are
--      more practical than one row with 40 mostly-null columns.
-- Storage/billing was not a factor either way: BigQuery is columnar, so
-- the sparse Div1-5 source columns cost nothing sitting unused on
-- stg_flights regardless of which approach is taken.
-- One row per diverted-airport landing, unpivoted from the wide Div1-5
-- columns in the source. Most flights are not diverted, so this table is
-- small relative to stg_flights — only rows with a non-null diversion
-- airport are kept. See "Div1-5 diversion detail" in
-- docs/data_quality_notes.md for why this shape exists in the source.

with source as (

    select * from {{ source('raw_bts', 'ontime_reporting') }}

),

-- a synthetic flight identifier to key the unpivoted rows back to
-- stg_flights, since the source has no single-column primary key
with_flight_key as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'source_year', 'source_month', 'Reporting_Airline',
            'Flight_Number_Reporting_Airline', 'FlightDate',
            'Origin', 'Dest', 'CRSDepTime'
        ]) }} as flight_id,
        *
    from source

),

unpivoted as (

    select
        flight_id,
        1 as diversion_sequence,
        Div1Airport as diversion_airport,
        Div1AirportID as diversion_airport_id,
        Div1AirportSeqID as diversion_airport_seq_id,
        Div1WheelsOn as wheels_on_time,
        Div1WheelsOff as wheels_off_time,
        Div1TotalGTime as total_ground_time_minutes,
        Div1LongestGTime as longest_ground_time_minutes,
        Div1TailNum as tail_number
    from with_flight_key

    union all

    select
        flight_id,
        2,
        Div2Airport,
        Div2AirportID,
        Div2AirportSeqID,
        Div2WheelsOn,
        Div2WheelsOff,
        Div2TotalGTime,
        Div2LongestGTime,
        Div2TailNum
    from with_flight_key

    union all

    select
        flight_id,
        3,
        Div3Airport,
        Div3AirportID,
        Div3AirportSeqID,
        Div3WheelsOn,
        Div3WheelsOff,
        Div3TotalGTime,
        Div3LongestGTime,
        Div3TailNum
    from with_flight_key

    union all

    select
        flight_id,
        4,
        Div4Airport,
        Div4AirportID,
        Div4AirportSeqID,
        Div4WheelsOn,
        Div4WheelsOff,
        Div4TotalGTime,
        Div4LongestGTime,
        Div4TailNum
    from with_flight_key

    union all

    select
        flight_id,
        5,
        Div5Airport,
        Div5AirportID,
        Div5AirportSeqID,
        Div5WheelsOn,
        Div5WheelsOff,
        Div5TotalGTime,
        Div5LongestGTime,
        Div5TailNum
    from with_flight_key

)

select *
from unpivoted
where diversion_airport is not null
