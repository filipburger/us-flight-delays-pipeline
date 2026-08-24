-- Airport dimension, sourced from BTS's own origin/destination fields
-- in stg_flights (no external join yet). Airport codes and their
-- associated city/state can appear on either the origin or
-- destination side of any flight, so both sides are combined and
-- deduplicated to get one row per airport.
--
-- Deliberately thin: no coordinates or timezone. Can be extended by
-- joining OurAirports (lat/long, timezone, airport type) if needed.

with origin_airports as (

    select distinct
        origin_airport as airport_code,
        origin_state as state

    from {{ ref('stg_flights') }}

),

destination_airports as (

    select distinct
        destination_airport as airport_code,
        destination_state as state

    from {{ ref('stg_flights') }}

),

combined as (

    select * from origin_airports
    union distinct
    select * from destination_airports

)

select
    airport_code,
    state

from combined
