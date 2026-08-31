-- Monthly delay summary by airport, covering both departure and
-- arrival activity. Grain: one row per (airport, direction, year,
-- month). "direction" distinguishes an airport's role as an origin
-- (departure delays) from its role as a destination (arrival delays)
-- — the same airport appears twice per month if it had both.

with flights as (

    select * from {{ ref('fct_flights') }}

),

departures as (

    select
        origin_airport as airport_code,
        'departure' as direction,
        flight_year,
        flight_month,

        count(*) as total_flights,
        countif(flight_outcome = 'completed') as completed_flights,
        countif(flight_outcome like 'cancelled%') as cancelled_flights,
        countif(delayed_on_departure) as delayed_flights,
        avg(departure_delay_minutes) as avg_delay_minutes

    from flights
    group by 1, 2, 3, 4

),

arrivals as (

    select
        destination_airport as airport_code,
        'arrival' as direction,
        flight_year,
        flight_month,

        count(*) as total_flights,
        countif(flight_outcome = 'completed') as completed_flights,
        countif(flight_outcome like 'cancelled%') as cancelled_flights,
        countif(delayed_on_arrival) as delayed_flights,
        avg(arrival_delay_minutes) as avg_delay_minutes

    from flights
    group by 1, 2, 3, 4

),

combined as (

    select * from departures
    union all
    select * from arrivals

)

select *
from combined
