-- Monthly delay and cancellation summary by carrier. Grain: one row
-- per (carrier, year, month).
--
-- Denominators: cancellation_rate_pct divides by all scheduled
-- flights (a cancellation is a real outcome of a scheduled flight).
-- delay_rate_pct divides by completed_flights only — a cancelled
-- flight was never "on time or late," so including it would
-- understate the rate.

with flights as (

    select * from {{ ref('fct_flights') }}

),

by_carrier_month as (

    select
        carrier_code,
        carrier_name,
        flight_year,
        flight_month,

        count(*) as total_flights,
        countif(flight_outcome = 'completed') as completed_flights,
        countif(flight_outcome = 'diverted') as diverted_flights,
        countif(flight_outcome like 'cancelled%') as cancelled_flights,

        countif(delayed_on_arrival) as delayed_arrivals,
        countif(delay_pattern = 'recovered_in_air') as recovered_in_air_flights,
        countif(delay_pattern = 'delayed_in_air') as delayed_in_air_flights,

        avg(arrival_delay_minutes) as avg_arrival_delay_minutes,
        avg(departure_delay_minutes) as avg_departure_delay_minutes,
        sum(arrival_delay_minutes) as total_delay_minutes,
        sum(if(delayed_on_arrival, arrival_delay_minutes, 0))
            as total_minutes_lost_to_delays,

        avg(carrier_delay_minutes) as avg_carrier_delay_minutes,
        avg(weather_delay_minutes) as avg_weather_delay_minutes,
        avg(nas_delay_minutes) as avg_nas_delay_minutes,
        avg(late_aircraft_delay_minutes) as avg_late_aircraft_delay_minutes

    from flights
    group by 1, 2, 3, 4

)

select
    *,
    round(safe_divide(cancelled_flights, total_flights) * 100, 2)
        as cancellation_rate_pct,
    round(safe_divide(delayed_arrivals, completed_flights) * 100, 2)
        as delay_rate_pct

from by_carrier_month
