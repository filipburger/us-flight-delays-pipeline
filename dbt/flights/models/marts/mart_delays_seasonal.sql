-- Delay and cancellation patterns by season and year, with holiday
-- flag broken out separately. Surfaces both cyclical seosonal patterns
-- and the Covid-era volume collape (2020-2-21) in one place.
-- Grain: one row per (year, season, is_holiday)

with flights as (

    select * from {{ ref("fct_flights") }}

),

dates as (

    select * from {{ ref('dim_date') }}
),

flights_with_season as (

    select
        f.*,
        d.travel_era,
        d.season,
        d.is_holiday

    from flights as f
    left join dates as d
        on f.flight_date = d.date_day

),

by_season as (
    select
        travel_era,
        flight_year,
        season,
        is_holiday,

        -- metrics
        count(*) as total_flights,
        countif(flight_outcome = 'completed') as completed_flights,
        countif(flight_outcome like 'cancelled%') as cancelled_flights,
        countif(delayed_on_arrival) as delayed_arrivals,

        avg(arrival_delay_minutes) as avg_arrival_delay_minutes,
        avg(departure_delay_minutes) as avg_departure_delay_minutes

    from flights_with_season
    group by 1, 2, 3, 4

)

select
    *,
    round(safe_divide(cancelled_flights, total_flights) * 100, 2)
        as cancellation_rate_pct,
    round(safe_divide(delayed_arrivals, total_flights) * 100, 2)
        as delay_rate_pct

from by_season
order by flight_year, season, is_holiday
