-- Delay and cancellation patterns by season and year, with holiday
-- flag broken out separately. Surfaces both cyclical seosonal patterns
-- and the Covid-era volume collape (2020-2-21) in one place.
-- Grain: one row per (year, season, is_holiday)

with flights as (

    select * from {{ ref('fct_flights') }}

),

dates as (

    select * from {{ ref('dim_date') }}

),

flights_with_season as (

    select
        f.*,
        d.season,
        d.is_holiday

    from flights as f
    left join dates as d
        on f.flight_date = d.date_day

),

by_season as (

    select
        flight_year,
        season,
        is_holiday,

        count(*) as total_flights,
        countif(flight_outcome = 'completed') as completed_flights,
        countif(flight_outcome like 'cancelled%') as cancelled_flights,
        countif(delayed_on_arrival) as delayed_arrivals,

        avg(arrival_delay_minutes) as avg_arrival_delay_minutes,
        avg(departure_delay_minutes) as avg_departure_delay_minutes

    from flights_with_season
    group by 1, 2, 3

),

pre_covid_baseline as (

    select
        safe_divide(sum(delayed_arrivals), sum(completed_flights))
            as baseline_delay_rate,
        safe_divide(sum(cancelled_flights), sum(total_flights))
            as baseline_cancellation_rate

    from by_season
    where flight_year in (2018, 2019)

)

select
    s.*,
    b.baseline_delay_rate,
    b.baseline_cancellation_rate

from by_season as s
cross join pre_covid_baseline as b
order by s.flight_year, s.season, s.is_holiday
