-- Delay and cancellation patterns by season and year, with holiday
-- flag broken out separately. Surfaces both cyclical seosonal patterns
-- and the Covid-era volume collape (2020-2-21) in one place.
-- Grain: one row per (year, season, is_holiday)
--
-- Rates are computed in the BI layer (SUM(numerator)/SUM(denominator))
-- rather than stored here — pre-computed percentages don't re-aggregate
-- correctly across grains.
-- Denominator for cancellation rate: total_flights (all scheduled).
-- Denominator for delay rate: completed_flights (cancelled excluded).
--
-- NOTE: avg delay columns are intentionally excluded. Storing averages
-- at (year, month) grain makes it impossible to correctly
-- aggregate to higher grains — summing or averaging those averages
-- weights each month equally regardless of flight volume (July has
-- significantly higher traffic than February). Always derive averages 
-- in the -- BI layer as SUM(delay_minutes) / SUM(delayed_flights)
-- so numerator and denominator scale together across any grain.

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
        format('%04d-%02d', f.flight_year, f.flight_month) as year_month

    from flights as f
    left join dates as d
        on f.flight_date = d.date_day

),

by_season as (

    select
        flight_year,
        season,
        year_month,
        date(flight_year, flight_month, 1) as flight_month_date,

        count(*) as total_flights,
        countif(flight_outcome = 'completed') as completed_flights,
        countif(flight_outcome like 'cancelled%') as cancelled_flights,
        countif(delayed_on_arrival) as delayed_arrivals,

        -- delay minutes by reason
        sum(carrier_delay_minutes) as carrier_delay_minutes,
        sum(late_aircraft_delay_minutes) as late_aircraft_delay_minutes,
        sum(weather_delay_minutes) as weather_delay_minutes,
        sum(nas_delay_minutes) as nas_delay_minutes,
        sum(security_delay_minutes) as security_delay_minutes,
        sum(
            carrier_delay_minutes + late_aircraft_delay_minutes
            + weather_delay_minutes + nas_delay_minutes
            + security_delay_minutes
        ) as delay_minutes_reason_total,

        -- cancelation counts by reason
        countif(cancellation_code = 'A') as carrier_cancellations,
        countif(cancellation_code = 'B') as weather_cancellations,
        countif(cancellation_code = 'C') as nas_cancellations,
        countif(cancellation_code = 'D') as security_cancellations

    from flights_with_season
    group by 1, 2, 3, 4

),

pre_covid_baseline as (

    select
        safe_divide(sum(delayed_arrivals), sum(completed_flights))
            as baseline_delay_rate,
        safe_divide(sum(cancelled_flights), sum(total_flights))
            as baseline_cancellation_rate,
        safe_divide(sum(total_flights), 2)
            as baseline_total_flights_per_year

    from by_season
    where flight_year in (2018, 2019)

)

select
    s.*,
    b.baseline_delay_rate,
    b.baseline_cancellation_rate,
    b.baseline_total_flights_per_year

from by_season as s
cross join pre_covid_baseline as b
order by s.flight_year, s.season
