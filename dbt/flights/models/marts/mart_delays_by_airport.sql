-- Monthly delay summary by airport, covering both departure and
-- arrival activity. Grain: one row per (airport, direction, year,
-- month). "direction" distinguishes an airport's role as an origin
-- (departure delays) from its role as a destination (arrival delays)
-- — the same airport appears twice per month if it had both.
--
-- Rates are computed in the BI layer (SUM(numerator)/SUM(denominator))
-- rather than stored here — pre-computed percentages don't re-aggregate
-- correctly across grains.
-- Denominator for cancellation rate: total_flights (all scheduled).
-- Denominator for delay rate: completed_flights (cancelled excluded).
--
-- NOTE: avg delay columns are intentionally excluded. Storing averages
-- at (airport, year, month) grain makes it impossible to correctly
-- aggregate to higher grains — summing or averaging those averages
-- weights each month equally regardless of flight volume (July has
-- significantly higher traffic than February). Always derive averages 
-- in the BI layer as SUM(delay_minutes) / SUM(delayed_flights) 
-- so numerator and denominator scale together across any grain.


with flights as (

    select * from {{ ref('fct_flights') }}

),

departures as (

    select
        origin_airport as airport_code,
        origin_city_name as city_name,
        'departure' as direction,
        flight_year,
        flight_month,
        date(flight_year, flight_month, 1) as flight_month_date,

        count(*) as total_flights,
        countif(flight_outcome = 'completed') as completed_flights,
        countif(flight_outcome like 'cancelled%') as cancelled_flights,
        countif(delayed_on_departure) as delayed_flights

    from flights
    group by 1, 2, 3, 4, 5, 6

),

arrivals as (

    select
        destination_airport as airport_code,
        destination_city_name as city_name,
        'arrival' as direction,
        flight_year,
        flight_month,
        date(flight_year, flight_month, 1) as flight_month_date,

        count(*) as total_flights,
        countif(flight_outcome = 'completed') as completed_flights,
        countif(flight_outcome like 'cancelled%') as cancelled_flights,
        countif(delayed_on_arrival) as delayed_flights

    from flights
    group by 1, 2, 3, 4, 5, 6

),

combined as (

    select * from departures
    union all
    select * from arrivals

),

with_era as (

    select
        *,
        case
            when flight_year in (2018, 2019)
                then 'pre_covid'
            when flight_year in (2020, 2021)
                then 'covid'
            when flight_year in (2022, 2023)
                then 'recovery'
            when flight_year in (2024, 2025)
                then 'new_normal'
        end as travel_era

    from combined

),

enriched as (

    -- Grand total across both directions combined — airport traffic
    -- share is conventionally measured as total movements
    -- (departures + arrivals), not tracked separately per direction.
    select
        *,
        sum(if(travel_era = 'new_normal', total_flights, 0)) over ()
            as new_normal_grand_total_flights

    from with_era

)

select * from enriched
