-- Monthly delay and cancellation summary by carrier. Grain: one row
-- per (carrier, year, month).
--
-- Rates are computed in the BI layer (SUM(numerator)/SUM(denominator))
-- rather than stored here — pre-computed percentages don't re-aggregate
-- correctly across grains.
-- Denominator for cancellation rate: total_flights (all scheduled).
-- Denominator for delay rate: completed_flights (cancelled excluded).
--
-- NOTE: avg delay columns are intentionally excluded. Storing averages
-- at (carrier, year, month) grain makes it impossible to correctly
-- aggregate to higher grains — summing or averaging those averages
-- weights each month equally regardless of flight volume (July has
-- significantly higher traffic than February).
-- Always derive averages in the -- BI layer as 
-- SUM(delay_minutes) / SUM(delayed_flights) so numerator and
-- denominator scale together across any grain.

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

        case
            when flight_year in (2018, 2019)
                then 'pre_covid'
            when flight_year in (2020, 2021)
                then 'covid'
            when flight_year in (2022, 2023)
                then 'recovery'
            when flight_year in (2024, 2025)
                then 'new_normal'
        end as travel_era,

        -- cancellation cause breakdown
        countif(cancellation_code = 'A') as carrier_cancellations,
        countif(cancellation_code = 'B') as weather_cancellations,
        countif(cancellation_code = 'C') as nas_cancellations,
        countif(cancellation_code = 'D') as security_cancellations,

        -- delay cause breakdown
        sum(carrier_delay_minutes) as carrier_delay_minutes,
        sum(late_aircraft_delay_minutes) as late_aircraft_delay_minutes,
        sum(weather_delay_minutes) as weather_delay_minutes,
        sum(nas_delay_minutes) as nas_delay_minutes,
        sum(security_delay_minutes) as security_delay_minutes

    from flights
    group by 1, 2, 3, 4

),

with_grand_total as (

    -- Data Studio's built-in "% of total" comparison calculation returns
    -- NULL on the synthetic "Other" row when a table groups the long
    -- tail of carriers together — computing the grand total here
    -- sidesteps that by making % of total a plain division on real
    -- columns, which works for every row including "Other".
    select
        *,
        sum(if(travel_era = 'new_normal', total_flights, 0)) over ()
            as new_normal_grand_total_flights

    from by_carrier_month

)

select * from with_grand_total
