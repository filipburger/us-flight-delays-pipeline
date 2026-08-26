-- One flow per operated flight leg. Grain matches stg_flights.
--
-- all dimension joins LEFT JOIN not INNER - cancellation code and 
-- the delay group codes are null for majority of flights (only
-- populated when cancelled / delayed respectively), and an INNER JOIN
-- would silently drop every flight missing that attribute. 

select
    --identifiers
    f.flight_id,
    f.flight_number,
    f.tail_number,
    f.carrier_code,
    dc.carrier_iata_code,
    dc.carrier_name,

    f.flight_date,
    f.flight_year,
    f.flight_month,

    f.origin_airport,
    f.origin_state,
    f.destination_airport,
    f.destination_state,

    f.scheduled_departure_time,
    f.actual_departure_time,
    f.scheduled_arrival_time,
    f.actual_arrival_time,

    f.departure_delay_minutes,
    f.delayed_on_departure,
    f.departure_delay_group_code,
    f.arrival_delay_minutes,
    f.delayed_on_arrival,
    f.arrival_delay_group_code,

    f.carrier_delay_minutes,
    f.weather_delay_minutes,
    f.nas_delay_minutes,
    f.security_delay_minutes,
    f.late_aircraft_delay_minutes,

    f.was_cancelled,
    f.cancellation_code,
    dcr.cancellation_reason,
    f.was_diverted,
    f.flight_outcome,
    f.delay_pattern,
    f.final_diversion_airport,

    f.scheduled_elapsed_minutes,
    f.actual_elapsed_minutes,
    f.air_time_minutes,
    f.distance_miles,
    f.distance_group

from {{ ref('stg_flights') }} as f
left join {{ ref('dim_carrier') }} as dc
    on f.carrier_code = dc.carrier_code
left join {{ ref('dim_cancellation_reason') }} as dcr
    on f.cancellation_code = dcr.cancellation_code
