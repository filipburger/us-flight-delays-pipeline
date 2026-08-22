-- Cancellation reason dimension. Only meaningful where 
-- stg_flights.cancellation_code is populated (i.e. was_canelled = true).

with cancellation_reasons as (

    select * from {{ ref('stg_cancellation_reasons') }}

)

select
    cancellation_code,
    cancellation_reason

from cancellation_reasons
