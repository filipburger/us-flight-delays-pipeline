-- Carrier dimension. Reporting_Airline (BTS's unique carrier code) is the
-- join key, not IATA code — see "Lookup tables" in
-- docs/data_quality_notes.md for why: IATA codes are reassigned to
-- different carriers over time, while the unique carrier code is stable.

with carriers as (

    select * from {{ ref('stg_carriers') }}
)

select
    carrier_code,
    carrier_name

from carriers
