-- Carrier dimension. Reporting_Airline (BTS's unique carrier code) is
-- the join key, not IATA code — see "Lookup tables" in
-- docs/data_quality_notes.md for why: IATA codes are reassigned to
-- different carriers over time, while the unique carrier code is
-- stable.
--
-- carrier_iata_code is pulled from stg_flights rather than the
-- carrier lookup table, since IATA code isn't cleanly available there
-- (see data_quality_notes.md) — it's the identifier most commonly
-- used in industry conversation, so worth surfacing despite coming
-- from a different source than the rest of this dimension.
--
-- Only carriers with flight data in the loaded date range get an IATA
-- code populated; carriers present in the lookup table but absent
-- from stg_flights will have a null carrier_iata_code.

with carrier_iata_codes as (
    select distinct
        carrier_code,
        carrier_iata_code

    from {{ ref('stg_flights') }}
)

select
    sc.carrier_code,
    cic.carrier_iata_code,
    sc.carrier_name

from {{ ref('stg_carriers') }} as sc
left join carrier_iata_codes as cic
    on sc.carrier_code = cic.carrier_code
