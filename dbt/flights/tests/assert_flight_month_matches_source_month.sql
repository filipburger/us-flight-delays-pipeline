-- tests/assert_flight_month_matches_source_month.sql
--
-- FlightDate is the source of truth for which month a flight belongs to;
-- source_month is which BTS file it was published in. These should always
-- agree — if they don't, a flight was reported in a different month's file
-- than the one it actually occurred in (e.g. a late-reported flight crossing
-- a file boundary). See "Provenance vs. source columns" in
-- docs/data_quality_notes.md.
--
-- This test fails (returns rows) if any mismatch exists.

select 
    flight_date,
    source_year,
    source_month,
    extract(month from flight_date) as actual_month
from {{ ref('stg_flights')}}
where extract(month from flight_date) != source_month
    or extract(year from flight_date) != source_year
