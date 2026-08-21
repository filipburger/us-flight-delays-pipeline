-- Enables official carrier name

select
    Code as carrier_code,
    Description as carrier_name

from {{ source('raw_bts', 'l_unique_carriers') }}
