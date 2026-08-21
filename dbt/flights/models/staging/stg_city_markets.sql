-- Consolidates airports serving same metro are (e.g. JFK/LGA/EWR all
-- map to a single Ney York city market), joined via OriginCityMarketID /
-- DestCityMarketID

select
    Code as city_market_id,
    Description as city_market_name
from {{ source('raw_bts', 'l_city_market_id') }}
