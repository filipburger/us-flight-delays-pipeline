-- Resolves DepartureDelayGroups / ArrivalDelayGroups: 15-minutes delay bands
-- from -15 min early to > 180 min late

select
    Code as delay_group_code,
    Description as delay_group_label

from {{ source("raw_bts", "l_ontime_delay_groups") }}
