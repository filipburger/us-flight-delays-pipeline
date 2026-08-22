-- Resolves DepartureDelayGroups / ArrivalDelayGroups: 15-minutes delay bands
-- from -15 min early to > 180 min late

with delay_groups as (

    select * from {{ ref('stg_delay_groups') }}

)

select
    delay_group_code,
    delay_group_label

from delay_groups
