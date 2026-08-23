-- Date dimension. Generated spine, not derived from stg_flights, so it
-- exists independently of what data happens to be loaded — useful for
-- gap analysis and stable regardless of backfill progress.
--
-- Scoped to 1987-01-01 through 2050-12-31: covers the full range BTS
-- could ever provide, plus a wide future buffer. Cost is negligible
-- (~23k rows) since this is table-materialized, a full rebuild is 
-- trivial and correctly picks up any change to the holidays seed
-- (e.g. Juneteenth was onlyadded federally in 2021).

with date_spine as (

    select date_day

    from
        unnest(
            generate_date_array('1987-01-01', '2050-12-31', interval 1 day)
        ) as date_day
),

dates_and_holidays as (

    select
        date_day,
        holiday_date,
        holiday_name

    from date_spine ds
    left join {{ ref('holidays') }} h
        on ds.date_day = h.holiday_date
),

enriched as (

    select
        date_day,

        extract(year from date_day) as year,
        extract(month from date_day) as month,
        format_date('%B', date_day) as month_name,
        extract(day from date_day) as day_of_month,

        -- BigQuery's DAYOFWEEK: 1=Sunday .. 7=Saturday
        extract(dayofweek from date_day) as day_of_week,
        format_date('%A', date_day) as day_name,
        extract(dayofweek from date_day) in (1, 7) as is_weekend,

        case
            when extract(month from date_day) in (12, 1, 2)
                then 'Winter'
            when extract(month from date_day) in (3, 4, 5)
                then 'Spring'
            when extract(month from date_day) in (6, 7, 8)
                then 'Summer'
            when extract(month from date_day) in (9, 10, 11)
                then 'Autumn'
        end as season,

        holiday_name,
        holiday_name is not null as is_holiday

    from dates_and_holidays

)

select * from enriched
