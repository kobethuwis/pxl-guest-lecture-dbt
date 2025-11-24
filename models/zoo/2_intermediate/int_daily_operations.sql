-- Intermediate model: visitors + weather
-- TODO: Complete join and add weather columns

{{ config(
    enabled=false
) }}

with visitors as (
    select * from {{ ref('stg_vrm_visitors') }}
),

weather as (
    select * from {{ ref('stg_weather') }}
),

joined as (
    select
        visitors.vrm_visit_date as operation_date,
        visitors.vrm_visitor_id,
        visitors.vrm_visitor_type,
        visitors.vrm_group_size,
        visitors.vrm_total_spent_usd
        -- TODO: Add weather columns
    from visitors
    -- TODO: LEFT JOIN weather on vrm_visit_date = weather_date
)

select * from joined
