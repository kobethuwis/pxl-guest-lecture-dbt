-- Intermediate model: visitors + weather

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
        visitors.vrm_total_spent_usd,
        weather.temperature_celsius,
        weather.humidity_percent,
        weather.precipitation_mm,
        weather.weather_condition
    from visitors
    left join weather on visitors.vrm_visit_date = weather.weather_date
)

select * from joined
