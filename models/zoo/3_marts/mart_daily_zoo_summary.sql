-- Mart: Daily zoo operations summary

with daily_operations as (
    select * from {{ ref('int_daily_operations') }}
),

animal_care as (
    select * from {{ ref('int_animal_enclosures') }}
),

daily_aggregated as (
    select
        operation_date,
        count(distinct vrm_visitor_id) as total_visitors,
        sum(vrm_total_spent_usd) as total_revenue_usd,
        avg(vrm_total_spent_usd) as avg_visitor_spending_usd,
        max(temperature_celsius) as max_temperature_celsius,
        min(temperature_celsius) as min_temperature_celsius,
        avg(temperature_celsius) as avg_temperature_celsius,
        max(precipitation_mm) as max_precipitation_mm,
        mode(weather_condition) as most_common_weather_condition
    from daily_operations
    group by operation_date
),

animal_counts as (
    select
        count(distinct act_animal_id) as total_animals_in_care,
        count(distinct act_enclosure_name) as total_enclosures_occupied
    from animal_care
)

select
    daily_aggregated.operation_date,
    daily_aggregated.total_visitors,
    daily_aggregated.total_revenue_usd,
    daily_aggregated.avg_visitor_spending_usd,
    daily_aggregated.max_temperature_celsius,
    daily_aggregated.min_temperature_celsius,
    daily_aggregated.avg_temperature_celsius,
    daily_aggregated.max_precipitation_mm,
    daily_aggregated.most_common_weather_condition,
    animal_counts.total_animals_in_care,
    animal_counts.total_enclosures_occupied,
    {{ calculate_visitor_capacity('daily_aggregated.total_visitors', '1000') }} as visitor_capacity_utilization_percent
from daily_aggregated
cross join animal_counts
