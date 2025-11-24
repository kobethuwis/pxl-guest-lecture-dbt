-- Mart: Daily zoo operations summary
-- TODO: Complete aggregation and add calculate_visitor_capacity macro

{{ config(
    enabled=false
) }}

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
        sum(vrm_total_spent_usd) as total_revenue_usd
        -- TODO: Add avg_visitor_spending_usd, weather aggregations
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
    animal_counts.total_animals_in_care,
    animal_counts.total_enclosures_occupied
    -- TODO: Add visitor_capacity_utilization_percent using calculate_visitor_capacity macro
from daily_aggregated
cross join animal_counts
