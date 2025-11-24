-- Test: Feeding schedule validity (complete)
-- Checks: times 6:00-20:00, positive quantities, non-null food_type

{{ config(
    warn_if = "> 0",
    error_if = "> 10"
) }}

select
    schedule_id,
    animal_id,
    feeding_time,
    quantity_kg,
    food_type
from {{ source('act_system', 'feeding_schedules') }}
where
    (cast(feeding_time as time) < cast('06:00:00' as time)
     or cast(feeding_time as time) > cast('20:00:00' as time))
    or quantity_kg <= 0
    or food_type is null
