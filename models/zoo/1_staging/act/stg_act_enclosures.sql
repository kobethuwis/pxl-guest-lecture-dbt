-- Staging model for enclosures from ACT system
-- This model is complete and ready to use!

with source as (
    select * from {{ source('act_system', 'enclosures') }}
),

renamed as (
    select
        enclosure_id as act_enclosure_id,
        name as act_enclosure_name,
        habitat_type as act_habitat_type,
        capacity as act_capacity,
        temperature_celsius as act_temperature_celsius,
        humidity_percent as act_humidity_percent,
        'ACT' as source_system
    from source
)

select * from renamed
