-- Staging model for weather data from seeds (complete)

with source as (
    select * from {{ ref('weather_data') }}
),

renamed as (
    select
        cast(date as date) as weather_date,
        cast(temperature_celsius as decimal(5,2)) as temperature_celsius,
        cast(humidity_percent as decimal(5,2)) as humidity_percent,
        cast(precipitation_mm as decimal(5,2)) as precipitation_mm,
        weather_condition,
        'WEATHER' as source_system
    from source
)

select * from renamed
