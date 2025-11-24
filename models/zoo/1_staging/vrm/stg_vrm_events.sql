-- Staging model for events from VRM system

with source as (
    select * from {{ source('vrm_system', 'events') }}
),

renamed as (
    select
        event_id as vrm_event_id,
        event_name as vrm_event_name,
        cast(event_date as date) as vrm_event_date,
        cast(start_time as time) as vrm_start_time,
        cast(end_time as time) as vrm_end_time,
        location as vrm_location,
        capacity as vrm_capacity,
        tickets_sold as vrm_tickets_sold,
        revenue_usd as vrm_revenue_usd,
        'VRM' as source_system
    from source
)

select * from renamed
