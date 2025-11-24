-- Staging model for events from VRM system
-- TODO: Fix date/time casting and add source_system

with source as (
    select * from {{ source('vrm_system', 'events') }}
),

renamed as (
    select
        event_id as vrm_event_id,
        event_name as vrm_event_name,
        event_date as vrm_event_date,  -- TODO: Cast to date
        start_time as vrm_start_time,  -- TODO: Cast to time
        end_time as vrm_end_time,      -- TODO: Cast to time
        location as vrm_location,
        capacity as vrm_capacity,
        tickets_sold as vrm_tickets_sold,
        revenue_usd as vrm_revenue_usd
        -- TODO: Add source_system = 'VRM'
    from source
)

select * from renamed
