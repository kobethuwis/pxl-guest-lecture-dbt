-- Staging model for visitors from VRM system
-- TODO: Fix date/time casting and add source_system

with source as (
    select * from {{ source('vrm_system', 'visitors') }}
),

renamed as (
    select
        visitor_id as vrm_visitor_id,
        ticket_id as vrm_ticket_id,
        visit_date as vrm_visit_date,  -- TODO: Cast to date
        entry_time as vrm_entry_time,  -- TODO: Cast to time
        exit_time as vrm_exit_time,    -- TODO: Cast to time
        visitor_type as vrm_visitor_type,
        group_size as vrm_group_size,
        total_spent_usd as vrm_total_spent_usd
        -- TODO: Add source_system = 'VRM'
    from source
)

select * from renamed
