-- Staging model for visitors from VRM system

with source as (
    select * from {{ source('vrm_system', 'visitors') }}
),

renamed as (
    select
        visitor_id as vrm_visitor_id,
        ticket_id as vrm_ticket_id,
        cast(visit_date as date) as vrm_visit_date,
        cast(entry_time as time) as vrm_entry_time,
        cast(exit_time as time) as vrm_exit_time,
        visitor_type as vrm_visitor_type,
        group_size as vrm_group_size,
        total_spent_usd as vrm_total_spent_usd,
        'VRM' as source_system
    from source
)

select * from renamed
