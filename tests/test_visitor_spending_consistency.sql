-- Test: Visitor spending consistency
-- Verifies that total_spent matches ticket_price * group_size (within $1 tolerance)
-- and that visitor_type matches ticket_type

{{ config(
    warn_if = "> 0",
    error_if = "> 10"
) }}

select
    v.vrm_visitor_id,
    v.vrm_ticket_id,
    v.vrm_total_spent_usd,
    v.vrm_group_size,
    v.vrm_visitor_type,
    t.ticket_id,
    t.ticket_type,
    t.price_usd,
    (t.price_usd * v.vrm_group_size) as expected_total_spent
from {{ ref('stg_vrm_visitors') }} v
inner join {{ source('vrm_system', 'tickets') }} t on v.vrm_ticket_id = t.ticket_id
where
    abs(v.vrm_total_spent_usd - (t.price_usd * v.vrm_group_size)) > 1.0
    or v.vrm_visitor_type != t.ticket_type
