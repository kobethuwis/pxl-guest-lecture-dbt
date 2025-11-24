-- Staging model for animals from ACT system
-- TODO: Add source_system column

with source as (
    select * from {{ source('act_system', 'animals') }}
),

renamed as (
    select
        animal_id as act_animal_id,
        name as act_name,
        species_id as act_species_id,
        enclosure_id as act_enclosure_id,
        cast(birth_date as date) as act_birth_date,
        cast(arrival_date as date) as act_arrival_date,
        health_status as act_health_status,
        keeper_id as act_keeper_id,
        cast(last_vet_check as date) as act_last_vet_check
        -- TODO: Add source_system = 'ACT'
    from source
)

select * from renamed
