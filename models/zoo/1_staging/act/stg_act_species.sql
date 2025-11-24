-- Staging model for species from ACT system (complete)

with source as (
    select * from {{ source('act_system', 'species') }}
),

renamed as (
    select
        species_id as act_species_id,
        common_name as act_common_name,
        scientific_name as act_scientific_name,
        habitat_type as act_habitat_type,
        diet_type as act_diet_type,
        average_lifespan_years as act_average_lifespan_years,
        'ACT' as source_system
    from source
)

select * from renamed
