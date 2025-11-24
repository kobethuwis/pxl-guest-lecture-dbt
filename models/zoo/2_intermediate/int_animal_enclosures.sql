-- Intermediate model: animals + enclosures + species
-- TODO: Complete joins and use normalize_species_name macro

with animals as (
    select * from {{ ref('stg_act_animals') }}
),

enclosures as (
    select * from {{ ref('stg_act_enclosures') }}
),

species as (
    select * from {{ ref('stg_act_species') }}
),

joined as (
    select
        animals.act_animal_id,
        animals.act_name,
        animals.act_health_status
        -- TODO: Join enclosures and species, use normalize_species_name macro
    from animals
    -- TODO: LEFT JOIN enclosures, LEFT JOIN species
)

select * from joined
