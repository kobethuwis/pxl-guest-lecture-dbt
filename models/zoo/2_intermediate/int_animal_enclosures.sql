-- Intermediate model: animals + enclosures + species

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
        animals.act_health_status,
        animals.act_birth_date,
        animals.act_arrival_date,
        animals.act_keeper_id,
        animals.act_last_vet_check,
        enclosures.act_enclosure_id,
        enclosures.act_enclosure_name,
        enclosures.act_habitat_type as enclosure_habitat_type,
        enclosures.act_capacity,
        enclosures.act_temperature_celsius,
        enclosures.act_humidity_percent,
        species.act_species_id,
        species.act_common_name,
        species.act_scientific_name,
        species.act_habitat_type as species_habitat_type,
        species.act_diet_type,
        species.act_average_lifespan_years,
        {{ normalize_species_name('species.act_common_name') }} as normalized_species_name
    from animals
    left join enclosures on animals.act_enclosure_id = enclosures.act_enclosure_id
    left join species on animals.act_species_id = species.act_species_id
)

select * from joined
