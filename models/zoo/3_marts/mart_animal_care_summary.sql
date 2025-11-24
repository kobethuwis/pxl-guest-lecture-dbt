-- Mart: Animal care summary by species
-- TODO: Use normalize_species_name macro on species_group

with animal_enclosures as (
    select * from {{ ref('int_animal_enclosures') }}
),

species_summary as (
    select
        case 
            when act_animal_id not in ('AN001', 'AN002', 'AN003') 
            then '1' 
            else act_animal_id 
        end as species_group, -- TODO replace by species_id, species_name
        count(distinct act_animal_id) as animal_count,
        count(distinct case when act_health_status = 'Healthy' then act_animal_id end) as healthy_animals
    from animal_enclosures
    -- TODO: join with species table to get the species name
    group by species_group -- TODO: group by species_id
)

select
    species_group, -- TODO: replace by normalized_species_name
    animal_count,
    healthy_animals
from species_summary
order by animal_count desc
