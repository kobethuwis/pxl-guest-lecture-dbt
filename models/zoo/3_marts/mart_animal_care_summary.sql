-- Mart: Animal care summary by species

with animal_enclosures as (
    select * from {{ ref('int_animal_enclosures') }}
),

species_summary as (
    select
        act_species_id,
        act_common_name,
        {{ normalize_species_name('act_common_name') }} as normalized_species_name,
        count(distinct act_animal_id) as animal_count,
        count(distinct case when act_health_status = 'Healthy' then act_animal_id end) as healthy_animals
    from animal_enclosures
    group by act_species_id, act_common_name
)

select
    normalized_species_name,
    act_common_name,
    animal_count,
    healthy_animals,
    round((healthy_animals::decimal / nullif(animal_count, 0)) * 100.0, 2) as health_rate_percent
from species_summary
order by animal_count desc
