-- pulled this from Confluence

SELECT 
    s.common_name,
    COUNT(a.animal_id) as animal_count,
    AVG(CAST(strftime('%Y', CURRENT_DATE) AS INTEGER) - CAST(strftime('%Y', a.birth_date) AS INTEGER)) as avg_age,
    e.capacity,
    COUNT(a.animal_id) * 100.0 / e.capacity as utilization_pct
FROM act_system.animals a
JOIN act_system.species s ON a.species_id = s.species_id
JOIN act_system.enclosures e ON a.enclosure_id = e.enclosure_id
WHERE a.health_status = 'Healthy'
GROUP BY s.common_name, e.capacity
HAVING utilization_pct > 50
ORDER BY animal_count DESC;