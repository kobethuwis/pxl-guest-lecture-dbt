-- Daily Zoo Report - Updated 2023-12-15 by Sarah
-- Modified 2024-01-03 by Mike - added weather
-- Fixed by John 2024-01-10 - removed duplicate visitors
-- TODO: Need to add animal count - Sarah 2024-01-20
-- URGENT FIX: Changed date filter per management request - Mike 2024-01-25

-- Get all visitors first
WITH all_visitors AS (
    SELECT 
        visitor_id,
        visit_date,
        total_spent_usd,
        visitor_type,
        -- entry_time,  -- Not needed anymore, removed 2024-01-10
        exit_time,
        group_size
        -- ticket_id,  -- Commented out, might need later
    FROM vrm_system.visitors
    WHERE visit_date >= '2024-01-01'  -- Changed from '2023-12-01' per request
    -- AND visitor_type != 'Staff'  -- Removed this filter, not sure why it was here
),

-- Weather stuff - added this later
weather_info AS (
    SELECT 
        date,
        temperature_celsius,
        -- humidity_percent,  -- Don't need this
        precipitation_mm,
        weather_condition
    FROM weather_data
    WHERE date >= '2024-01-01'
),

-- This was for something else but leaving it here just in case
-- visitor_types AS (
--     SELECT DISTINCT visitor_type FROM vrm_system.visitors
-- ),

-- Animal count - added this CTE but not sure if it's right
animal_data AS (
    SELECT 
        animal_id,
        name,
        species_id,
        -- enclosure_id,  -- Don't need this
        health_status,
        birth_date
    FROM act_system.animals
    -- WHERE health_status = 'Healthy'  -- Removed filter, need all animals
),

-- Revenue calculation - this might be wrong
revenue_calc AS (
    SELECT 
        visit_date,
        SUM(total_spent_usd) as daily_revenue,
        COUNT(*) as visitor_count
        -- AVG(total_spent_usd) as avg_revenue  -- Moved to main query
    FROM all_visitors
    GROUP BY visit_date
),

-- This CTE doesn't seem to be used anywhere but Sarah said to keep it
-- Maybe for future use?
-- unused_enclosures AS (
--     SELECT enclosure_id, name FROM act_system.enclosures
-- ),

-- Main query - this is what we actually use
SELECT 
    v.visit_date as report_date,
    -- v.visit_date,  -- Duplicate, keeping both for now
    COUNT(DISTINCT v.visitor_id) as total_visitors,
    -- COUNT(v.visitor_id) as total_visitors_old,  -- Old way, keeping for comparison
    SUM(v.total_spent_usd) as total_revenue,
    -- SUM(v.total_spent_usd) / COUNT(DISTINCT v.visitor_id) as manual_avg,  -- Alternative calc
    AVG(v.total_spent_usd) as avg_spending,
    -- MAX(v.total_spent_usd) as max_spending,  -- Not needed but might be useful
    -- MIN(v.total_spent_usd) as min_spending,  -- Also not needed
    w.temperature_celsius,
    -- w.humidity_percent,  -- Removed, not in report
    w.precipitation_mm,  -- Added this 2024-01-15
    w.weather_condition,
    -- COUNT(DISTINCT a.animal_id) as total_animals,  -- This doesn't work, need to fix
    (SELECT COUNT(*) FROM animal_data) as total_animals,  -- Quick fix, probably wrong
    -- v.visitor_type,  -- Don't need in final output
    -- r.daily_revenue,  -- Already have total_revenue above
    COUNT(DISTINCT CASE WHEN v.visitor_type = 'Adult' THEN v.visitor_id END) as adult_visitors  -- Added per request
    -- COUNT(DISTINCT CASE WHEN v.visitor_type = 'Child' THEN v.visitor_id END) as child_visitors  -- Not needed
FROM all_visitors v
LEFT JOIN weather_info w ON v.visit_date = w.date  -- Changed from INNER JOIN 2024-01-10
-- LEFT JOIN animal_data a ON 1=1  -- This was wrong, removed
LEFT JOIN revenue_calc r ON v.visit_date = r.visit_date  -- Not really using this but keeping it
CROSS JOIN animal_data a  -- This is probably wrong but it works
WHERE v.visit_date >= '2024-01-01'  -- Duplicate filter, but keeping it here too
  -- AND v.total_spent_usd > 0  -- Removed, some refunds might be negative
  AND v.visit_date <= CURRENT_DATE  -- Added this to prevent future dates
GROUP BY 
    v.visit_date, 
    w.temperature_celsius, 
    w.weather_condition,
    w.precipitation_mm  -- Added this when we added the column
    -- w.humidity_percent  -- Removed when we removed the column
ORDER BY v.visit_date DESC
-- LIMIT 100  -- Was testing, removed for production
;

