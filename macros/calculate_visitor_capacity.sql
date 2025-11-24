{% macro calculate_visitor_capacity(visitor_count_column, max_capacity) %}
    -- Calculate visitor capacity utilization percentage
    -- Formula: (visitor_count / max_capacity) * 100
    round(
        (cast({{ visitor_count_column }} as decimal(10,2)) / nullif(cast({{ max_capacity }} as decimal(10,2)), 0)) * 100.0,
        2
    )
{% endmacro %}
