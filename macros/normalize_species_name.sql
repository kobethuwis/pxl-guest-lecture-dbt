{% macro normalize_species_name(column_name) %}
    -- Normalizes species names: uppercase, spaces to underscores, remove special chars
    upper(
        regexp_replace(
            replace({{ column_name }}, ' ', '_'),
            '[^A-Z0-9_]',
            ''
        )
    )
{% endmacro %}

