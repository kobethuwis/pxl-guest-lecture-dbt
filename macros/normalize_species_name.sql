{% macro normalize_species_name(column_name) %}
    -- Uppercase, spaces to underscores, strip anything that is not letter, digit, or underscore
    regexp_replace(
        replace(upper({{ column_name }}), ' ', '_'),
        '[^[:alnum:]_]',
        '',
        'g'
    )
{% endmacro %}
