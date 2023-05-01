{% macro grant(role) %}

    GRANT SELECT ON {{ this }} TO {{ role }};

{% endmacro %}
