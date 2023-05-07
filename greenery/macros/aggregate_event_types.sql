{% macro aggregate_event_types() %}

    {% set event_type_list = dbt_utils.get_column_values (table= ref('stg_postgres__events'), column = 'event_type')  %}

    {% for event_type in event_type_list %}
        ,SUM(CASE WHEN event_type = '{{ event_type }}' THEN 1 ELSE 0 END) AS macro_generated_{{ event_type }}_count
    {% endfor %}

{% endmacro %}


