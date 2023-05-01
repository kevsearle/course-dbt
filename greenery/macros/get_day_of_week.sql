{% macro get_day_of_week(date) %}
   date_part('dow', '{{ date }}::DATE')
{% endmacro %}

