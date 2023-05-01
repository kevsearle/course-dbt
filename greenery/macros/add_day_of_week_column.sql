{% macro add_day_of_week_column(date_column) %}

SELECT
  {{ date_column }},
  CASE
    WHEN date_part('dow', {{ date_column }}::timestamp) = 0 THEN 'Sunday'
    WHEN date_part('dow', {{ date_column }}::timestamp) = 1 THEN 'Monday'
    WHEN date_part('dow', {{ date_column }}::timestamp) = 2 THEN 'Tuesday'
    WHEN date_part('dow', {{ date_column }}::timestamp) = 3 THEN 'Wednesday'
    WHEN date_part('dow', {{ date_column }}::timestamp) = 4 THEN 'Thursday'
    WHEN date_part('dow', {{ date_column }}::timestamp) = 5 THEN 'Friday'
    WHEN date_part('dow', {{ date_column }}::timestamp) = 6 THEN 'Saturday'
  END AS day_of_week

{% endmacro %}

