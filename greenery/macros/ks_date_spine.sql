{% macro date_spine(start_date, end_date) %} 

with recursive date_spine as ( 
  select '{{ start_date }}'::date as date 
  union all 
  select date + interval '1 day' 
  from date_spine 
  where date < '{{ end_date }}'::date 
) 
select date 
from date_spine 

{% endmacro %}
