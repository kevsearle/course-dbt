{% macro day_of_week(date_column) %}

    SELECT date_column from {{ this }}

/*  select {{ date_column }},
         case date_part('dow', {{ date_column }}::date)
           when 0 then 'Sunday'
           when 1 then 'Monday'
           when 2 then 'Tuesday'
           when 3 then 'Wednesday'
           when 4 then 'Thursday'
           when 5 then 'Friday'
           when 6 then 'Saturday'
         end as day_of_week
  from {{ this }}
*/


{% endmacro %}

