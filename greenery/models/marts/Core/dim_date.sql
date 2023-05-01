{{
  config(
    materialised = 'table'
  )
}}

{{ dbt_date.get_date_dimension('01/01/2018', '01/01/2024') }}

