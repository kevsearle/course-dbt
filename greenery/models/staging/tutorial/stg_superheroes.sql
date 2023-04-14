/*

{{
  config(
    materialized='table'
  )
}}

SELECT 
    id AS superhero_id,
    name,
    gender,
    eye_color,
    race,
    hair_color,
    CASE
      when height < 0 THEN 1
      else height
    end as height,
    publisher,
    skin_color,
    alignment,
    CASE 
      when weight < 0 then 1
      else weight
    end as weight
FROM {{ source('tutorial', 'superheroes') }}

*/

{{
  config(
    materialized='table'
  )
}}

SELECT 
    id AS superhero_id,
    name,
    gender,
    eye_color,
    race,
    hair_color,
    NULLIF(height, -99) AS height,
    publisher,
    skin_color,
    alignment,
    NULLIF(weight, -99) AS weight_lbs,
    {{ lbs_to_kgs('weight') }} AS weight_kg
FROM {{ source('tutorial', 'superheroes') }}


