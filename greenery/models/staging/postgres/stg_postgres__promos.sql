{{
  config(
    materialized='table'
  )
}}

with source as (
		select * from {{ source('postgres', 'promos') }}
)

, renamed_recast as (
	select 
		promo_id AS promo_type
       ,discount
       ,status
	FROM source
)

SELECT * FROM renamed_recast

