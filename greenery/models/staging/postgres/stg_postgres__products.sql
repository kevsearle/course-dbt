{{
  config(
    materialized='table'
  )
}}

with source as (
		select * from {{ source('postgres', 'products') }}
)

, renamed_recast as (
	select 
		product_id AS product_guid
	   ,name
       ,price
       ,inventory
	FROM source
)

SELECT * FROM renamed_recast
