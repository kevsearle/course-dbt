{{
  config(
    materialized='table'
  )
}}

with source as (
		select * from {{ source('postgres', 'order_items') }}
)

, renamed_recast as (
	select 
		order_id AS order_guid
	   ,product_id AS product_guid
	   ,quantity
	FROM source
)

SELECT * FROM renamed_recast
