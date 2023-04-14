{{
  config(
    materialized='table'
  )
}}

with source as (
		select * from {{ source('postgres', 'orders') }}
)

, renamed_recast as (
	select 
		order_id AS order_guid
	   ,user_id AS user_guid
	   ,promo_id AS promo_type_name
	   ,address_id as address_guid
	   ,created_at
	   ,order_cost
	   ,shipping_cost
	   ,order_total
	   ,tracking_id AS tracking_guid
	   ,shipping_service AS shipping_service_name
	   ,estimated_delivery_at
	   ,delivered_at
	   ,status as status_name
	from source
)

select * from renamed_recast
