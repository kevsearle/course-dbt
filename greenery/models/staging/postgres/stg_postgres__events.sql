{{
  config(
    materialized='table'
  )
}}

with source as (
		select * from {{ source('postgres', 'events') }}
)

, renamed_recast as (
	select 
		event_id AS event_guid
	   ,session_id AS session_guid
	   ,user_id AS user_guid
	   ,page_url
	   ,created_at
	   ,event_type
	   ,order_id AS order_guid
	   ,product_id AS product_guid
	FROM source
)

SELECT * FROM renamed_recast