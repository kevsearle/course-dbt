{{
  config(
    materialized='table'
  )
}}

with source as (
		select * from {{ source('postgres', 'users') }}
)

, renamed_recast as (
	select 
		user_id AS user_guid
       ,first_name
       ,last_name
       ,email
       ,phone_number
       ,created_at
       ,updated_at
       ,address_id AS address_guid
	FROM source
)

SELECT * FROM renamed_recast


