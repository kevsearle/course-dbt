{{
  config(
    materialised = 'table'
  )
}}

WITH user AS (
  SELECT * FROM {{ ref('stg_postgres__users') }}
)

, address AS (
  SELECT * FROM {{ ref('stg_postgres__addresses') }}
)

, final AS (
SELECT u.user_guid
      ,u.first_name
      ,u.last_name
      ,CONCAT(u.last_name, ', ', u.first_name) AS full_name
      ,u.email
      ,u.phone_number
      ,u.created_at
      ,u.updated_at
      ,u.address_guid
      ,a.address
      ,a.state
      ,a.zip_code
      ,a.country
FROM user u
LEFT JOIN address a ON a.address_guid = u.address_guid
)

SELECT * FROM final
