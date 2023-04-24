{{
  config(
    materialised = 'table'
  )
}}

WITH events AS (
  SELECT * FROM {{ ref('stg_postgres__events') }}
)

, final AS (
    SELECT event_guid
          ,session_guid
          ,user_guid
          ,page_url
          ,created_at
          ,DATE(created_at) created_at_date
          ,product_guid
    FROM events
    WHERE event_type = 'page_view'
)

SELECT * FROM final
