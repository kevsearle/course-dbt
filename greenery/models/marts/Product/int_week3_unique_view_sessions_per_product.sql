{{
  config(
    materialised = 'table'
  )
}}

WITH events AS (
  SELECT * FROM {{ ref('stg_postgres__events') }}
)
, products AS (
  SELECT * FROM {{ ref('stg_postgres__products') }}
)

, final AS (
    SELECT p.product_guid
          ,COUNT(DISTINCT e.session_guid) count_distinct_views
    FROM   events e
    JOIN   products p ON p.product_guid = e.product_guid 
    WHERE event_type = 'page_view'
    GROUP BY p.product_guid
)

SELECT * FROM final
