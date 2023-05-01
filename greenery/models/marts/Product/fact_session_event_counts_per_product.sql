{{
  config(
    materialised = 'table'
  )
}}

WITH unique_view_sessions_per_product AS (
  SELECT * FROM {{ ref('int_week3_unique_view_sessions_per_product') }}
)
, unique_purchase_sessions_per_product AS (
  SELECT * FROM {{ ref('int_week3_unique_purchase_sessions_per_product') }}
)

, final AS (
    SELECT nvl(ps.product_guid, vs.product_guid) AS product_guid
        ,count_distinct_purchases
        ,count_distinct_views
        ,count_distinct_purchases / count_distinct_views conversion_rate
    FROM unique_purchase_sessions_per_product ps
    FULL OUTER JOIN unique_view_sessions_per_product vs ON vs.product_guid = ps.product_guid
)

SELECT * FROM final


