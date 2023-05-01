{{
  config(
    materialised = 'table'
  )
}}

WITH events AS (
  SELECT * FROM {{ ref('stg_postgres__events') }}
)

, final AS (
    SELECT 
         DATE(created_at) AS created_at_date
        ,product_guid
        ,COUNT(DISTINCT session_guid) AS distinct_session_count
        ,COUNT(DISTINCT user_guid) AS distinct_user_count
        ,COUNT(DISTINCT order_guid) AS distinct_order_count
        ,SUM(CASE WHEN event_type = 'add_to_cart'     THEN 1 ELSE 0 END) AS add_to_cart_count
        ,SUM(CASE WHEN event_type = 'checkout'        THEN 1 ELSE 0 END) AS checkout_count
        ,SUM(CASE WHEN event_type = 'package_shipped' THEN 1 ELSE 0 END) AS package_shipped_count
        ,SUM(CASE WHEN event_type = 'page_view'       THEN 1 ELSE 0 END) AS page_view_count

        {{ aggregate_event_types() }}

    FROM events
    WHERE product_guid IS NOT NULL -- some events are not related to products
    GROUP BY 
         created_at_date
        ,product_guid
)
SELECT * FROM final
