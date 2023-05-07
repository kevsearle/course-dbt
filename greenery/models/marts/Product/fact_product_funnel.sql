{{
  config(
    materialised = 'table'
  )
}}

WITH events AS (
  SELECT * FROM {{ ref('stg_postgres__events') }}
)
, orders AS (
  SELECT * FROM {{ ref('stg_postgres__orders') }}
)
, order_items AS (
  SELECT * FROM {{ ref('stg_postgres__order_items') }}
)
, final AS (
  WITH product_checkouts AS (
      SELECT oi.product_guid
            ,COUNT(DISTINCT e.session_guid) distinct_product_checkout_sessions
      FROM events e
      JOIN orders o ON o.order_guid = e.order_guid AND e.event_type = 'checkout'
      JOIN order_items oi ON oi.order_guid = o.order_guid
      GROUP BY oi.product_guid
  )
  , product_pv_atc AS (
      SELECT product_guid
            ,COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN session_guid ELSE null END) distinct_product_page_view_sessions
            ,COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN session_guid ELSE null END) distinct_product_add_to_cart_sessions
      FROM  events
      GROUP BY product_guid
  )
  SELECT COALESCE(p1.product_guid, p2.product_guid) AS product_guid
        ,distinct_product_page_view_sessions
        ,distinct_product_add_to_cart_sessions
        ,distinct_product_checkout_sessions
        ,DIV0((distinct_product_page_view_sessions - distinct_product_add_to_cart_sessions), distinct_product_page_view_sessions)*100 AS page_view_to_cart_drop_off_rate
        ,DIV0((distinct_product_add_to_cart_sessions - distinct_product_checkout_sessions), distinct_product_add_to_cart_sessions)*100 AS cart_to_checkout_drop_off_rate
        ,DIV0((distinct_product_page_view_sessions - distinct_product_checkout_sessions), distinct_product_page_view_sessions)*100 AS page_view_to_checkout_drop_off_rate
  FROM product_checkouts p1
  FULL OUTER JOIN product_pv_atc p2 ON p1.product_guid = p2.product_guid
  ORDER BY page_view_to_checkout_drop_off_rate desc)

SELECT * FROM final


