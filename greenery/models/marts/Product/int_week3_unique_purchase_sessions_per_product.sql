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
    SELECT * FROM {{ ref('stg_postgres__order_items')}}
)

, final AS (
    select oi.product_guid
          ,COUNT(DISTINCT e.session_guid) count_distinct_purchases
    from events e
    JOIN  orders o ON o.order_guid = e.order_guid
    JOIN  order_items oi ON oi.order_guid = o.order_guid
    WHERE event_type = 'checkout'
    GROUP BY oi.product_guid
)

SELECT * FROM final