{{
  config(
    materialised = 'table'
  )
}}

WITH orders AS (
  SELECT * FROM {{ ref('stg_postgres__orders') }}
)

, order_items AS (
    SELECT * FROM {{ ref('stg_postgres__order_items')}}
)

, final AS (
    SELECT
        DATE(o.created_at) created_at_date
       ,oi.product_guid
       ,SUM(oi.quantity) AS order_quantity
    FROM orders o
    JOIN order_items oi ON oi.order_guid = o.order_guid
    GROUP BY DATE(o.created_at)
       ,oi.product_guid
)

SELECT * FROM final