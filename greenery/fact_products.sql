{{
  config(
    materialised = 'table'
  )
}}

WITH fact_daily_products AS (
  SELECT * FROM {{ ref('fact_daily_products') }}
)

, final AS (
    SELECT product_guid
          ,SUM(page_view_count) AS total_product_page_views
          ,SUM(CASE when page_view_count >= 1 THEN 1 ELSE 0 END) AS total_product_view_days
          ,total_product_page_views / total_product_view_days AS avg_product_daily_page_views
          ,SUM(order_quantity) AS total_product_order_quantity
          ,SUM(CASE WHEN order_quantity >= 1 THEN 1 ELSE 0 END) AS total_product_order_days
          ,total_product_order_quantity / total_product_order_days AS avg_product_daily_orders 
    FROM fact_daily_products
    GROUP BY product_guid
)

SELECT * FROM final