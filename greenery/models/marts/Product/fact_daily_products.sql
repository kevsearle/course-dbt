{{
  config(
    materialised = 'table'
  )
}}

WITH daily_events_by_product AS (
  SELECT * FROM {{ ref('int_daily_events_by_product') }}
)

, daily_orders_by_product AS (
    SELECT * FROM {{ ref('int_daily_orders_by_product') }}
)

, final AS (
    SELECT IFNULL(ep.created_at_date, op.created_at_date) created_at_date
          ,IFNULL(ep.product_guid, op.product_guid) product_guid
          ,ep.distinct_session_count
          ,ep.distinct_user_count
          ,ep.distinct_order_count
          ,ep.add_to_cart_count
          ,ep.checkout_count
          ,ep.package_shipped_count
          ,ep.page_view_count
          ,op.order_quantity
    FROM daily_events_by_product ep
    FULL OUTER JOIN daily_orders_by_product op ON op.created_at_date = ep.created_at_date
        AND op.product_guid = ep.product_guid
)

SELECT * FROM final