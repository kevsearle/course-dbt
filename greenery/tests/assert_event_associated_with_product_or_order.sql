SELECT *
FROM {{ ref('stg_postgres__events') }}
WHERE (order_guid IS NOT NULL AND product_guid IS NOT NULL)
OR (order_guid IS NULL AND product_guid IS NULL)
