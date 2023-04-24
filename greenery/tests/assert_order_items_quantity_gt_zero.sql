SELECT *
FROM {{ ref('stg_postgres__order_items') }}
WHERE quantity < 1