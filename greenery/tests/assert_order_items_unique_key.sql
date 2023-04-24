SELECT order_guid
      ,product_guid
FROM {{ ref('stg_postgres__order_items') }}
GROUP BY 
       order_guid
      ,product_guid
HAVING COUNT(*) > 1