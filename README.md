# Analytics engineering with dbt

Template repository for the projects and environment of the course: Analytics engineering with dbt

> Please note that this sets some environment variables so if you create some new terminals please load them again.

## License
GPL-3.0

-------------------------
--  WEEK 1
-------------------------

--How many users do we have?

SELECT COUNT(*)
FROM stg_postgres_users;

--Answer: 130

--On average, how many orders do we receive per hour?

WITH created_during_hour AS (
    SELECT created_at
          ,CONCAT(year(created_at), MONTH(created_at), DAY(created_at), hour(created_at)) AS YMDH
    FROM stg_postgres__orders
)
, orders_per_hour AS (
    SELECT ymdh
          ,COUNT(*) order_count
    FROM created_during_hour
    GROUP BY ymdh
)
select SUM(order_count)/count(*) as average_orders_per_hour
from orders_per_hour;

-- Answer 7.53

--On average, how long does an order take from being placed to being delivered?

WITH elapsed_delivery AS (
    SELECT DATEDIFF('hours', created_at, delivered_at)  x
    FROM stg_postgres__orders a
    WHERE delivered_at IS NOT NULL
)
SELECT sum(x)/count(*)
FROM elapsed_delivery;

-- Answer 93 hours

--How many users have only made one purchase? Two purchases? Three+ purchases?
-- Note: you should consider a purchase to be a single order. In other words, if a user places one order for 3 products, they are considered to have made 1 purchase.

SELECT user_purchase_count
      ,COUNT(*) number_of_users
FROM (
    SELECT user_guid
          ,COUNT(*) user_purchase_count
    FROM stg_postgres__orders
    GROUP BY user_guid
)
GROUP BY user_purchase_count
ORDER BY user_purchase_count;

/*
Answer: 
USER_PURCHASE_COUNT	
    NUMBER_OF_USERS
1	25
2	28
3	34
4	20
5	10
6	2
7	4
8	1
*/

-- On average, how many unique sessions do we have per hour?

WITH sess_hour AS (
    SELECT DISTINCT
           CONCAT(year(created_at), MONTH(created_at), DAY(created_at), hour(created_at)) AS distinct_hour
          ,session_guid
    FROM   stg_postgres__events
) 
, dist_sess_in_hour AS (
    SELECT distinct_hour
          ,COUNT(*) distinct_sess
    FROM sess_hour
    GROUP BY distinct_hour
)
SELECT SUM(distinct_sess)/COUNT(*)
FROM dist_sess_in_hour
;

--Answer 16.32

-------------------------
--  WEEK 2
-------------------------

/*
|| What is our user repeat rate?
||
|| Repeat Rate = Users who purchased 2 or more times / users who purchased
*/

use dev_db.dbt_kevinsearlesonycom;

WITH user_order_count AS (
    SELECT user_guid
          ,COUNT(*) order_count
    FROM   stg_postgres__orders
    GROUP BY user_guid
), order_count_user_count AS (
    SELECT order_count
          ,COUNT(*) user_count
    FROM user_order_count
    GROUP BY order_count
)
SELECT (SELECT SUM(user_count) FROM order_count_user_count WHERE order_count >= 2) /
       (SELECT SUM(user_count) FROM order_count_user_count) AS "Repeat Rate"
;

-- Repeate Rate = 0.798387

-- slightly improved sql method 

WITH user_order_count AS (
    SELECT user_guid
          ,COUNT(*) order_count
    FROM   stg_postgres__orders
    GROUP BY user_guid
)
, user_order_buckets AS (
    SELECT user_guid
          ,(order_count = 1)::int has_one_order
          ,(order_count = 2)::int has_two_orders
          ,(order_count = 3)::int has_three_orders
          ,(order_count >= 2)::int has_multi_orders
    FROM user_order_count
)
SELECT SUM(has_one_order) AS user_with_one_order_count
        ,SUM(has_two_orders) AS user_with_two_orders_count
        ,SUM(has_three_orders) AS user_with_three_orders_count
        ,SUM(has_multi_orders) AS user_with_multi_orders_count
        ,COUNT(*) AS total_users
        ,div0(user_with_multi_orders_count, total_users) AS repeat_rate
FROM user_order_buckets
;

/*
What are good indicators of a user who will likely purchase again? What about indicators of users who are likely NOT to purchase again? If you had more data, what features would you want to look into to answer this question?

NOTE: This is a hypothetical question vs. something we can analyze in our Greenery data set. Think about what exploratory analysis you would do to approach this question.
*/

/*
Answer
----------

Indicators of likely repeat purchase:
  - users who make multiple return visits to the site as measured by page_view event_type in different sessions.
  - users who progress further down the sales funnel but don't actually make the order.(event_type = add_to_cart) 
  - users who purchase regularly
  - 

Indicators of unlikely repeat purchase
  - users who don't have further page_view events on the site.
  - users who have only ordered once.
  - users who have cancelled orders.
  - 

*/

/*
Explain the product mart models you added. Why did you organize the models in the way you did?


We're looking for information about products rolled up to a daily level, so I aggregated the fact table
at this level which would make the queries more performant. )

I used intermediate queries to roll up the event data and the order item data seperately and then fed both 
of those intermediate tables into a final fact table - both at the same grain of product/day.

I created a dim_user table in core which combined the basic stg_user data with stg_address data since
the data is organised so that a user can only ever have one address.  So we might as well combine 
to minimise downstream joins.

I put in fact_product table which rolled up date from the fact_daily_product table to provide average values
for some of the attributes (eg. average_product_daily_page_views and average_product_daily_orders.  I'm pretty 
sure this is not really required and the insight would be provided within the BI tool, but I've left it in anyway.

I was unsure whether you could/should actually put some of those attributes on dim_product rather than fact_product.

*/

/*
Tests

I added some basic (pre-built) tests to ensure that expected primary keys were unique and not null.

I added some simple tests into the test folder to test that 
    event had a foreign key to either product or orders but not both.
    order items quantity greater than 0
    order items was unique on order_id and product_id
some of these can be replaced by macros and/or tests from packages going forward.

I tried to add multi column uniqueness tests but I couldn't get the syntax to work and have run out of time.
(perhaps I need to add a package to do this?)
*/

/*
Snapshots
*/

SELECT DISTINCT dbt_updated_at, dbt_valid_from, dbt_valid_to FROM products_snapshot;
-- There are only 2 distinct dates in the table

SELECT *
FROM   products_snapshot
ORDER BY PRODUCT_ID, DBT_UPDATED_AT;

WITH updated_products_origin AS (
    SELECT *
    FROM   products_snapshot
    WHERE dbt_valid_to IS NOT NULL
)
, updated_products_new AS (
    SELECT *
    FROM   products_snapshot
    WHERE product_id IN (
        SELECT product_id
        FROM updated_products_origin
    )
    AND dbt_valid_to IS NULL
)
SELECT o.product_id
FROM updated_products_origin o
JOIN updated_products_new n ON n.product_id = o.product_id
WHERE o.inventory != n.inventory
;

/*
These 4 products have had the inventory changed

4cda01b9-62e2-46c5-830f-b7f262a58fb1
55c6a062-5f4a-4a8b-a8e5-05ea5e6715a3
be49171b-9f72-4fc9-bf7a-9a52e259836b
fb0e8be7-5ac4-4a76-a1fa-2cc4bf0b2d80
*/


# Week 3 Submission

Just using SQL from the existing models could get the conversion rates like this:

WITH ps as (
    -- Unique purchase sessions per product
    select oi.product_guid
          ,COUNT(DISTINCT e.session_guid) count_distinct_purchases
    from stg_postgres__events e
    JOIN  stg_postgres__orders o ON o.order_guid = e.order_guid
    JOIN  stg_postgres__order_items oi ON oi.order_guid = o.order_guid
    WHERE event_type = 'checkout'
    GROUP BY oi.product_guid
)
, vs AS (
-- Unique view sessions per product
    select p.product_guid
          ,COUNT(DISTINCT e.session_guid) count_distinct_views
    from stg_postgres__events e
    JOIN  stg_postgres__products p ON p.product_guid = e.product_guid 
    WHERE event_type = 'page_view'
    GROUP BY p.product_guid
)
SELECT nvl(ps.product_guid, vs.product_guid) AS product_guid
      ,count_distinct_purchases
      ,count_distinct_views
      ,count_distinct_purchases / count_distinct_views conversion_rate
FROM ps
FULL OUTER JOIN vs ON vs.product_guid = ps.product_guid
;
























