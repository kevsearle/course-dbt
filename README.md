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

I've been a bit more careful with my formatting this week :)

## Part 1 - Create new models to answer the first two questions 

Just using SQL from the existing models could get the conversion rates like this:

```
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
```

I also added in the following models to answer the questions on conversion rate.
I'm still doubtful about the modeling - whether we should actually create a fact table which has the conversion rate pre-calculated or whether this is best left to a BI tool?  Anyway....

```
SELECT * FROM int_week3_unique_purchase_sessions_per_product;
SELECT * FROM int_week3_unique_view_sessions_per_product;
SELECT * FROM fact_session_event_counts_per_product;
```
```
PRODUCT_GUID	                        COUNT_DISTINCT_PURCHASES	
                                            COUNT_DISTINCT_VIEWS	
                                                CONVERSION_RATE
c7050c3b-a898-424d-8d98-ab0aaad7bef4	34	75	0.453333
e18f33a6-b89a-4fbc-82ad-ccba5bb261cc	28	70	0.400000
80eda933-749d-4fc6-91d5-613d29eb126f	31	74	0.418919
689fb64e-a4a2-45c5-b9f2-480c2155624d	36	67	0.537313
bb19d194-e1bd-4358-819e-cd1f1b401c0c	33	78	0.423077
b66a7143-c18a-43bb-b5dc-06bb5d1d3160	34	63	0.539683
4cda01b9-62e2-46c5-830f-b7f262a58fb1	21	61	0.344262
5ceddd13-cf00-481f-9285-8340ab95d06d	33	67	0.492537
fb0e8be7-5ac4-4a76-a1fa-2cc4bf0b2d80	39	64	0.609375
58b575f2-2192-4a53-9d21-df9a0c14fc25	24	61	0.393443
74aeb414-e3dd-4e8a-beef-0fa45225214d	35	63	0.555556
35550082-a52d-4301-8f06-05b30f6f3616	22	45	0.488889
be49171b-9f72-4fc9-bf7a-9a52e259836b	25	49	0.510204
5b50b820-1d0a-4231-9422-75e7f6b0cecf	28	59	0.474576
b86ae24b-6f59-47e8-8adc-b17d88cbd367	27	53	0.509434
e2e78dfc-f25c-4fec-a002-8e280d61a2f2	26	63	0.412698
37e0062f-bd15-4c3e-b272-558a86d90598	29	62	0.467742
55c6a062-5f4a-4a8b-a8e5-05ea5e6715a3	30	62	0.483871
a88a23ef-679c-4743-b151-dc7722040d8c	22	46	0.478261
64d39754-03e4-4fa0-b1ea-5f4293315f67	28	59	0.474576
6f3a3072-a24d-4d11-9cef-25b0b5f8a4af	21	51	0.411765
843b6553-dc6a-4fc4-bceb-02cd39af0168	29	68	0.426471
05df0866-1a66-41d8-9ed7-e2bbcddd6a3d	27	60	0.450000
e8b6528e-a830-4d03-a027-473b411c7f02	29	73	0.397260
615695d3-8ffd-4850-bcf7-944cf6d3685b	32	65	0.492308
d3e228db-8ca5-42ad-bb0a-2148e876cc59	26	56	0.464286
579f4cd0-1f45-49d2-af55-9ab2b72c3b35	28	54	0.518519
c17e63f7-0d28-4a95-8248-b01ea354840e	30	55	0.545455
e5ee99b6-519f-4218-8b41-62f48f59f700	27	66	0.409091
e706ab70-b396-4d30-a6b2-a1ccf3625b52	28	56	0.500000
```

The OVERALL CONVERSION RATE

```
SELECT SUM(count_distinct_purchases) / SUM(count_distinct_views) 
FROM fact_session_event_counts_per_product;

-- 0.457209
```

## Part 2 - We’re getting really excited about dbt macros after learning more about them and want to apply them to improve our dbt project. 

Created aggregate_event_types.sql macro which pulls the event types dynamically from the database and creates appropriate case statements to count up the events per type.

```
{% macro aggregate_event_types() %}

    {% set event_type_list = dbt_utils.get_column_values (table= ref('stg_postgres__events'), column = 'event_type')  %}

    {% for event_type in event_type_list %}
        ,SUM(CASE WHEN event_type = '{{ event_type }}' THEN 1 ELSE 0 END) AS macro_generated_{{ event_type }}_count
    {% endfor %}

{% endmacro %}
```

which is used like this in int_week3_daily_events_by_product.sql (n.b. I left the existing case statements in just so it was easy to compare the old and new):

```
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
```

## Part 3: We’re starting to think about granting permissions to our dbt models in our snowflake database so that other roles can have access to them.

Created a macro to grant the permission:

```
{% macro grant(role) %}

    GRANT SELECT ON {{ this }} TO {{ role }};

{% endmacro %}
```

The macro is referenced from the dbt_project.yml

```
# In this example config, we tell dbt to build all models in the example/
# directory as views. These settings can be overridden in the individual model
# files using the `{{ config(...) }}` macro.
models:
  greenery:
    +post-hook:
      - "{{ grant(role='reporting') }}"
    # Config indicated by + and applies to all files under models/example/
    example:
      +materialized: view
```

The snowflake query history shows the grant being created for every model....

![image](https://user-images.githubusercontent.com/130085262/235443340-0b4e6d2c-46e9-46a7-b45a-c7773349df63.png)


## Part 4:  After learning about dbt packages, we want to try one out and apply some macros or tests.

I installed a dbt_date package (via the packages.yml)

```
packages:
  - package: dbt-labs/dbt_utils
    version: 0.8.2
  - package: calogica/dbt_date
    version: [">=0.7.0", "<0.8.0"]
```

The package macro is then called in a dim_date model

```
{{
  config(
    materialised = 'table'
  )
}}

{{ dbt_date.get_date_dimension('01/01/2018', '01/01/2024') }}
```

and this produces a date dimension with lots of helpful attributes...

![image](https://user-images.githubusercontent.com/130085262/235443964-5ed3fd9c-1c41-4cc2-9f38-3c5731de67bb.png)

## Part 5: After improving our project with all the things that we have learned about dbt, we want to show off our work!

![image](https://user-images.githubusercontent.com/130085262/235444038-d49ed459-e0e2-4a40-907b-4be641510070.png)


## Part 6. dbt Snapshots

This shows the whole snapshot and marks which products have an inventory change from week 2 to week 3

```
WITH products_week2 AS (
    -- one row per product which is the max dbt_updated_at record that is less than 2023-04-29
    -- so this is whatever state it's in at the end of the 2nd week 
    SELECT *
    FROM   products_snapshot ps1
    WHERE date(dbt_updated_at) = (
        SELECT MAX(DATE(dbt_updated_at))
        FROM products_snapshot ps2
        WHERE DATE(ps2.dbt_updated_at) < '2023-04-29'
        AND ps2.product_id = ps1.product_id
    ) 
)
, updated_products_week3 AS (
    -- these are all the products that got updated in the 3rd week
    SELECT *
    FROM   products_snapshot
    WHERE DATE(dbt_updated_at) = '2023-04-29'
    
)

, changed_products AS (
    -- this is a list of products where the inventory changed w2 to w3
    SELECT pw3.product_id
          ,pw3.name
    FROM updated_products_week3 pw3
    JOIN products_week2 pw2 ON pw2.product_id = pw3.product_id
    where pw3.inventory <> pw2.inventory
)
  SELECT * FROM changed_products
  -- this provides the full snapshot picture and marks which
  -- products had an inventory change on w2 to w3.
  SELECT ps.*
        ,CASE
            WHEN cp.product_id IS NOT NULL THEN 1 
            ELSE NULL
         END AS changed_w2_w3
  FROM products_snapshot ps
  LEFT JOIN changed_products cp ON cp.product_id = ps.product_id AND ps.dbt_valid_to IS NULL
  ORder by ps.product_id, dbt_updated_at
;
```

These are the changed products...

![image](https://user-images.githubusercontent.com/130085262/235444302-6e268a74-447e-4bd7-8a38-cd4a30981b2b.png)






