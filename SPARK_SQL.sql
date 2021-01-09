
-- EJERCICIOS VARIOS
1)
DROP TABLE IF EXISTS discounts;
CREATE TABLE discounts AS
SELECT
  to_date(time) as date,
  discount_id,
  CAST(unit_price AS FLOAT)
FROM
  sales;
SELECT
  *
FROM
  discounts

2)
--OPTION
CREATE OR REPLACE TEMPORARY VIEW overdue_payments AS
SELECT cust_first_name, 
  cust_last_name,
  cust_city, 
  cust_state, 
  CAST(overdue AS BOOLEAN), 
  ROUND(cust_credit_lim * .02, 2) AS overdue_charge
FROM customers;

SELECT * FROM overdue_payments;

3)
--OPTION
DROP TABLE IF EXISTS restaurant_reviews;
CREATE TABLE restaurant_reviews (
  restaurant_id STRING,
  user_id INT,
  rating FLOAT,
  text_review STRING,
  time_recorded INT
) USING csv OPTIONS (
  path "/mnt/training/restaurants/reviews.csv",
  header "true"
);

4)
--OPTION
SELECT
  DISTINCT *
FROM
  (
    SELECT
      title,
      user_id,
      rating,
      text_review,
      from_unixtime(time_recorded) as time
    FROM
      movie_evals
  )
ORDER BY
  rating ASC,
  time DESC

5)
--OPTION
SELECT
  product_id,
  to_date(time_recorded) closing_date,
  net_revenue
FROM
  q1_sales
ORDER BY
  net_revenue DESC, closing_date ASC
LIMIT
  5

6)
--OPTION
SELECT
  id,
  match_date,
  stadium,
  player
FROM
  games
  INNER JOIN goals ON id = match_id

7)
--OPTION
SELECT *
FROM q1_earnings
UNION ALL
SELECT *
FROM q2_earnings

8)
--OPTION
SELECT
  month(time) month,
  day(time) day,
  rating
FROM
  timetable1

9)
--OPTION
SELECT
  date_format(to_date(time), "E") day,
  purchase_amt,
  sale_id
FROM
  sales

10)
--OPTION
WITH total_rev AS (
  SELECT
    *,
    ROUND(sale_price * quantity, 2) total_revenue
  FROM
    feb_sales
)
SELECT
  category,
  ROUND(SUM(total_revenue), 2) category_sum,
  MIN(total_revenue) min_revenue
FROM
  total_rev
GROUP BY
  category

11) NOOOO ES 
--OPTION
WITH total_rev AS (
  SELECT
    *,
    ROUND(sale_price * quantity, 2) total_revenue
  FROM
    feb_sales
)
SELECT
  category,
  ROUND(SUM(total_revenue), 2) category_sum,
  MAX(total_revenue) max_revenue
FROM
  total_rev
GROUP BY
  category item_id

ES
--OPTION
SELECT 
  category,
  item_id,
  ROUND(SUM(total_revenue), 2) category_sum,
  MAX(total_revenue) max_revenue
  FROM (
    SELECT *, 
    ROUND(sale_price * quantity, 2) total_revenue
    FROM feb_sales
   )
  GROUP BY category, item_id


12)
--OPTION
SELECT
  category,
  ROUND(STD(total_revenue), 3) std_deviation,
  ROUND(AVG(total_revenue),2) avg_revenue
FROM
  (
    SELECT
      *,
      ROUND(sale_price * quantity, 2) total_revenue
    FROM
      feb_sales
  )
GROUP BY
  category

13) NOOO ES
--OPTION
SELECT
  *
FROM
  (
    SELECT
      *,
      ROUND(sale_price * quantity, 2) total_revenue
    FROM
      feb_sales
  ) PIVOT (
    ROUND(SUM(total_revenue), 2) FOR category IN ('books', 'magazines', 'movies')
  );


--OPTION
SELECT
  *
FROM
  (
    SELECT
      category,
      ROUND(SUM(total_revenue), 2)
    FROM
      feb_sales
  ) PIVOT (
    ROUND(sale_price * quantity, 2) total_revenue FOR category IN ('books', 'magazines', 'movies')
  );

14)
--OPTION
SELECT
  dept,
  SUM(sales_rev)
FROM
  inventory
WHERE
  (
    item_id IS NOT NULL
    AND dept IS NOT NULL
  )
GROUP BY
  Dept

15)
--OPTION
SELECT 
  COALESCE(title, "All items") AS title,
  COALESCE(month, "All months") AS month,
  ROUND(AVG(revenue), 2) as avg_revenue
FROM book_sales
GROUP BY ROLLUP (title, month)
ORDER BY title, month;

16)
--OPTION
WITH explode_subs AS (
  SELECT
    first_name,
    last_name,
    EXPLODE (subscriptions)
  FROM
    customers
)
SELECT
  first_name,
  last_name,
  value.titles,
  value.payment_methods
FROM
  explode_subs

17)
--OPTION
SELECT
  first_name,
  last_name,
  value.titles[0] AS title
FROM (
    SELECT
    first_name,
    last_name,
    EXPLODE (subscriptions)
  FROM
    customers
  )

18)
--OPTION
SELECT 
  TRANSFORM(titles, t -> UPPER(t)) titles
FROM 
  books

19)
--OPTION
SELECT 
  location_id, 
  EXISTS(battery_levels, b -> b < 4 AND backups = 0) needs_service
FROM devices

20)

--OPTION
SELECT
  location_id,
  REDUCE(battery_levels, 0, (level, acc) -> level + acc) total_batt_levels
FROM
  devices

