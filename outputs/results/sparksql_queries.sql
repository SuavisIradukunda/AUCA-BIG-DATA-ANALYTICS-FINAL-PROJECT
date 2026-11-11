-- q1_funnel_by_device
WITH session_flags AS (
            SELECT s.session_id, s.device_type,
                   s.n_products_viewed > 0                         AS viewed_product,
                   s.n_cart_items > 0                              AS added_to_cart,
                   s.reached_checkout                              AS reached_checkout,
                   s.conversion_status = 'converted'               AS purchased
            FROM sessions s
        )
        SELECT device_type,
               COUNT(*)                                            AS sessions,
               SUM(CAST(viewed_product AS INT))                    AS viewed_product,
               SUM(CAST(added_to_cart AS INT))                     AS added_to_cart,
               SUM(CAST(reached_checkout AS INT))                  AS reached_checkout,
               SUM(CAST(purchased AS INT))                         AS purchased,
               ROUND(100 * AVG(CAST(purchased AS INT)), 2)         AS conversion_rate_pct,
               ROUND(100 * SUM(CAST(purchased AS INT)) /
                     NULLIF(SUM(CAST(added_to_cart AS INT)), 0), 2) AS cart_to_purchase_pct
        FROM session_flags
        GROUP BY device_type
        ORDER BY sessions DESC;

-- q1b_funnel_overall
SELECT 'sessions' AS stage, 1 AS step, COUNT(*) AS sessions FROM sessions
        UNION ALL SELECT 'viewed a product', 2, COUNT(*) FROM sessions WHERE n_products_viewed > 0
        UNION ALL SELECT 'added to cart', 3, COUNT(*) FROM sessions WHERE n_cart_items > 0
        UNION ALL SELECT 'reached checkout', 4, COUNT(*) FROM sessions WHERE reached_checkout
        UNION ALL SELECT 'purchased', 5, COUNT(*) FROM sessions WHERE conversion_status = 'converted'
        ORDER BY step;

-- q2_top3_products_per_category
WITH product_revenue AS (
            SELECT category_name, product_id,
                   SUM(net_amount) AS revenue, SUM(quantity) AS units
            FROM transaction_items GROUP BY category_name, product_id
        ), ranked AS (
            SELECT pr.*,
                   RANK() OVER (PARTITION BY category_name ORDER BY revenue DESC)        AS rnk,
                   revenue / SUM(revenue) OVER (PARTITION BY category_name)             AS share
            FROM product_revenue pr
        )
        SELECT r.category_name, r.rnk, r.product_id, p.name,
               ROUND(r.revenue, 2) AS revenue, r.units, ROUND(100 * r.share, 2) AS pct_of_category
        FROM ranked r JOIN products p USING (product_id)
        WHERE r.rnk <= 3
        ORDER BY r.category_name, r.rnk;

-- q3_referrer_performance
SELECT s.referrer,
               COUNT(*)                                             AS sessions,
               ROUND(100 * AVG(CASE WHEN t.transaction_id IS NOT NULL THEN 1 ELSE 0 END), 2)
                                                                    AS conversion_rate_pct,
               ROUND(SUM(COALESCE(t.total, 0)), 2)                  AS revenue,
               ROUND(SUM(COALESCE(t.total, 0)) / COUNT(*), 2)       AS revenue_per_session,
               ROUND(AVG(t.total), 2)                               AS avg_order_value,
               ROUND(AVG(s.duration_seconds) / 60, 1)               AS avg_session_min
        FROM sessions s
        LEFT JOIN transactions t ON t.session_id = s.session_id
        GROUP BY s.referrer
        ORDER BY revenue_per_session DESC;

-- q4_revenue_weekday_hour
SELECT date_format(timestamp, 'E') AS weekday, dayofweek(timestamp) AS dow,
               hour(timestamp) AS hour, COUNT(*) AS orders, ROUND(SUM(total), 2) AS revenue
        FROM transactions
        GROUP BY 1, 2, 3
        ORDER BY dow, hour;

-- q5_price_change_impact
WITH last_change AS (                  -- most recent price change per product
            SELECT product_id, effective_from AS changed_at, price AS new_price,
                   LAG(price) OVER (PARTITION BY product_id ORDER BY pos) AS old_price,
                   ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY pos DESC) AS rn
            FROM product_price_history
        ), changes AS (
            SELECT * FROM last_change
            WHERE rn = 1 AND old_price IS NOT NULL
              AND changed_at BETWEEN TIMESTAMP '2026-08-16' AND TIMESTAMP '2026-10-17'
        ), daily AS (                          -- units/day in the 14 days before vs after
            SELECT c.product_id,
                   CASE WHEN c.new_price > c.old_price THEN 'price increase'
                        ELSE 'price decrease' END                              AS change_type,
                   (c.new_price - c.old_price) / c.old_price                     AS pct_change,
                   SUM(CASE WHEN ti.timestamp <  c.changed_at THEN ti.quantity ELSE 0 END) / 14.0 AS units_before,
                   SUM(CASE WHEN ti.timestamp >= c.changed_at THEN ti.quantity ELSE 0 END) / 14.0 AS units_after
            FROM changes c
            JOIN transaction_items ti
              ON ti.product_id = c.product_id
             AND ti.timestamp BETWEEN c.changed_at - INTERVAL 14 DAYS AND c.changed_at + INTERVAL 14 DAYS
            GROUP BY c.product_id, c.new_price, c.old_price
        )
        SELECT change_type, COUNT(*) AS products,
               ROUND(100 * AVG(pct_change), 1)  AS avg_price_change_pct,
               ROUND(AVG(units_before), 3)      AS avg_units_per_day_before,
               ROUND(AVG(units_after), 3)       AS avg_units_per_day_after,
               ROUND(100 * (AVG(units_after) - AVG(units_before)) / AVG(units_before), 1) AS units_change_pct
        FROM daily GROUP BY change_type;

-- q6_daily_revenue_by_channel
SELECT txn_date, channel, COUNT(*) AS orders, ROUND(SUM(total), 2) AS revenue,
               ROUND(SUM(discount), 2) AS discounts
        FROM transactions GROUP BY txn_date, channel ORDER BY txn_date, channel;

-- q7_top_sold_vs_viewed
WITH views AS (
            SELECT product_id, COUNT(*) AS views FROM page_views
            WHERE page_type = 'product_detail' GROUP BY product_id
        ), sales AS (
            SELECT product_id, SUM(quantity) AS units_sold, SUM(net_amount) AS revenue
            FROM transaction_items GROUP BY product_id
        )
        SELECT p.product_id, p.name, p.category_name, p.base_price, p.is_active, p.current_stock,
               v.views, COALESCE(s.units_sold, 0) AS units_sold, ROUND(COALESCE(s.revenue, 0), 2) AS revenue,
               ROUND(COALESCE(s.units_sold, 0) / v.views, 4) AS units_per_view,
               RANK() OVER (ORDER BY v.views DESC) AS view_rank,
               RANK() OVER (ORDER BY COALESCE(s.units_sold, 0) DESC) AS sales_rank
        FROM products p JOIN views v USING (product_id) LEFT JOIN sales s USING (product_id);

-- q8_mongo_spend_by_age_region
SELECT age_group,
               CASE WHEN state IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'Northeast'
                    WHEN state IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'Midwest'
                    WHEN state IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'West'
                    ELSE 'South' END                                   AS region,
               COUNT(*)                                                AS users,
               ROUND(AVG(total_spent), 2)                              AS avg_spent,
               ROUND(AVG(total_orders), 2)                             AS avg_orders,
               ROUND(SUM(total_spent) / SUM(SUM(total_spent)) OVER (), 4) AS share_of_revenue
        FROM users_mongo
        GROUP BY 1, 2
        ORDER BY age_group, region;

-- q9_hbase_category_conversion
SELECT p.category_name,
               SUM(m.views)                                       AS views,
               SUM(m.cart_adds)                                   AS cart_adds,
               SUM(m.purchases)                                   AS purchases,
               ROUND(100 * SUM(m.cart_adds) / SUM(m.views), 2)    AS view_to_cart_pct,
               ROUND(100 * SUM(m.purchases) / SUM(m.views), 2)    AS view_to_purchase_pct,
               ROUND(SUM(m.revenue), 2)                           AS revenue,
               ROUND(SUM(m.revenue * p.profit_margin), 2)         AS gross_margin
        FROM product_metrics_hbase m
        JOIN products_mongo p ON p.product_id = m.product_id
        GROUP BY p.category_name
        ORDER BY view_to_purchase_pct DESC;

