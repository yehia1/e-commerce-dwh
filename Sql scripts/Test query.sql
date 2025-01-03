USE `e-commerce-dwh`;

select * from dim_seller;

select * from fact_order;

WITH RankedOrders AS (
    SELECT 
        s.seller_city AS pickup_city,
        s.seller_state AS pickup_state,
        u.customer_city AS delivery_city,
        u.customer_state AS delivery_state,
        d.hour,
        COUNT(o.order_key) AS order_count,
        ROW_NUMBER() OVER (
            PARTITION BY 
                s.seller_city, s.seller_state, u.customer_city, u.customer_state
            ORDER BY 
                COUNT(o.order_key) DESC
        ) AS rank_
    FROM 
        fact_order o
    JOIN 
        dim_seller s ON o.seller_id = s.seller_id
    JOIN 
        dim_user u ON o.user_id = u.user_id
    JOIN 
        dim_date d ON o.delivered_date_id = d.date_id
    GROUP BY 
        s.seller_city, s.seller_state, u.customer_city, u.customer_state, d.hour
)
SELECT 
    pickup_city,
    pickup_state,
    delivery_city,
    delivery_state,
    hour AS max_order_hour,
    order_count
FROM 
    RankedOrders
WHERE 
    rank_ = 1
ORDER BY 
    order_count DESC
LIMIT 10;


