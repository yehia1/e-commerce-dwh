USE `e-commerce-dwh`;
DROP TABLE IF EXISTS fact_order;

CREATE TABLE fact_order (
	order_key INT AUTO_INCREMENT PRIMARY KEY,
    order_id VARCHAR(50) ,                               
    user_id INT,                                                    -- Foreign key to user dimension
    product_id INT,                                                 -- Foreign key to product dimension
    seller_id INT,                                                  -- Foreign key to seller dimension
	feedback_id INT,                                                -- Foreign key to feedback dimension
    payment_id INT,                                                 -- Foreign key to payment dimension
    order_date_id INT,                                              -- Foreign key to the order date dimension
    order_approved_date_id INT,                                     -- Foreign key to the approved date dimension
    pickup_date_id INT,                                             -- Foreign key to the pickup date dimension
    delivered_date_id INT,                                          -- Foreign key to the delivered date dimension
    estimated_time_delivery date,                                 -- Foreign key to the estimated date dimension
    pickup_limit_date_id INT,										-- Foreign key to the pick up limit date dimension
	order_status VARCHAR(50),                                       -- Order status (e.g., 'Pending', 'Shipped')
    order_item_id INT,                                              -- Order item identifier
    price DECIMAL(10, 2),                                           -- Price of the order/item
    shipping_cost DECIMAL(10, 2),                                   -- Shipping cost

    record_created_date DATETIME DEFAULT CURRENT_TIMESTAMP,         -- Timestamp for when the record is created
    record_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, -- Last update timestamp
    FOREIGN KEY (user_id) REFERENCES dim_user(user_id),             -- Foreign key to dim_user
    FOREIGN KEY (product_id) REFERENCES dim_product(product_key),   -- Foreign key to dim_product
    FOREIGN KEY (seller_id) REFERENCES dim_seller(seller_key),       -- Foreign key to dim_seller
    FOREIGN KEY (order_date_id) REFERENCES dim_date(date_id),       -- Foreign key to dim_date (order date)
    FOREIGN KEY (order_approved_date_id) REFERENCES dim_date(date_id), -- Foreign key to dim_date (approved date)
    FOREIGN KEY (pickup_date_id) REFERENCES dim_date(date_id),      -- Foreign key to dim_date (pickup date)
    FOREIGN KEY (delivered_date_id) REFERENCES dim_date(date_id),   -- Foreign key to dim_date (delivered date)
    FOREIGN KEY (feedback_id) REFERENCES dim_feedback(feedback_key), -- Foreign key to dim_feedback
    FOREIGN KEY (payment_id) REFERENCES dim_payment(payment_id)    -- Foreign key to dim_payment
);


