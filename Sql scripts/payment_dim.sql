USE `e-commerce-dwh`;

DROP TABLE IF EXISTS dim_payment;
CREATE TABLE dim_payment (
    payment_id INT AUTO_INCREMENT PRIMARY KEY,                   -- Surrogate key for payment dimension
    payment_sequential INT,                                       -- Sequential number for payment
    payment_type VARCHAR(50),                                     -- Type of payment (e.g., Credit Card, PayPal)
    payment_installments INT,                                     -- Number of installments for the payment
    payment_value DECIMAL(10, 2),                                 -- Total value of the payment
    record_created_date DATETIME DEFAULT CURRENT_TIMESTAMP,       -- Timestamp for when the record is created
    record_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, -- Last update timestamp
    is_active BOOLEAN DEFAULT TRUE                                -- Flag to indicate if the payment record is active
);


