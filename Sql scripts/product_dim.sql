USE `e-commerce-dwh`;
DROP TABLE IF EXISTS dim_product;

CREATE TABLE dim_product (
    product_key INT AUTO_INCREMENT PRIMARY KEY,        -- Surrogate key, auto-incremented
    product_id VARCHAR(50),                                             -- Natural key (business key) from the source data
    product_category VARCHAR(100),                               -- Category of the product
    product_name_length INT,                                     -- Length of the product name (in characters)
    product_description_length INT,                              -- Length of the product description (in characters)
    product_photos_qty INT,                                      -- Quantity of product photos available
    product_weight_g DECIMAL(10, 2),                              -- Weight of the product in grams
    product_length_cm DECIMAL(10, 2),                            -- Length of the product in centimeters
    product_height_cm DECIMAL(10, 2),                            -- Height of the product in centimeters
    product_width_cm DECIMAL(10, 2),                             -- Width of the product in centimeters
    record_created_date DATETIME DEFAULT CURRENT_TIMESTAMP,      -- Timestamp for when the record is created
    record_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, -- Last update timestamp
    is_active BOOLEAN DEFAULT TRUE                               -- Flag to indicate if the product is active
);

DROP INDEX d_p_i_1 ON dim_product;
create Index d_p_i_1 on dim_product(product_key);

