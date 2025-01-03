USE `e-commerce-dwh`;
DROP TABLE IF EXISTS dim_seller;

CREATE TABLE dim_seller (
	seller_key INT AUTO_INCREMENT PRIMARY KEY, -- Surrogate key for seller
    seller_id VARCHAR(50),
    seller_zip_code VARCHAR(10),              -- Seller's zip code
    seller_city VARCHAR(100),                 -- Seller's city
    seller_state VARCHAR(50),                 -- Seller's state
    
    -- Audit Columns
    record_created_date DATETIME DEFAULT CURRENT_TIMESTAMP,  -- When the record was created
    record_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,  -- Last updated
    is_active BOOLEAN DEFAULT TRUE                           -- Flag for soft deletes (logical deletion)
);


drop index d_s_i_1 on dim_seller;
create Index d_s_i_1 on dim_seller(seller_key);

ALTER TABLE fact_order DROP FOREIGN KEY fact_order_ibfk_3;
-- 4. Add the foreign key constraint again
ALTER TABLE fact_order
ADD CONSTRAINT fact_order_ibfk_3
    FOREIGN KEY (seller_id)
    REFERENCES dim_seller(seller_key)
    ON DELETE CASCADE;