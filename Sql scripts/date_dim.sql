USE `e-commerce-dwh`;
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date (
    date_id int auto_increment primary key,                                      -- Surrogate key for date dimension
    full_timestamp DATETIME NOT NULL,                              -- Full timestamp including date, hour, minute, second
    year INT,                                                     -- Year part of the date
    quarter INT,                                                  -- Quarter (1-4)
    month INT,                                                    -- Month (1-12)
    month_name VARCHAR(20),                                       -- Full name of the month (e.g., January, February)
    day INT,                                                      -- Day of the month (1-31)
    day_of_week INT,                                              -- Day of the week (1-7, where 1 is Sunday)
    day_of_week_name VARCHAR(20),                                 -- Full name of the day (e.g., Sunday, Monday)
    hour INT,                                                     -- Hour part of the timestamp (0-23)
    minute INT,                                                   -- Minute part of the timestamp (0-59)                                                  -- Second part of the timestamp (0-59)
    week_of_year INT,                                             -- Week of the year (1-52)
    record_created_date DATETIME DEFAULT CURRENT_TIMESTAMP,       -- Timestamp for when the record is created
    record_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, -- Last update timestamp
    is_active BOOLEAN DEFAULT TRUE                                 -- Flag to indicate if the record is active
);

ALTER TABLE dim_date AUTO_INCREMENT = 1;


ALTER TABLE fact_order DROP FOREIGN KEY fact_order_ibfk_4;
ALTER TABLE fact_order DROP FOREIGN KEY fact_order_ibfk_5;
ALTER TABLE fact_order DROP FOREIGN KEY fact_order_ibfk_6;
ALTER TABLE fact_order DROP FOREIGN KEY fact_order_ibfk_7;
-- 4. Add the foreign key constraint again
ALTER TABLE fact_order
ADD CONSTRAINT fact_order_ibfk_4
    FOREIGN KEY (order_date_id)
    REFERENCES dim_date(date_id)
    ON DELETE CASCADE;

ALTER TABLE fact_order
ADD CONSTRAINT fact_order_ibfk_5
    FOREIGN KEY (order_approved_date_id)
    REFERENCES dim_date(date_id)
    ON DELETE CASCADE;

ALTER TABLE fact_order
ADD CONSTRAINT fact_order_ibfk_6
    FOREIGN KEY (pickup_date_id)
    REFERENCES dim_date(date_id)
    ON DELETE CASCADE;

ALTER TABLE fact_order
ADD CONSTRAINT fact_order_ibfk_7
    FOREIGN KEY (delivered_date_id)
    REFERENCES dim_date(date_id)
    ON DELETE CASCADE;

