USE `e-commerce-dwh`;
DROP TABLE IF EXISTS dim_feedback;

CREATE TABLE dim_feedback (
	feedback_key INT AUTO_INCREMENT PRIMARY KEY, 						-- Surrogate key for feedback dimension
    feedback_id VARCHAR(50),                 
    feedback_score INT,                                          -- Feedback score (e.g., rating)
    feedback_form_sent_date DATETIME,                            -- Date the feedback form was sent
    feedback_answer_date DATETIME,                               -- Date the feedback was answered
    record_created_date DATETIME DEFAULT CURRENT_TIMESTAMP,      -- Timestamp for when the record is created
    record_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, -- Last update timestamp
    is_active BOOLEAN DEFAULT TRUE                               -- Flag to indicate if the feedback record is active
);

drop index d_f_i_1 on dim_feedback;
create Index d_f_i_1 on dim_feedback(feedback_key);

