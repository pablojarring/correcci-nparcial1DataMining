CREATE TABLE IF NOT EXISTS gold.fct_monthly_usage (
    fact_id             BIGSERIAL,
    user_id             INT             NOT NULL,
    plan_name           VARCHAR(50)     NOT NULL,
    date_key            INT             NOT NULL,
    month_year          DATE            NOT NULL,
    total_calls         INT             DEFAULT 0,
    total_call_minutes  DECIMAL(12,2)   DEFAULT 0,
    total_messages      INT             DEFAULT 0,
    total_mb_used       DECIMAL(14,2)   DEFAULT 0,
    excess_minutes      DECIMAL(12,2)   DEFAULT 0,
    excess_messages     INT             DEFAULT 0,
    excess_mb           DECIMAL(14,2)   DEFAULT 0,
    base_revenue        DECIMAL(10,2)   DEFAULT 0,
    excess_call_revenue     DECIMAL(10,2)   DEFAULT 0,
    excess_message_revenue  DECIMAL(10,2)   DEFAULT 0,
    excess_internet_revenue DECIMAL(10,2)   DEFAULT 0,
    total_revenue       DECIMAL(12,2)   DEFAULT 0,

    PRIMARY KEY (fact_id, month_year),
    FOREIGN KEY (user_id) REFERENCES gold.dim_users(user_id),
    FOREIGN KEY (plan_name) REFERENCES gold.dim_plans(plan_name),
    FOREIGN KEY (date_key) REFERENCES gold.dim_date(date_key)

) PARTITION BY RANGE (month_year);

CREATE TABLE gold.fct_monthly_usage_2025_01
    PARTITION OF gold.fct_monthly_usage
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

CREATE TABLE gold.fct_monthly_usage_2025_02
    PARTITION OF gold.fct_monthly_usage
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
