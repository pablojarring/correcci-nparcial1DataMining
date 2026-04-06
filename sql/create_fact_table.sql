-- =============================================================================
-- CREATE TABLE: fct_monthly_usage (tabla de hechos)
-- =============================================================================
-- Extra (5 puntos): Escribir el código SQL para crear la tabla de hechos,
-- y el código para particionar, escribir el particionamiento solo de
-- Enero-Febrero 2025.
-- =============================================================================

-- Crear la tabla de hechos con particionamiento por rango de mes
CREATE TABLE IF NOT EXISTS gold.fct_monthly_usage (
    fact_id             BIGSERIAL,
    user_id             INT             NOT NULL,
    plan_name           VARCHAR(50)     NOT NULL,
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

    -- Constraints
    PRIMARY KEY (fact_id, month_year),
    FOREIGN KEY (user_id) REFERENCES gold.dim_users(user_id),
    FOREIGN KEY (plan_name) REFERENCES gold.dim_plans(plan_name)

) PARTITION BY RANGE (month_year);


-- =============================================================================
-- Particiones para Enero - Febrero 2025
-- =============================================================================

-- Partición: Enero 2025
CREATE TABLE gold.fct_monthly_usage_2025_01
    PARTITION OF gold.fct_monthly_usage
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

-- Partición: Febrero 2025
CREATE TABLE gold.fct_monthly_usage_2025_02
    PARTITION OF gold.fct_monthly_usage
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');


-- =============================================================================
-- Índices para optimizar consultas analíticas
-- =============================================================================
CREATE INDEX idx_fct_usage_user_id ON gold.fct_monthly_usage (user_id);
CREATE INDEX idx_fct_usage_plan ON gold.fct_monthly_usage (plan_name);
CREATE INDEX idx_fct_usage_month ON gold.fct_monthly_usage (month_year);
