-- ============================================================
-- E-Commerce Sales Data Warehouse Schema
-- Dimensional Model: Star Schema with partitioned fact table
-- ============================================================

-- Enable uuid extension for surrogate keys
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================
-- DIMENSION: dim_date  (calendar dimension, pre-populated)
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER PRIMARY KEY,          -- YYYYMMDD integer
    full_date       DATE        NOT NULL UNIQUE,
    day_of_week     SMALLINT    NOT NULL,          -- 1=Mon … 7=Sun
    day_name        VARCHAR(9)  NOT NULL,
    day_of_month    SMALLINT    NOT NULL,
    day_of_year     SMALLINT    NOT NULL,
    week_of_year    SMALLINT    NOT NULL,
    month_number    SMALLINT    NOT NULL,
    month_name      VARCHAR(9)  NOT NULL,
    quarter_number  SMALLINT    NOT NULL,
    quarter_name    VARCHAR(2)  NOT NULL,          -- Q1 … Q4
    year_number     SMALLINT    NOT NULL,
    is_weekend      BOOLEAN     NOT NULL DEFAULT FALSE,
    is_holiday      BOOLEAN     NOT NULL DEFAULT FALSE,
    fiscal_period   VARCHAR(7)  NOT NULL           -- YYYY-Qn
);

-- ============================================================
-- DIMENSION: dim_product  (SCD Type-2)
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_product (
    product_sk          SERIAL PRIMARY KEY,
    product_id          VARCHAR(20)  NOT NULL,     -- natural key
    product_name        VARCHAR(255) NOT NULL,
    category            VARCHAR(100) NOT NULL,
    subcategory         VARCHAR(100) NOT NULL,
    brand               VARCHAR(100),
    unit_cost           NUMERIC(12,4) NOT NULL,
    unit_price          NUMERIC(12,4) NOT NULL,
    margin_pct          NUMERIC(6,4)  GENERATED ALWAYS AS
                            (ROUND((unit_price - unit_cost) / NULLIF(unit_price,0), 4)) STORED,
    is_active           BOOLEAN     NOT NULL DEFAULT TRUE,
    valid_from          DATE        NOT NULL,
    valid_to            DATE        NOT NULL DEFAULT '9999-12-31',
    row_hash            CHAR(64)    NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (product_id, valid_from)
);
CREATE INDEX IF NOT EXISTS idx_dim_product_id       ON dim_product (product_id);
CREATE INDEX IF NOT EXISTS idx_dim_product_active   ON dim_product (product_id) WHERE is_active = TRUE;

-- ============================================================
-- DIMENSION: dim_customer
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk     SERIAL PRIMARY KEY,
    customer_id     VARCHAR(20)  NOT NULL UNIQUE,
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    email           VARCHAR(255),
    segment         VARCHAR(50)  NOT NULL,         -- Consumer, Corporate, Home Office
    country         VARCHAR(100) NOT NULL,
    region          VARCHAR(100) NOT NULL,
    city            VARCHAR(100),
    signup_date     DATE         NOT NULL,
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dim_customer_id      ON dim_customer (customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_customer_region  ON dim_customer (region);

-- ============================================================
-- FACT TABLE: fact_sales  (partitioned by sale_date, monthly)
-- ============================================================
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_sk             BIGSERIAL,
    sale_id             VARCHAR(40)   NOT NULL,    -- natural key / idempotency
    date_key            INTEGER       NOT NULL REFERENCES dim_date(date_key),
    product_sk          INTEGER       NOT NULL REFERENCES dim_product(product_sk),
    customer_sk         INTEGER       NOT NULL REFERENCES dim_customer(customer_sk),
    sale_date           DATE          NOT NULL,
    order_id            VARCHAR(40)   NOT NULL,
    quantity            INTEGER       NOT NULL CHECK (quantity > 0),
    unit_price          NUMERIC(12,4) NOT NULL,
    discount_pct        NUMERIC(6,4)  NOT NULL DEFAULT 0 CHECK (discount_pct BETWEEN 0 AND 1),
    gross_amount        NUMERIC(16,4) NOT NULL,
    discount_amount     NUMERIC(16,4) NOT NULL DEFAULT 0,
    net_amount          NUMERIC(16,4) NOT NULL,
    shipping_cost       NUMERIC(12,4) NOT NULL DEFAULT 0,
    return_flag         BOOLEAN       NOT NULL DEFAULT FALSE,
    return_date         DATE,
    channel             VARCHAR(50)   NOT NULL DEFAULT 'online',
    row_checksum        CHAR(64)      NOT NULL,
    pipeline_run_id     UUID          NOT NULL,
    loaded_at           TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (sale_sk, sale_date)
) PARTITION BY RANGE (sale_date);

-- Create monthly partitions for 2020-2025
DO $$
DECLARE
    yr      INT;
    mo      INT;
    p_start DATE;
    p_end   DATE;
    tname   TEXT;
BEGIN
    FOR yr IN 2020..2025 LOOP
        FOR mo IN 1..12 LOOP
            p_start := make_date(yr, mo, 1);
            p_end   := p_start + INTERVAL '1 month';
            tname   := format('fact_sales_%s_%s', yr, lpad(mo::text, 2, '0'));
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF fact_sales
                 FOR VALUES FROM (%L) TO (%L)',
                tname, p_start, p_end
            );
        END LOOP;
    END LOOP;
END
$$;

-- ============================================================
-- AUDIT TABLE: pipeline_audit_log
-- ============================================================
CREATE TABLE IF NOT EXISTS pipeline_audit_log (
    audit_id        BIGSERIAL   PRIMARY KEY,
    run_id          UUID        NOT NULL,
    dag_id          VARCHAR(100),
    task_id         VARCHAR(100),
    run_date        DATE        NOT NULL,
    stage           VARCHAR(50) NOT NULL,          -- extract | transform | load | validate
    status          VARCHAR(20) NOT NULL,          -- success | failed | partial
    rows_processed  BIGINT      DEFAULT 0,
    rows_inserted   BIGINT      DEFAULT 0,
    rows_updated    BIGINT      DEFAULT 0,
    rows_rejected   BIGINT      DEFAULT 0,
    source_checksum VARCHAR(64),
    target_checksum VARCHAR(64),
    error_message   TEXT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    duration_secs   NUMERIC(10,3) GENERATED ALWAYS AS
                        (EXTRACT(EPOCH FROM (finished_at - started_at))) STORED
);
CREATE INDEX IF NOT EXISTS idx_audit_run_id   ON pipeline_audit_log (run_id);
CREATE INDEX IF NOT EXISTS idx_audit_run_date ON pipeline_audit_log (run_date);

-- ============================================================
-- AGGREGATE MART: mart_daily_sales  (materialized, refreshed daily)
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mart_daily_sales AS
SELECT
    dd.full_date,
    dd.year_number,
    dd.quarter_number,
    dd.month_number,
    dd.week_of_year,
    dp.category,
    dp.subcategory,
    dc.region,
    dc.country,
    dc.segment,
    COUNT(DISTINCT fs.order_id)     AS order_count,
    COUNT(fs.sale_sk)               AS line_items,
    SUM(fs.quantity)                AS total_units,
    SUM(fs.gross_amount)            AS total_gross,
    SUM(fs.discount_amount)         AS total_discount,
    SUM(fs.net_amount)              AS total_net,
    AVG(fs.net_amount)              AS avg_order_value,
    SUM(fs.net_amount) FILTER (WHERE fs.return_flag = FALSE) AS revenue,
    COUNT(*) FILTER (WHERE fs.return_flag = TRUE)            AS return_count
FROM   fact_sales   fs
JOIN   dim_date     dd ON dd.date_key   = fs.date_key
JOIN   dim_product  dp ON dp.product_sk = fs.product_sk
JOIN   dim_customer dc ON dc.customer_sk = fs.customer_sk
GROUP  BY 1,2,3,4,5,6,7,8,9,10
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mart_daily_sales
    ON mart_daily_sales (full_date, category, region, segment);
