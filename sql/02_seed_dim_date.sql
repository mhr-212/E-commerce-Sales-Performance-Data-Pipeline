-- ============================================================
-- Seed dim_date with all dates from 2019-01-01 to 2025-12-31
-- Run once during initial DB bootstrap
-- ============================================================
INSERT INTO dim_date (
    date_key, full_date, day_of_week, day_name,
    day_of_month, day_of_year, week_of_year,
    month_number, month_name, quarter_number, quarter_name,
    year_number, is_weekend, fiscal_period
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER                        AS date_key,
    d                                                       AS full_date,
    EXTRACT(ISODOW FROM d)::SMALLINT                       AS day_of_week,
    TO_CHAR(d, 'Day')                                       AS day_name,
    EXTRACT(DAY   FROM d)::SMALLINT                        AS day_of_month,
    EXTRACT(DOY   FROM d)::SMALLINT                        AS day_of_year,
    EXTRACT(WEEK  FROM d)::SMALLINT                        AS week_of_year,
    EXTRACT(MONTH FROM d)::SMALLINT                        AS month_number,
    TO_CHAR(d, 'Month')                                     AS month_name,
    EXTRACT(QUARTER FROM d)::SMALLINT                      AS quarter_number,
    'Q' || EXTRACT(QUARTER FROM d)::TEXT                   AS quarter_name,
    EXTRACT(YEAR  FROM d)::SMALLINT                        AS year_number,
    EXTRACT(ISODOW FROM d) IN (6, 7)                       AS is_weekend,
    TO_CHAR(d, 'YYYY') || '-Q' || EXTRACT(QUARTER FROM d)::TEXT AS fiscal_period
FROM generate_series(
    '2019-01-01'::DATE,
    '2025-12-31'::DATE,
    '1 day'::INTERVAL
) AS g(d)
ON CONFLICT (full_date) DO NOTHING;
