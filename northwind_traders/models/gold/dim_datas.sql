{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'dimensao']
) }}

WITH date_series AS (
    SELECT explode(sequence(to_date('1996-01-01'), to_date('2030-12-31'), interval 1 day)) AS date_day
),

enriched_dates AS (
    SELECT
        CAST(date_format(date_day, 'yyyyMMdd') AS INT) AS sk_date,
        date_day AS date_actual,
        year(date_day) AS date_year,
        month(date_day) AS date_month,
        day(date_day) AS date_day_of_month,
        quarter(date_day) AS date_quarter,
        date_format(date_day, 'MMMM') AS date_month_name,
        date_format(date_day, 'EEEE') AS date_day_name,
        CASE WHEN dayofweek(date_day) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
    FROM date_series
)

SELECT * FROM enriched_dates