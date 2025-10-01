-- Creates or updates a Date Dimension table.
-- Populates date-related attributes for easier time-series analysis.

CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.dwh_dataset }}.dim_dates`
AS
SELECT
  CAST(FORMAT_DATE('%Y%m%d', dt) AS INT64) AS date_id,
  dt AS full_date,
  FORMAT_DATE('%A', dt) AS day_name,
  EXTRACT(DAYOFWEEK FROM dt) AS day_of_week, -- Sunday = 1, Saturday = 7
  EXTRACT(DAY FROM dt) AS day_of_month,
  EXTRACT(WEEK FROM dt) AS week_of_year,
  EXTRACT(MONTH FROM dt) AS month,
  FORMAT_DATE('%B', dt) AS month_name,
  EXTRACT(QUARTER FROM dt) AS quarter,
  EXTRACT(YEAR FROM dt) AS year,
  CASE
    WHEN FORMAT_DATE('%A', dt) IN ('Saturday', 'Sunday') THEN TRUE
  ELSE
  FALSE
  END AS is_weekend
FROM
  UNNEST(GENERATE_DATE_ARRAY(
      (SELECT MIN(DATE(InvoiceDate)) FROM `{{ params.project_id }}.{{ params.staging_dataset }}.stg_transactions`),
      (SELECT MAX(DATE(InvoiceDate)) FROM `{{ params.project_id }}.{{ params.staging_dataset }}.stg_transactions`),
      INTERVAL 1 DAY
    )) AS dt