-- Identifies the top N (e.g., Top 10) products by sales each month.

CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.analytics_dataset }}.top_n_products_monthly`
PARTITION BY
  month_start_date
CLUSTER BY
  rank_this_month
AS
WITH
  monthly_product_sales AS (
  SELECT
    DATE_TRUNC(DATE(fs.invoice_date), MONTH) AS month_start_date,
    dp.stock_code,
    dp.description,
    dp.product_category,
    SUM(fs.line_item_total_amount) AS total_sales_this_month
  FROM
    `{{ params.project_id }}.{{ params.dwh_dataset }}.fact_sales` AS fs
  JOIN
    `{{ params.project_id }}.{{ params.dwh_dataset }}.dim_products` AS dp
    ON fs.StockCode = dp.stock_code
  WHERE
    DATE_TRUNC(DATE(fs.invoice_date), MONTH) = DATE_TRUNC(DATE('{{ params.execution_date }}'), MONTH) 
  GROUP BY
    1,
    2,
    3,
    4
  )
SELECT
  month_start_date,
  stock_code,
  description,
  product_category,
  total_sales_this_month,
  ROW_NUMBER() OVER (PARTITION BY month_start_date ORDER BY total_sales_this_month DESC) AS rank_this_month
FROM
  monthly_product_sales
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY month_start_date ORDER BY total_sales_this_month DESC) <= 10 