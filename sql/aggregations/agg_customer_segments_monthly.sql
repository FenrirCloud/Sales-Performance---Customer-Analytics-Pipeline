-- Aggregates monthly performance of RFM customer segments.

CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.analytics_dataset }}.agg_customer_segments_monthly`
PARTITION BY
  month_start_date
CLUSTER BY
  rfm_segment
AS
SELECT
  DATE_TRUNC(DATE(fs.invoice_date), MONTH) AS month_start_date,
  dc.rfm_segment,
  COUNT(DISTINCT fs.CustomerID) AS distinct_customers_in_segment,
  SUM(fs.line_item_total_amount) AS total_segment_sales,
  AVG(fs.line_item_total_amount) AS average_order_value_in_segment
FROM
  `{{ params.project_id }}.{{ params.dwh_dataset }}.fact_sales` AS fs
JOIN
  `{{ params.project_id }}.{{ params.dwh_dataset }}.dim_customers` AS dc
  ON fs.CustomerID = dc.customer_id
WHERE
  DATE_TRUNC(DATE(fs.invoice_date), MONTH) = DATE_TRUNC(DATE('{{ params.execution_date }}'), MONTH) 
GROUP BY
  1,
  2