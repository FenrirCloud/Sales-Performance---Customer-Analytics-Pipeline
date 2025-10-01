-- Aggregates daily sales by date and country for fast reporting.

CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.analytics_dataset }}.agg_daily_sales`
PARTITION BY
  sale_date
CLUSTER BY
  country
AS
SELECT
  DATE(fs.invoice_date) AS sale_date,
  dc.country,
  SUM(fs.line_item_total_amount) AS total_daily_sales,
  SUM(fs.Quantity) AS total_daily_quantity,
  COUNT(DISTINCT fs.InvoiceNo) AS total_daily_orders,
  AVG(fs.line_item_total_amount / fs.Quantity) AS average_unit_price
FROM
  `{{ params.project_id }}.{{ params.dwh_dataset }}.fact_sales` AS fs
JOIN
  `{{ params.project_id }}.{{ params.dwh_dataset }}.dim_customers` AS dc
  ON fs.CustomerID = dc.customer_id
WHERE
  DATE(fs.invoice_date) = '{{ params.execution_date }}' 
GROUP BY
  1,
  2