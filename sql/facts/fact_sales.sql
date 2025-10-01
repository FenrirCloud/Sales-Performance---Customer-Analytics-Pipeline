-- Creates or appends to the Sales Fact table.
-- Joins staging data with dimension tables to get surrogate keys.
-- Calculates line_item_total_amount.

CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.dwh_dataset }}.fact_sales`
PARTITION BY
  DATE(invoice_date) 
CLUSTER BY
  customer_id, stock_code 
AS
SELECT
  FORMAT_DATE('%Y%m%d', DATE(st.InvoiceDate)) AS date_id, 
  st.InvoiceNo,
  st.InvoiceDate,
  st.CustomerID, 
  st.StockCode, 
  st.Quantity,
  st.UnitPrice,
  st.Quantity * st.UnitPrice AS line_item_total_amount 
FROM
  `{{ params.project_id }}.{{ params.staging_dataset }}.stg_transactions` AS st
WHERE
  DATE(st.InvoiceDate) <= CURRENT_DATE() 
  AND st.InvoiceNo NOT LIKE 'C%' 