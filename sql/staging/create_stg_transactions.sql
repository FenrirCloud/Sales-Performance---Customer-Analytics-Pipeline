-- Creates a cleaned and standardized staging table for e-commerce transactions.
-- Handles data type conversions, filters invalid records, and standardizes CustomerID.

CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.staging_dataset }}.stg_transactions` AS
SELECT
  t.InvoiceNo,
  t.StockCode,
  t.Description,
  CAST(t.Quantity AS INT64) AS Quantity,
  PARSE_TIMESTAMP('%m/%d/%Y %H:%M', t.InvoiceDate) AS InvoiceDate, 
  CAST(t.UnitPrice AS NUMERIC) AS UnitPrice,
  COALESCE(t.CustomerID, 'ANONYMOUS') AS CustomerID, 
  t.Country
FROM
  `{{ params.project_id }}.{{ params.staging_dataset }}.stg_raw_sales` AS t
WHERE
  t.Quantity > 0 
  AND t.UnitPrice > 0 
  AND t.InvoiceNo IS NOT NULL 
  AND PARSE_TIMESTAMP('%m/%d/%Y %H:%M', t.InvoiceDate) IS NOT NULL 