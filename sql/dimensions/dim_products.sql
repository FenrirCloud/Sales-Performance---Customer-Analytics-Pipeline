-- Creates or updates the Product Dimension table.
-- Deduplicates products and attempts to categorize them based on description.
CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.dwh_dataset }}.dim_products`
AS
SELECT
  StockCode,
  ANY_VALUE(Description) AS Description, 
  CASE
    WHEN LOWER(Description) LIKE '%candle%' THEN 'Candles'
    WHEN LOWER(Description) LIKE '%bag%' OR LOWER(Description) LIKE '%sack%' THEN 'Bags'
    WHEN LOWER(Description) LIKE '%box%' OR LOWER(Description) LIKE '%case%' THEN 'Boxes & Cases'
    WHEN LOWER(Description) LIKE '%mug%' OR LOWER(Description) LIKE '%cup%' THEN 'Mugs & Cups'
    WHEN LOWER(Description) LIKE '%heart%' THEN 'Heart Items'
    WHEN LOWER(Description) LIKE '%christmas%' THEN 'Christmas'
    WHEN LOWER(Description) LIKE '%kitchen%' THEN 'Kitchenware'
    WHEN LOWER(Description) LIKE '%wall art%' OR LOWER(Description) LIKE '%poster%' THEN 'Wall Art'
    WHEN LOWER(Description) LIKE '%vintage%' THEN 'Vintage'
    WHEN LOWER(Description) LIKE '%holder%' OR LOWER(Description) LIKE '%stand%' THEN 'Holders & Stands'
  ELSE
  'Other'
  END AS product_category
FROM
  `{{ params.project_id }}.{{ params.staging_dataset }}.stg_transactions`
GROUP BY
  StockCode