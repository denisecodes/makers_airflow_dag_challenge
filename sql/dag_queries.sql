-- Create table joining products and product sales filtering by sale_date
CREATE TABLE IF NOT EXISTS new_product_sales AS
SELECT 
product_id, 
quantity,
sale_date 
FROM 
product_sales
WHERE sale_date >= '2023-04-25';

-- Create table to show the total quantity of each product
CREATE TABLE IF NOT EXISTS aggregated_product_sales AS
SELECT 
product_id,
SUM(quantity) as total_quantity
FROM new_product_sales
GROUP BY product_id;

-- Insert values into product_sales_summary
INSERT INTO product_sales_summary (product_id, total_quantity)
SELECT 
  product_id,
  total_quantity
FROM 
  aggregated_product_sales 
ON CONFLICT (product_id) DO UPDATE
  SET total_quantity = product_sales_summary.total_quantity + EXCLUDED.total_quantity;

-- Delete temporary tables
DROP TABLE new_product_sales;
DROP TABLE aggregated_product_sales;