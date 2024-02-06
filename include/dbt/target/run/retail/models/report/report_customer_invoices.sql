
  
    

    create or replace table `sapient-cycling-413017`.`retail`.`report_customer_invoices`
    
    

    OPTIONS()
    as (
      -- report_customer_invoices.sql
SELECT
  c.country,
  c.iso,
  COUNT(fi.invoice_id) AS total_invoices,
  SUM(fi.total) AS total_revenue
FROM `sapient-cycling-413017`.`retail`.`fct_invoices` fi
JOIN `sapient-cycling-413017`.`retail`.`dim_customer` c ON fi.customer_id = c.customer_id
GROUP BY c.country, c.iso
ORDER BY total_revenue DESC
LIMIT 10
    );
  