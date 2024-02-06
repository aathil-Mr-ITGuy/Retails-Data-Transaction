
  
    

    create or replace table `sapient-cycling-413017`.`retail`.`dim_datetime`
    
    

    OPTIONS()
    as (
      WITH datetime_cte AS (  
  SELECT DISTINCT
    InvoiceDate AS datetime_id,
    CASE
      WHEN LENGTH(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', InvoiceDate)) = 16 THEN
        -- Date format: "DD/MM/YYYY HH:MM"
        PARSE_DATETIME('%m/%d/%Y %H:%M', FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', InvoiceDate))
      WHEN LENGTH(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', InvoiceDate)) <= 14 THEN
        -- Date format: "MM/DD/YY HH:MM"
        PARSE_DATETIME('%m/%d/%y %H:%M', FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', InvoiceDate))
      ELSE
        NULL
    END AS date_part
  FROM `sapient-cycling-413017`.`retail`.`raw_invoices`
  WHERE InvoiceDate IS NOT NULL
)
SELECT
  datetime_id,
  date_part as datetime,
  EXTRACT(YEAR FROM date_part) AS year,
  EXTRACT(MONTH FROM date_part) AS month,
  EXTRACT(DAY FROM date_part) AS day,
  EXTRACT(HOUR FROM date_part) AS hour,
  EXTRACT(MINUTE FROM date_part) AS minute,
  EXTRACT(DAYOFWEEK FROM date_part) AS weekday
FROM datetime_cte
    );
  