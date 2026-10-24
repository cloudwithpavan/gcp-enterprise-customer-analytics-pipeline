-- Run these queries in BigQuery after the DAG succeeds.
SELECT * FROM `gcp-data-project-510810.practice.customers_staging`;

SELECT * FROM `gcp-data-project-510810.practice.customers_cleaned`
ORDER BY customer_id;

SELECT * FROM `gcp-data-project-510810.practice.customer_city_report`
ORDER BY city;
