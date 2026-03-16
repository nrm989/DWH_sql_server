/*
This SQL file performs Data Quality Checks for the Gold Layer.

The Gold Layer contains curated, business-ready views derived from the Silver Layer.
These checks ensure that the aggregated and transformed data is accurate
and reliable for reporting and analytics. Typical validations include verifying
referential integrity between fact and dimension tables, ensuring no NULLs or
unexpected values in key metrics, checking calculated aggregates, and confirming
that business rules applied during transformations are correctly enforced.
The goal is to guarantee that end users and BI tools can consume high-quality,
trusted data without errors or inconsistencies.
*/



select distinct gender from gold.dim_customers;

--Checking null values
select * from gold.fact_sales where order_number is null;

--Foreign keys integrity (Dimensions)
select * from gold.fact_sales f
left join gold.dim_customers dc
on dc.customer_key=f.customer_key
left join gold.dim_products dp
on f.product_key=dp.product_key;
