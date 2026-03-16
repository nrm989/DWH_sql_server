/*
This SQL file builds the Gold Layer of the Data Warehouse.

The Gold layer contains curated, business-ready data models designed for
analytics, reporting, and business intelligence. Data from the Silver layer
is aggregated, structured, and organized into analytical models (such as
fact and dimension views) to support dashboards, reporting tools, and
decision-making processes. The objective of this layer is to provide
high-quality, trusted, and optimized data for end users and BI applications.
*/

--CUSTOMER DIMENSION
create view gold.dim_customers as
select row_number() over(order by cst_id ) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as firstname,
ci.cst_lastname as lastname,
ca.bdate as birthdate,
la.cntry as country,
ci.cst_material_status as marital_status,
case when ci.cst_gndr != 'n/a' then ci.cst_gndr else coalesce(ca.gen, 'n/a')
end as gender,
ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca 
on ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
on la.cid=ci.cst_key;


--PRODUCTS DIMENTION USING ONLY THE RECENT ACTIVE PRODUCTS
create view gold.dim_products as
select row_number() over(order by ci.prd_start_dt,ci.prd_key) as product_key ,
ci.prd_id as product_id,
ecat.id as category_id,
ci.prd_key as product_number,
ci.prd_nm as product_name,
ecat.cat as category,
ecat.subcat as sub_category,
ci.prd_cost as cost,
ci.prd_line as product_line,
ci.prd_start_dt as starting_date,
ci.prd_end_dt as end_date,
ecat.maintenance as maintenance
from silver.erp_px_cat_g1v2 as ecat
join silver.crm_prd_info as ci
on ci.cat_id=ecat.id
where prd_end_dt is null;

-- FACT TABLE: SALES 
create view gold.fact_sales as 
select sls_ord_num as order_number,
p.product_key as product_key,
gc.customer_key as customer_key,
sls_order_dt as order_date,
sls_ship_dt as ship_date,
sls_due_dt as due_date,
sls_quantity as quantity,
sls_sales as sales_amount,
sls_price as price
from silver.crm_sales_details fs
join gold.dim_customers gc
on gc.customer_id=fs.sls_cust_id
join gold.dim_products p
on p.product_number=fs.sls_prd_key; 
