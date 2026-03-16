/*
This SQL file performs Data Quality Checks for the Silver Layer.

Its purpose is to validate and ensure that the data transformed from the Bronze layer
meets the required quality standards before being used in the Silver layer. These checks
include validations such as null values, incorrect formats, duplicate records, and
inconsistent or invalid data. The objective is to guarantee reliable, clean, and
consistent data for downstream processes and analytics.
*/

-------- Data quality checks --------

-- =====================================================
-- CRM CUSTOMER TABLE CHECKS (silver.crm_cust_info)
-- Validate customer data quality: duplicates, NULL keys,
-- unexpected categorical values, and string formatting issues
-- =====================================================

-- Check distinct values for categorical fields
select distinct cst_material_status from [DataWarehouse].[silver].[crm_cust_info];
select distinct cst_gndr from [DataWarehouse].[silver].[crm_cust_info];

-- Check duplicates in primary key
select cst_id ,count(*) as dupes 
from [DataWarehouse].[silver].[crm_cust_info]  
group by cst_id 
having count(*)>1;

-- Check NULL values in primary key
select count(*) 
from [DataWarehouse].[silver].[crm_cust_info] 
where [cst_id] is null;

-- Check leading/trailing spaces in string fields
select [cst_firstname] 
from [DataWarehouse].[silver].[crm_cust_info] 
where [cst_firstname] != trim([cst_firstname]);

select [cst_lastname] 
from [DataWarehouse].[silver].[crm_cust_info] 
where [cst_lastname] != trim([cst_lastname]);



-- =====================================================
-- CRM PRODUCT TABLE CHECKS (silver.crm_prd_info)
-- Validate product attributes: NULL fields, date logic,
-- string formatting and cost anomalies
-- =====================================================

-- Check NULL values
select * from silver.crm_prd_info where prd_line is null;
select * from silver.crm_prd_info where prd_id is null;

-- Validate product line values
select distinct prd_line from silver.crm_prd_info; 

-- Check date anomalies (start date after end date)
select * 
from silver.crm_prd_info 
where prd_start_dt > prd_end_dt;

-- Check string formatting issues
select * 
from silver.crm_prd_info 
where prd_line != trim(prd_line);

-- Check negative or NULL cost values
select * 
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null;



-- =====================================================
-- CRM SALES TABLE CHECKS (silver.crm_sales_details)
-- Validate sales metrics: NULL values and invalid
-- negative or zero measures
-- =====================================================

-- Check NULL or negative measures
select * 
from silver.crm_sales_details
where sls_sales is null or sls_sales < 0 
or sls_price is null or sls_price <= 0
or sls_quantity <= 0 or sls_quantity is null;



-- =====================================================
-- ERP CUSTOMER TABLE CHECKS (silver.erp_cust_az12)
-- Validate customer demographic data such as
-- birth dates and gender values
-- =====================================================

-- Check invalid future birth dates
select * 
from silver.erp_cust_az12 
where bdate > getdate();

-- Check allowed gender values
select gen 
from silver.erp_cust_az12 
where gen not in ('Female','Male','n/a');



-- =====================================================
-- ERP LOCATION TABLE CHECKS (silver.erp_loc_a101)
-- Validate referential integrity between location
-- records and CRM customer table
-- =====================================================

-- Check for location records without matching customers
select * 
from silver.erp_loc_a101
where cid not in (select distinct cid from silver.crm_cust_info);
