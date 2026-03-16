/*This script is for the silver layer tables ingestion.
Data transformations are performed.
There are quality checks for each table as well*/

use DataWarehouse;
GO

create or alter procedure silver.load_silver as
begin
	declare @start_time datetime, @end_time datetime;
	begin try
		--Inserting into bronze layer tables
		print '========================================='
		print 'Loading silver layer ....'
		print '========================================='
		print '>>> Truncating table : crm_cust_info ....'
		set @start_time= getdate();
		truncate table silver.crm_cust_info;

		print '>>> INSERTING DATA INTO SILVER LAYER TABLE : crm_cust_info ....'
		insert into [DataWarehouse].[silver].[crm_cust_info] ( cst_id
			  ,cst_key
			  ,cst_firstname
			  ,cst_lastname
			  ,cst_material_status
			  ,cst_gndr
			  ,cst_create_date)

			  select cst_id ,
			  cst_key,
			  trim(cst_firstname) as cst_firstname,
			  trim(cst_lastname) as cst_lastname,
			  case when upper(trim(cst_material_status))='S' then 'Single'
			  when upper(trim(cst_material_status))='M' then 'Married'
			  else 'n/a' 
			  end as cst_material_status,
			  case when upper(trim(cst_gndr))='F' then 'Female'
			  when upper(trim(cst_gndr))='M' then 'Male'
			  else 'n/a' 
			  end as cst_gndr,
			  cst_create_date
			  from (
			  select *,
			  row_number() over(partition by cst_id order by cst_create_date desc ) as rk
			  from bronze.crm_cust_info
			  ) t
			  where rk = 1 and
			  cst_id is not null;


		-------- Data quality checks --------
		--Check  for nulls and duplicates in primary keys and columns
		select distinct cst_material_status from [DataWarehouse].[silver].[crm_cust_info];
		select distinct cst_gndr from [DataWarehouse].[silver].[crm_cust_info];

		select cst_id ,count(*) as dupes from [DataWarehouse].[silver].[crm_cust_info]  group by cst_id having  count(*)>1;
		select count(*) from [DataWarehouse].[silver].[crm_cust_info] where [cst_id] is null ;

		--Checks for spaces in strings 
		select [cst_firstname] from [DataWarehouse].[silver].[crm_cust_info] where [cst_firstname] !=trim([cst_firstname]);
		select [cst_lastname] from [DataWarehouse].[silver].[crm_cust_info] where [cst_lastname]!=trim([cst_lastname]);

		--select * from DataWarehouse.bronze.crm_prd_info;

		print '>>> Truncating table : crm_prd_info ....'
		truncate table silver.crm_prd_info;
		print '>>> INSERTING DATA INTO SILVER LAYER TABLE : crm_prd_info ....'
		insert into silver.crm_prd_info (
		prd_id ,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt) 
		select prd_id,
		replace(substring(prd_key,1,5), '-','_') as cat_id,
		substring(prd_key,7,len(prd_nm)) as prd_key,
		prd_nm ,
		isnull(prd_cost,0) as prd_cost,
		case upper(trim(prd_line)) when 'R' then 'Road'
		when 'M' then 'Mountain'
		when  'S' then 'Other Sales'
		when 'T' then 'Touring' else 'n/a'
		end as prd_line ,
		cast(prd_start_dt as date) prd_start_dt,
		cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) as date) as prd_end_dt
		from DataWarehouse.bronze.crm_prd_info;

		-------- Data quality checks --------
		select * from silver.crm_prd_info;
		--checking NULL values
		select * from silver.crm_prd_info where prd_line is null;
		select * from silver.crm_prd_info where prd_id is null;
		select distinct prd_line from silver.crm_prd_info; 

		--checking dates anomalies
		select * from silver.crm_prd_info where prd_start_dt>prd_end_dt;

		--checking strings
		select * from silver.crm_prd_info where prd_line != trim(prd_line);


		--checking negative values
		select * from silver.crm_prd_info
		where prd_cost<0 or prd_cost is null;


		----select * from bronze.crm_sales_details ;

		--------------------------------------------------
		print '>>> Truncating table : crm_sales_details ....'
		truncate table silver.crm_sales_details;
		print '>>> INSERTING DATA INTO SILVER LAYER TABLE : crm_sales_details ....'
		insert into silver.crm_sales_details (
		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id ,
		sls_order_dt ,
		sls_ship_dt ,
		sls_due_dt ,
		sls_sales ,
		sls_quantity ,
		sls_price )
		select sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case when sls_order_dt =0 or len(sls_order_dt)!=8 then NULL 
		else cast(cast(sls_order_dt as varchar) as date) 
		end as sls_order_dt,
		case when sls_ship_dt =0 or len(sls_ship_dt)!=8 then NULL 
		else cast(cast(sls_ship_dt as varchar) as date) 
		end as sls_ship_dt,
		case when sls_due_dt =0 or len(sls_due_dt)!=8 then NULL 
		else cast(cast(sls_due_dt as varchar) as date) 
		end as sls_due_dt,
		case when sls_price is null or sls_price=0 then  sls_sales/nullif(sls_quantity,0)
		when sls_price<0 then abs(sls_price) else sls_price end as sls_price,
		sls_quantity,
		case when sls_sales is null or sls_sales<=0 or sls_sales!= sls_quantity * abs(sls_price) 
		then sls_quantity * abs(sls_price) else sls_sales end as sls_sales
		from bronze.crm_sales_details;

		-------- Data quality checks --------
		--select * from silver.crm_sales_details;
		--checking NULL and negative values
		select * from silver.crm_sales_details
		where sls_sales is null or sls_sales<0 
		or sls_price is null or sls_price<=0
		or sls_quantity<=0 or sls_quantity is null;

		-------------------------------------------
		print '>>> Truncating table : erp_cust_az12 ....'
		truncate table silver.erp_cust_az12;

		print '>>> INSERTING DATA INTO SILVER LAYER TABLE : erp_cust_az12 ....'
		insert into silver.erp_cust_az12 (
		cid,
		bdate,
		gen)
		select
		case when trim(cid) not like 'AW000%' then substring(cid,4,len(cid)) else cid end as cid,
		case when bdate> getdate() then null else bdate end as bdate,
		CASE WHEN upper(trim(gen)) in ( 'F','Female') THEN 'Female'
		WHEN upper(trim(gen)) in ('M' ,'Male') THEN 'Male'
		else 'n/a'
		END AS gen
		from bronze.erp_cust_az12;


		-------- Data quality checks --------
		--select * from silver.erp_cust_az12;
		--checking dates and gen values

		select * from silver.erp_cust_az12 where bdate > getdate();
		select gen from  silver.erp_cust_az12 where gen not in ('Female','Male','n/a');

		-------------------------------------------
		print '>>> Truncating table : erp_loc_a101 ....'
		truncate table silver.erp_loc_a101;

		print '>>> INSERTING DATA INTO SILVER LAYER TABLE : erp_loc_a101 ....'

		insert into silver.erp_loc_a101(
		cid,
		cntry)
		select replace(cid,'-','') as cid,
		case when trim(cntry) in ('USA','United States','US') then 'United States'
		when trim(cntry) = 'DE' then 'Germany'
		when trim(cntry) is null or trim(cntry)='  ' then 'n/a'
		else cntry end as cntry
		from bronze.erp_loc_a101;

		-------- Data quality checks --------
		--select * from silver.erp_loc_a101;
		select  * from silver.erp_loc_a101
		where cid not in (select distinct cid from silver.crm_cust_info);


		--------------------------------------
		print '>>> Truncating table : erp_px_cat_g1v2 ....'
		truncate table silver.erp_px_cat_g1v2;

		print '>>> INSERTING DATA INTO SILVER LAYER TABLE : erp_px_cat_g1v2 ....'
		insert into silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance)
		select * from bronze.erp_px_cat_g1v2;
		set @end_time= getdate();

		PRINT '>>> SILVER LAYER LOADED SUCCESSFULLY <<<'
		print 'Silver layer loading duration ' + cast(datediff(second,@start_time,@end_time) as varchar)
		PRINT '>>> DATA QUALITY CHECKS PASSED <<<'
	end try
	begin catch 
		print '####################################################'
		print '~~ There is an issue with loading the silver layer '
		print '####################################################'
		print 'ERROR MESSAGE : ' + ERROR_MESSAGE()
		print 'ERROR NUMBER : ' + cast(ERROR_NUMBER() as varchar)
		print 'ERROR STATE : ' + cast(error_state() as varchar)
	end catch
end 

exec silver.load_silver