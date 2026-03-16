--bulk insert into created tables

create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime, @end_time datetime;
	begin try
		print '========================================================================';
		print 'Loading Bronze Layer';
		print '========================================================================';
		
		set @start_time = getdate();
		print '>>>>>>>>  Loading CRM tables .....';
		truncate table bronze.crm_cust_info;
		Bulk insert bronze.crm_cust_info
		from 'E:\DWH_project_sql_server\source_crm\cust_info.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock);

		select count(*) from bronze.crm_cust_info;
		set @end_time = getdate();
		print 'the loading duration is ' + cast(datediff(second, @end_time,@start_time) as nvarchar) + 'seconds';

		set @start_time = getdate();
		truncate table bronze.crm_prd_info;
		bulk insert bronze.crm_prd_info
		from 'E:\DWH_project_sql_server\source_crm\prd_info.csv'
		with(
		firstrow= 2,
		fieldterminator=',',
		tablock);
		select count(*) from bronze.crm_prd_info;
		set @end_time = getdate();
		print 'the loading duration is ' + cast(datediff(second, @end_time,@start_time) as nvarchar) + 'seconds';

		set @start_time = getdate();
		truncate table bronze.crm_sales_details;
		bulk insert bronze.crm_sales_details
		from'E:\DWH_project_sql_server\source_crm\sales_details.csv'
		with (
		firstrow= 2,
		fieldterminator=',',
		tablock );
		select count(*) from bronze.crm_sales_details;
		set @end_time = getdate();
		print 'the loading duration is ' + cast(datediff(second, @end_time,@start_time) as nvarchar) + 'seconds';

		print '>>>>>>>>  Loading ERP tables .....';
		set @start_time = getdate();
		truncate table bronze.erp_cust_az12;
		bulk insert bronze.erp_cust_az12
		from 'E:\DWH_project_sql_server\source_erp\CUST_AZ12.csv'
		with (
		firstrow= 2,
		fieldterminator= ',',
		tablock);
		select count(*) from bronze.erp_cust_az12;

		set @end_time = getdate();
		print 'the loading duration is ' + cast(datediff(second, @end_time,@start_time) as nvarchar) + 'seconds';

		set @start_time = getdate();
		truncate table bronze.erp_loc_a101;
		bulk insert bronze.erp_loc_a101
		from 'E:\DWH_project_sql_server\source_erp\LOC_A101.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock);
		select count(*) from bronze.erp_loc_a101;

		set @end_time = getdate();
		print 'the loading duration is ' + cast(datediff(second, @end_time,@start_time) as nvarchar) + 'seconds';

		set @start_time = getdate();
		truncate table bronze.erp_px_cat_g1v2;
		bulk insert bronze.erp_px_cat_g1v2
		from 'E:\DWH_project_sql_server\source_erp\PX_CAT_G1V2.csv'
		with (
		firstrow=2,
		fieldterminator =',',
		tablock);
		select count(*) from bronze.erp_px_cat_g1v2;

		set @end_time = getdate();
		print 'the loading duration is ' + cast(datediff(second, @end_time,@start_time) as nvarchar) + 'seconds';
	end try
	begin catch
		print '********** ERROR OCCURED DURING LOADING BRONZE LAYER ****************';
		print 'Error message' + error_message();
		print 'Error message' + cast(error_state() as nvarchar);
	end catch
end

exec bronze.load_bronze