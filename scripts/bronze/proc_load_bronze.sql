/*
==================================================================================
Stored procedure: Load Bronze Layer (Source -> Bronze)
==================================================================================
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
  -Truncates the bronze tables before loading data.
  -Uses the 'BULK INSERT' command to load data from csv Files to bronze tables.
Parameters:
  None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
  EXEC bronze.load_bronze;
====================================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================================================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '----------------------------------------------------------------------------------------------------------------';
		PRINT 'Loading CRM Table';
		PRINT '----------------------------------------------------------------------------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT 'Truncating Table :Bronze.crm_cust_info';
		TRUNCATE TABLE Bronze.crm_cust_info

		PRINT 'Inserting Data Into: Bronze.crm_cust_info';
		BULK INSERT Bronze.crm_cust_info
		FROM "C:\Users\Testuser\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv"
		WITH(
			FIRSTROW =2,
			FIELDTERMINATOR= ',',
			TABLOCK
			);
		SET @end_time = GETDATE();

		PRINT 'Loading duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------';
		
		SET @start_time = GETDATE();
		PRINT 'Truncating Table :Bronze.crm_prd_info';
		TRUNCATE TABLE Bronze.crm_prd_info

		PRINT 'Inserting Data Into: Bronze.crm_prd_info';
		BULK INSERT Bronze.crm_prd_info
		FROM "C:\Users\Testuser\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv"
		WITH(
			FIRSTROW =2,
			FIELDTERMINATOR= ',',
			TABLOCK
			);

		SET @end_time = GETDATE();
		PRINT 'Loading duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table :Bronze.crm_sales_details';
		TRUNCATE TABLE Bronze.crm_sale_details
	
		PRINT 'Inserting Data Into: Bronze.crm_sales_details';
		BULK INSERT Bronze.crm_sale_details
		FROM "C:\Users\Testuser\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv"
		WITH(
			FIRSTROW =2,
			FIELDTERMINATOR= ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT 'Loading duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------';
		
		SET @start_time = GETDATE();
		PRINT 'Truncating Table :Bronze.erp_CUST_AZ12';
		TRUNCATE TABLE Bronze.erp_CUST_AZ12
	
		PRINT 'Inserting Data Into: Bronze.erp_CUST_AZ12';
		BULK INSERT Bronze.erp_CUST_AZ12
		FROM "C:\Users\Testuser\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv"
		WITH(
			FIRSTROW =2,
			FIELDTERMINATOR= ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT 'Loading duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table :Bronze.erp_LOC_A101';
		TRUNCATE TABLE Bronze.erp_LOC_A101
	
		PRINT 'Inserting Data Into: Bronze.erp_LOC_A101';
		BULK INSERT Bronze.erp_LOC_A101
		FROM "C:\Users\Testuser\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv"
		WITH(
			FIRSTROW =2,
			FIELDTERMINATOR= ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT 'Loading duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table :Bronze.erp_PX_CAT_G1V2';
		TRUNCATE TABLE Bronze.erp_PX_CAT_G1V2

		PRINT 'Inserting Data Into: Bronze.erp_PX_CAT_G1V2';
		BULK INSERT Bronze.erp_PX_CAT_G1V2
		FROM "C:\Users\Testuser\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv"
		WITH(
			FIRSTROW =2,
			FIELDTERMINATOR= ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT 'Loading duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------';
		SET @batch_end_time = GETDATE();
		PRINT '----------------------------------------';
		PRINT 'Loading bronze layer is completed ';
		PRINT ' Total Duration ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' Seconds';
		PRINT '-----------------------------------------';
	END TRY
	BEGIN CATCH
	PRINT '========================================================================================================================';
	PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);

	

	END CATCH
	END
