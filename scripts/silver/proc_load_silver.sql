/*
====================================================================================
Stored Procedure: Load silver layer(Bronze-> Silver)
===================================================================================
Script Purpose: 
  This stored procedure performs the ETL( Extract, Transform, Load) process to 
  populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed: 
  - Truncates silver tables.
  - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
  None.
  This store procedure does not accept any parameters or return any values.

Usage Example:
  EXEC Silver.load_silver;
====================================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN 
BEGIN TRY
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start DATETIME, @batch_end DATETIME;
	SET @batch_start = GETDATE();
	PRINT '======================================================';
	PRINT 'Loading Silver Layer ';
	PRINT '======================================================';

	PRINT '------------------------------------------------------';
	PRINT 'Loading CRM Tables'
	PRINT '------------------------------------------------------';
	
	SET @start_time = GETDATE();
	PRINT 'Truncating Table Silver.crm_cust_info';
	TRUNCATE TABLE Silver.crm_cust_info;
	PRINT 'Insering Data Into Silver.crm_cust_info';
	INSERT INTO Silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)

	SELECT cst_id,
	cst_key,
	TRIM(UPPER(cst_firstname)) as cst_firstname,
	TRIM((cst_lastname)) AS cst_lastname,
	CASE WHEN cst_marital_status = 'M' then 'Married'
		 WHEN cst_marital_status ='S' then 'Single'
		 ELSE 'N/A'
		 END AS cst_marital_status,
	CASE WHEN cst_gndr = 'M' then 'Male'
		 WHEN cst_gndr = 'F' then 'Female'
		 ELSE 'N/A'
		 END AS cst_gndr,
	cst_create_date
	 FROM(
	SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Flag_list 
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	)T
	WHERE FlaG_list =1; 
	SET @end_time = GETDATE();
	PRINT 'Loading duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)  + ' seconds';
	PRINT '----------------------------------------------------------------------------------';
	
	SET @start_time = GETDATE();
	PRINT 'Truncating Table Silver.crm_prd_info';
	TRUNCATE TABLE Silver.crm_prd_info;
	PRINT 'Inserting data Into Silver.crm_prd_info';
	INSERT INTO Silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt)
	SELECT prd_id,
	REPLACE(SUBSTRING(prd_key,1,5) , '-','_') AS cat_id,
	SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
	prd_nm,
	ISNULL(prd_cost,0) as prd_cost,
	CASE UPPER(trim(prd_line))
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'Other Sales'
		 WHEN 'T' THEN 'Touring'
		 ELSE 'N/A'
		 END AS prd_line,
	CAST(prd_start_dt AS DATE) as prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE)  as prd_end_dt 
	FROM Bronze.crm_prd_info;
	SET @end_time = GETDATE();
	PRINT 'Loading Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)  + ' seconds'


	SET @start_time = GETDATE();
	PRINT 'Truncating Table Silver.crm_sale_details';
	TRUNCATE TABLE Silver.crm_sale_details;
	PRINT 'Insering Data Into Silver.crm_sale_details';
	INSERT INTO Silver.crm_sale_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price)
	SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
	CASE WHEN sls_due_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
	CASE WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) 
		 THEN ABS(sls_price) * sls_quantity
		 ELSE sls_sales 
		 END AS sls_sales,
	sls_quantity,
	CASE WHEN sls_price <=0 OR sls_price IS NULL
		 THEN NULLIF(sls_sales,0)/ sls_quantity 
		 ELSE sls_price
		 END AS sls_price
	FROM Bronze.crm_sale_details;
	SET @end_time = GETDATE();
	PRINT 'Loading crm_sale_details duration: ' +  CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)  + ' seconds'

	PRINT '-------------------------------------------------------';
	PRINT 'Loading ERP Tables';
	PRINT '-------------------------------------------------------';

	SET @start_time = GETDATE();
	PRINT 'Truncating Table Silver.erp_Cust_AZ12';
	TRUNCATE TABLE Silver.erp_CUST_AZ12;
	PRINT 'Insering Data into Silver.erp_Cust_AZ12';
	INSERT INTO Silver.erp_CUST_AZ12(
	CID,
	BDATE,
	GEN)
	SELECT CASE WHEN CID LIKE '%NAS%' THEN SUBSTRING(CID,4,LEN(CID))
	ELSE CID
	END AS CID,
	CASE WHEN BDATE > GETDATE() THEN NULL
		 ELSE BDATE
		 END AS BDATE,
	CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'Female') THEN 'Female'
		 WHEN UPPER(TRIM(GEN)) IN ('M', 'Male') THEN 'Male'
		 ELSE 'N/A'
		 END AS GEN
	FROM Bronze.erp_CUST_AZ12;

	PRINT 'Truncating Table Silver.erp_LOC_A101';
	TRUNCATE TABLE Silver.erp_LOC_A101;
	PRINT 'Inserting Data Into Silver.erp_LOC_A101';
	INSERT INTO Silver.erp_LOC_A101(
	CID,
	CNTRY)

	SELECT REPLACE(CID, '-', '') AS CID,
	CASE WHEN CNTRY = 'DE' THEN 'Germany'
		 WHEN CNTRY IN ('USA', 'United States', 'US') THEN ('United States')
		 WHEN CNTRY IS NULL OR CNTRY ='' THEN 'N/A'
		 ELSE CNTRY
		 END AS CNTRY
		 FROM
	bronze.erp_LOC_A101;
	SET @end_time = GETDATE();
	PRINT 'Loading duration silver.erp_LOC_A101: ' +  CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)  + ' seconds'

	SET @start_time = GETDATE();
	  PRINT 'Truncating Table Silver.erp_PX_CAT_G1V2';
	  TRUNCATE TABLE Silver.erp_PX_CAT_G1V2;
	  PRINT 'Inserting Data Into Silver.erp_PX_CAT_G1V2';
	  INSERT INTO Silver.erp_PX_CAT_G1V2 (
	  ID,
	  CAT,
	  SUBCAT,
	  MAINTENANCE)
	  SELECT
	  ID, 
	  CAT,
	  SUBCAT,
	  MAINTENANCE 
	  FROM Bronze.erp_PX_CAT_G1V2;
	  SET @end_time = GETDATE();
	  PRINT 'Loading silver.erp_PX_CAT_G1V2' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)  + ' seconds';
	  SET @batch_end = GETDATE();
	  PRINT '--------------------------------------------------------------------------';
	  PRINT 'Loading Duration Silver layer ' + CAST(DATEDIFF(second, @batch_start, @batch_end) AS NVARCHAR) + ' seconds';
	  END TRY
	  BEGIN CATCH
	  PRINT '------------------------------------------';
	  PRINT 'Error occured during loading silver layer';
	  PRINT 'Error Message' + ERROR_MESSAGE();
	  PRINT 'Error State' + CAST(ERROR_STATE() AS VARCHAR);
	  PRINT 'Error Number' + CAST(ERROR_NUMBER() AS VARCHAR);
	  END CATCH
	  END

