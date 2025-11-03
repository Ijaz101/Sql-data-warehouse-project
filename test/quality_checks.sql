/* 
=============================================================================================
Quality Checks
=============================================================================================
Script Purpose: 
  This script performs various quality checks for data consistency, accuracy, 
  and standardization accross the 'silver' layer. It includes checks for:
  - NUll or duplicated primary keys.
  - Unwanted spaces in string fields.
  - Data standardization and consistency.
  - Invalid data ranges and orders.
  - Data consistency between related fields.

Usage Notes: 
  - Run these checks after data loading Silver Layer.
  - Investigate and resolve any discrepancies found during the checks.
=============================================================================================
*/

-- ==========================================================================================
Checking 'silver.crm_cust_info'
-- ==========================================================================================
-- Check for NULLS or duplicates in primary key
-- Expectation: No Results
SELECT cst_id, COUNT(*)  
FROM Silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted spaces
-- Expectation: No Results
SELECT cst_key
FROM Silver.crm_prd_info
WHERE cst_key != TRIM(cst_key);

-- Data Standardization & Consistency
SELECT DISTINCT cst_marital_status
FROM Silver.crm_cust_info;

-- ===========================================================================================
-- Checking 'silver.crm_prd_info'
-- ===========================================================================================
-- Check for NULLS or DUplicates in Primary Key
-- Expectation: No results
SELECT prd_id, COUNT(*)  
FROM Silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted spaces
-- Expectation: No Results
SELECT prd_nm
FROM Silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLS or Negative Values in Cost
-- Expectaion: No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM Silver.crm_cust_info;

-- Check for Invalid Date Orders( Start Date > End Date)
-- Expectation: No results
SELECT * FROM Silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;

-- ===========================================================================================
-- Checking 'silver.crm_sales_details'
-- ===========================================================================================
-- Check for Invalid Dates
--Expectation: No Invalid Dates


