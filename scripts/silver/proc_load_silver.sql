CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
	proc_start_time TIMESTAMP;
	proc_end_time TIMESTAMP;
	task_start_time TIMESTAMP;
	task_end_time TIMESTAMP;
BEGIN
	proc_start_time := clock_timestamp();
	RAISE NOTICE '================================================';
	RAISE NOTICE 'Loading Silver Layer';
	RAISE NOTICE '================================================';

	RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading CRM Tables';
	RAISE NOTICE '------------------------------------------------';
	-- Loading silver.crm_cust_info
	task_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
	INSERT INTO
		SILVER.CRM_CUST_INFO (
			CST_ID,
			CST_KEY,
			CST_FIRSTNAME,
			CST_LASTNAME,
			CST_MARITAL_STATUS,
			CST_GNDR,
			CST_CREATE_DATE
		)
	SELECT
		CST_ID,
		CST_KEY,
		TRIM(CST_FIRSTNAME) AS CST_FIRSTNAME,
		TRIM(CST_LASTNAME) AS CST_LASTNAME,
		CASE
			WHEN UPPER(TRIM(CST_MARITAL_STATUS)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(CST_MARITAL_STATUS)) = 'M' THEN 'Married'
			ELSE 'unknown'
		END AS CST_MARITAL_STATUS, -- Normalize marital status values to readable format
		CASE
			WHEN UPPER(TRIM(CST_GNDR)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(CST_GNDR)) = 'M' THEN 'Male'
			ELSE 'unknown'
		END AS CST_GNDR, -- Normalize gender values to readable format
		CST_CREATE_DATE
	FROM
		(
			SELECT
				*,
				ROW_NUMBER() OVER (
					PARTITION BY
						CST_ID
					ORDER BY
						CST_CREATE_DATE DESC
				) AS FLAG_LAST
			FROM
				BRONZE.CRM_CUST_INFO
			WHERE
				CST_ID IS NOT NULL
		) T
	WHERE
		FLAG_LAST = 1;-- Select the most recent record per customer
	task_end_time := clock_timestamp();
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM task_end_time - task_start_time);
	RAISE NOTICE '------------------------------------------------';
	
	-- Loading silver.crm_prd_info
	task_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
	INSERT INTO
		SILVER.CRM_PRD_INFO (
			PRD_ID,
			CAT_ID,
			PRD_KEY,
			PRD_NM,
			PRD_COST,
			PRD_LINE,
			PRD_START_DT,
			PRD_END_DT
		)
	SELECT
		PRD_ID,
		REPLACE(SUBSTRING(PRD_KEY, 1, 5), '-', '_') AS CAT_ID, -- Extract category ID
		SUBSTRING(PRD_KEY, 7, LENGTH(PRD_KEY)) AS PRD_KEY, -- Extract product key
		PRD_NM,
		COALESCE(PRD_COST, 0) AS PRD_COST,
		CASE
			WHEN UPPER(TRIM(PRD_LINE)) = 'M' THEN 'Mountain'
			WHEN UPPER(TRIM(PRD_LINE)) = 'R' THEN 'Road'
			WHEN UPPER(TRIM(PRD_LINE)) = 'S' THEN 'Other Sales'
			WHEN UPPER(TRIM(PRD_LINE)) = 'T' THEN 'Touring'
			ELSE 'unknown'
		END AS PRD_LINE, -- Map product line codes to descriptive values
		CAST(PRD_START_DT AS DATE) AS PRD_START_DT,
		CAST(
			LEAD(PRD_START_DT) OVER (
				PARTITION BY
					PRD_KEY
				ORDER BY
					PRD_START_DT
			) - INTERVAL '1 DAY' AS DATE
		) AS PRD_END_DT -- Calculate end date as one day before the next start date
	FROM
		BRONZE.CRM_PRD_INFO;
	task_end_time := clock_timestamp();
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM task_end_time - task_start_time);
	RAISE NOTICE '------------------------------------------------';
	
	-- Loading crm_sales_details
	task_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
	INSERT INTO
		SILVER.CRM_SALES_DETAILS (
			SLS_ORD_NUM,
			SLS_PRD_KEY,
			SLS_CUST_ID,
			SLS_ORDER_DT,
			SLS_SHIP_DT,
			SLS_DUE_DT,
			SLS_SALES,
			SLS_QUANTITY,
			SLS_PRICE
		)
	SELECT
		SLS_ORD_NUM,
		SLS_PRD_KEY,
		SLS_CUST_ID,
		CASE
			WHEN SLS_ORDER_DT = 0
			OR LENGTH(SLS_ORDER_DT::TEXT) != 8 THEN NULL
			ELSE CAST(CAST(SLS_ORDER_DT AS TEXT) AS DATE)
		END AS SLS_ORDER_DT,
		CASE
			WHEN SLS_SHIP_DT = 0
			OR LENGTH(SLS_SHIP_DT::TEXT) != 8 THEN NULL
			ELSE CAST(CAST(SLS_SHIP_DT AS TEXT) AS DATE)
		END AS SLS_SHIP_DT,
		CASE
			WHEN SLS_DUE_DT = 0
			OR LENGTH(SLS_DUE_DT::TEXT) != 8 THEN NULL
			ELSE CAST(CAST(SLS_DUE_DT AS TEXT) AS DATE)
		END AS SLS_DUE_DT,
		CASE
			WHEN SLS_SALES IS NULL
			OR SLS_SALES <= 0
			OR SLS_SALES != SLS_QUANTITY * ABS(SLS_PRICE) THEN SLS_QUANTITY * ABS(SLS_PRICE)
			ELSE SLS_SALES
		END AS SLS_SALES, -- Recalculate sales if original value is missing or incorrect
		SLS_QUANTITY,
		CASE
			WHEN SLS_PRICE IS NULL
			OR SLS_PRICE <= 0 THEN SLS_SALES / NULLIF(SLS_QUANTITY, 0)
			ELSE SLS_PRICE -- Derive price if original value is invalid
		END AS SLS_PRICE
	FROM
		BRONZE.CRM_SALES_DETAILS;
	task_end_time := clock_timestamp();
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM task_end_time - task_start_time);
	RAISE NOTICE '------------------------------------------------';
	
	-- Loading erp_cust_az12
	task_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';
	INSERT INTO
		SILVER.ERP_CUST_AZ12 (CID, BDATE, GEN)
	SELECT
		CASE
			WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LENGTH(CID)) -- Remove 'NAS' prefix if present
			ELSE CID
		END AS CID,
		CASE
			WHEN BDATE > CURRENT_TIMESTAMP THEN NULL
			ELSE BDATE
		END AS BDATE, -- Set future birthdates to NULL
		CASE
			WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'unknown'
		END AS GEN -- Normalize gender values and handle unknown cases
	FROM
		BRONZE.ERP_CUST_AZ12;
	task_end_time := clock_timestamp();
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM task_end_time - task_start_time);
	RAISE NOTICE '------------------------------------------------';
	
	-- Loading erp_loc_a101
	task_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';
	INSERT INTO
		SILVER.ERP_LOC_A101 (CID, CNTRY)
	SELECT
		REPLACE(CID, '-', '') AS CID,
		CASE
			WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
			WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(CNTRY) = ''
			OR CNTRY IS NULL THEN 'unknown'
			ELSE TRIM(CNTRY)
		END AS CNTRY -- Normalize and Handle missing or blank country codes
	FROM
		BRONZE.ERP_LOC_A101;
	task_end_time := clock_timestamp();
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM task_end_time - task_start_time);
	RAISE NOTICE '------------------------------------------------';
	
	-- Loading erp_px_cat_g1v2
	task_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
	INSERT INTO
		SILVER.ERP_PX_CAT_G1V2 (ID, CAT, SUBCAT, MAINTENANCE)
	SELECT
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
	FROM
		BRONZE.ERP_PX_CAT_G1V2;
	task_end_time := clock_timestamp();
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM task_end_time - task_start_time);
	RAISE NOTICE '------------------------------------------------';

	proc_end_time := clock_timestamp();
	RAISE NOTICE '================================================';
	RAISE NOTICE 'Loading Silver Layer is Completed';
	RAISE NOTICE '   - Total Procedure Duration: % seconds', EXTRACT(EPOCH FROM proc_end_time - proc_start_time);
	RAISE NOTICE '================================================';
EXCEPTION
	WHEN OTHERS THEN
		RAISE NOTICE '================================================';
		RAISE EXCEPTION 'An error occurred during loading silver layer';
		RAISE EXCEPTION 'SQLSTATE % - %', SQLSTATE, SQLERRM;
		RAISE NOTICE '================================================';
END;
$$;