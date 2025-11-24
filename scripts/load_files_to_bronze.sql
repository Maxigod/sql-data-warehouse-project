/*===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from csv files to bronze tables.

Parameters:
    None. 

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
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
	RAISE NOTICE 'Loading Bronze Layer';
	RAISE NOTICE '================================================';

	RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading CRM Tables';
	RAISE NOTICE '------------------------------------------------';

	task_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;
	RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
	COPY bronze.crm_cust_info FROM 'C:\Users\Maximiliano\Documents\DataEngineerPractice\sql-data-warehouse-project\datasets\source_crm\cust_info.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
	task_end_time := clock_timestamp();
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM task_end_time - task_start_time);
	RAISE NOTICE '------------------------------------------------';

	task_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;
	RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
	COPY bronze.crm_prd_info FROM 'C:\Users\Maximiliano\Documents\DataEngineerPractice\sql-data-warehouse-project\datasets\source_crm\prd_info.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
	task_end_time := clock_timestamp();
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM task_end_time - task_start_time);
	RAISE NOTICE '------------------------------------------------';
	
	task_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;
	RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
	COPY bronze.crm_sales_details FROM 'C:\Users\Maximiliano\Documents\DataEngineerPractice\sql-data-warehouse-project\datasets\source_crm\sales_details.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
	task_end_time := clock_timestamp();
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM task_end_time - task_start_time);
	RAISE NOTICE '------------------------------------------------';
	
	task_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;
	RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
	COPY bronze.erp_cust_az12 FROM 'C:\Users\Maximiliano\Documents\DataEngineerPractice\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
	task_end_time := clock_timestamp();
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM task_end_time - task_start_time);
	RAISE NOTICE '------------------------------------------------';
	
	task_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;
	RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
	COPY bronze.erp_loc_a101 FROM 'C:\Users\Maximiliano\Documents\DataEngineerPractice\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
	task_end_time := clock_timestamp();
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM task_end_time - task_start_time);
	RAISE NOTICE '------------------------------------------------';
	
	task_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
	COPY bronze.erp_px_cat_g1v2 FROM 'C:\Users\Maximiliano\Documents\DataEngineerPractice\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
	task_end_time := clock_timestamp();
	RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM task_end_time - task_start_time);
	RAISE NOTICE '------------------------------------------------';
	
	proc_end_time := clock_timestamp();
	RAISE NOTICE '================================================';
	RAISE NOTICE 'Loading Bronze Layer is Completed';
	RAISE NOTICE '   - Total Procedure Duration: % seconds', EXTRACT(EPOCH FROM proc_end_time - proc_start_time);
	RAISE NOTICE '================================================';

EXCEPTION
	WHEN OTHERS THEN
		RAISE NOTICE '================================================';
		RAISE EXCEPTION 'An error occurred during loading bronze layer';
		RAISE EXCEPTION 'SQLSTATE % - %', SQLSTATE, SQLERRM;
		RAISE NOTICE '================================================';
END;
$$;