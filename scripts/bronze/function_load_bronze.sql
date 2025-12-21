/*
===============================================================================
Function Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This script implements a PostgreSQL function to load data into the Bronze layer. The function orchestrates the ingestion process by truncating existing Bronze tables, bulk-loading source data, handling errors using exception blocks, and logging execution details such as load status and duration. This approach ensures repeatable, controlled, and auditable data ingestion, similar to stored procedures used in enterprise ETL pipelines.

Usage Example:
   select bronze.load_bronze();
===============================================================================
*/
create or replace function bronze.load_bronze()
returns void as $$
declare
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	duration INTERVAL;
begin
	raise notice '=====================================';
	raise notice 'Loading Bronze Layer';
	raise notice '=====================================';

	raise notice '-------------------------------------';
	raise notice 'Loading CRM Tables';
	raise notice '-------------------------------------';

	start_time :=clock_timestamp();
	raise notice '>> Truncating table : bronze.crm_cust_info ';
	truncate table bronze.crm_cust_info;
	raise notice '## Inserting Data Into: bronze.crm_cust_info';
	copy bronze.crm_cust_info
	from 'C:\Program Files\PostgreSQL\18\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	delimiter ','
	csv header
	;
	end_time :=clock_timestamp();
	duration :=end_time-start_time;
	raise notice 'crm_cust_info loaded in % seconds',extract(epoch from duration);

	start_time :=clock_timestamp();
	raise notice '>> Truncating table : bronze.crm_prd_info ';
	truncate table bronze.crm_prd_info;
	raise notice '## Inserting Data Into: bronze.crm_prd_info';
	copy bronze.crm_prd_info
	from 'C:\Program Files\PostgreSQL\18\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	delimiter ','
	csv header
	;
	end_time :=clock_timestamp();
	duration :=end_time-start_time;
	raise notice 'crm_prd_info loaded in % seconds',extract(epoch from duration);


	start_time :=clock_timestamp();
	raise notice '>> Truncating table : bronze.crm_sales_details ';
	truncate table bronze.crm_sales_details;
	raise notice '## Inserting Data Into: bronze.crm_sales_details';
	copy bronze.crm_sales_details
	from 'C:\Program Files\PostgreSQL\18\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	delimiter ','
	csv header
	;
	end_time :=clock_timestamp();
	duration :=end_time-start_time;
	raise notice 'crm_sales_details loaded in % seconds',extract(epoch from duration);

	
	raise notice '-------------------------------------';
	raise notice 'Loading ERP Tables';
	raise notice '-------------------------------------';

	start_time :=clock_timestamp();
	raise notice '>> Truncating table : bronze.erp_CUST_AZ12 ';
	truncate table bronze.erp_CUST_AZ12;
	raise notice '## Inserting Data Into: bronze.erp_CUST_AZ12';
	copy bronze.erp_CUST_AZ12
	from 'C:\Program Files\PostgreSQL\18\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	delimiter ','
	csv header
	;
	end_time :=clock_timestamp();
	duration :=end_time-start_time;
	raise notice 'erp_CUST_AZ12 loaded in % seconds',extract(epoch from duration);

	start_time :=clock_timestamp();
	raise notice '>> Truncating table : bronze.erp_LOC_A101';
	truncate table bronze.erp_LOC_A101;
	raise notice '## Inserting Data Into: bronze.erp_LOC_A101 ';
	copy bronze.erp_LOC_A101
	from 'C:\Program Files\PostgreSQL\18\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	delimiter ','
	csv header
	;
	end_time :=clock_timestamp();
	duration :=end_time-start_time;
	raise notice 'erp_LOC_A101 loaded in % seconds',extract(epoch from duration);

	start_time :=clock_timestamp();
	raise notice '>> Truncating table : bronze.erp_PX_CAT_G1V2';
	truncate table bronze.erp_PX_CAT_G1V2;
	raise notice '## Inserting Data Into: bronze.erp_PX_CAT_G1V2';
	copy bronze.erp_PX_CAT_G1V2
	from 'C:\Program Files\PostgreSQL\18\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	delimiter ','
	csv header
	;
	end_time :=clock_timestamp();
	duration :=end_time-start_time;
	raise notice 'erp_PX_CAT_G1V2 loaded in % seconds',extract(epoch from duration);

exception 
when others then 
raise notice 'Error : %',SQLERRM;
RAISE;
end;
$$ language plpgsql ;
