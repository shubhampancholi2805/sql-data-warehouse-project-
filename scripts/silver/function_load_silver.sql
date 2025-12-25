/*
===============================================================================
Function: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This Function performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Usage Example:
   SELECT Silver.load_silver;
===============================================================================
*/
create or replace function silver.load_silver()
returns void as $$
declare
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	duration INTERVAL;
begin
-----------------------------------------------------------------
raise notice '=====================================';
	raise notice 'Loading Silver Layer';
	raise notice '=====================================';

	raise notice '-------------------------------------';
	raise notice 'Loading CRM Tables';
	raise notice '-------------------------------------';


	
--silver.crm_cust_info
start_time :=clock_timestamp();
raise notice '>> Truncating table : silver.crm_prd_info ';
truncate table silver.crm_cust_info;
raise notice '## Inserting Data Into: silver.crm_cust_info';
insert into silver.crm_cust_info(
cst_id ,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
)

select 
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case when cst_marital_status='S' then 'Single'
		when cst_marital_status='M' then 'Married'
		else 'n/a'
		end as cst_marital_status,
case when cst_gndr='M' then 'Male'
		when cst_gndr='F' then 'Female'
		else 'n/a'
		end as cst_gndr,
cst_create_date
from
(
SELECT * ,
row_number() over(partition by cst_id order by cst_create_date desc) as flag_number
from bronze.crm_cust_info 
)
where flag_number = 1;

end_time :=clock_timestamp();
duration :=end_time-start_time;
raise notice 'crm_cust_info loaded in % seconds',extract(epoch from duration);



--silver.crm_prd_info
start_time :=clock_timestamp();
raise notice '>> Truncating table : silver.crm_prd_info ';
truncate table silver.crm_prd_info;
raise notice '## Inserting Data Into: silver.crm_prd_info';
insert into silver.crm_prd_info (
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)
select 
prd_id,
replace(substring(prd_key from 1 for 5),'-','_') as cat_id,
substring(prd_key from 7 for length(prd_key)) prd_key,
prd_nm,
coalesce(prd_cost,0) as prd_cost,
case when trim(prd_line)='M' then 'Mountain'
		when trim(prd_line)='R' then 'Road'
		when trim(prd_line)='S' then 'Other Sales'
		when trim(prd_line)='T' then 'Touring'
		else 'n/a' end as prd_line ,
prd_start_dt,
lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as prd_end_dt
from bronze.crm_prd_info;

end_time :=clock_timestamp();
duration :=end_time-start_time;
raise notice 'crm_prd_info loaded in % seconds',extract(epoch from duration);


--silver.crm_sales_details
start_time :=clock_timestamp();
raise notice '>> Truncating table : silver.crm_sales_details ';
truncate table silver.crm_sales_details;
raise notice '## Inserting Data Into: silver.crm_sales_details';
insert into silver.crm_sales_details(
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt, 
sls_ship_dt, 
sls_due_dt, 
sls_sales, 
sls_quantity, 
sls_price
)
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt=0 or length(cast(sls_order_dt as varchar))!=8 then null
		else cast(cast(sls_order_dt as varchar) as date) end as sls_order_dt,
case when sls_ship_dt=0 or length(cast(sls_ship_dt as varchar))!=8 then null
		else cast(cast(sls_ship_dt as varchar) as date) end as sls_ship_dt,
case when sls_due_dt=0 or length(cast(sls_due_dt as varchar))!=8 then null
		else cast(cast(sls_due_dt as varchar) as date) end as sls_due_dt,
case when sls_sales <=0 or sls_sales is null or sls_sales != sls_price * sls_quantity then abs(sls_price) * abs(sls_quantity)
			else sls_sales end as sls_sales ,
sls_quantity,
case when sls_price <=0 or sls_price is null then sls_sales/sls_quantity 
		else sls_price end as sls_price
from bronze.crm_sales_details;

end_time :=clock_timestamp();
duration :=end_time-start_time;
raise notice 'crm_sales_details loaded in % seconds',extract(epoch from duration);


--ERP
raise notice '-------------------------------------';
	raise notice 'Loading ERP Tables';
	raise notice '-------------------------------------';

--silver.erp_cust_az12
start_time :=clock_timestamp();
raise notice '>> Truncating table : silver.erp_cust_az12 ';
truncate table silver.erp_cust_az12;
raise notice '## Inserting Data Into: silver.erp_cust_az12';
insert into silver.erp_cust_az12(cid,bdate,gen)
select 
case when cid like 'NAS%' then substring (cid from 4 for length(cid)) 
		else cid end as cid,
case when bdate>current_date then null
		else bdate end as bdate ,
case when gen=trim('M') or gen='Male' then 'Male'
		when gen=trim('F') or gen='Female' then 'Male'
		else 'n/a' end as gen
from bronze.erp_cust_az12;

end_time :=clock_timestamp();
duration :=end_time-start_time;
raise notice 'erp_cust_az12 loaded in % seconds',extract(epoch from duration);


--silver.erp_loc_a101
start_time :=clock_timestamp();
raise notice '>> Truncating table : silver.erp_loc_a101 ';
truncate table silver.erp_loc_a101;
raise notice '## Inserting Data Into: silver.erp_loc_a101';
insert into silver.erp_loc_a101(cid,cntry)
select 
replace(cid,'-','') as cid ,
case when trim(cntry)=trim('US') or trim(cntry)=trim('United States') or trim(cntry)=trim('United Kingdom') or trim(cntry)=trim('USA') then 'USA'
		when trim(cntry)=trim('Germany') or trim(cntry)=trim('DE') then 'Germany'
		when trim(cntry)=trim(' ') or cntry is null then 'n/a'
		else cntry end as cntry
from bronze.erp_loc_a101;

end_time :=clock_timestamp();
duration :=end_time-start_time;
raise notice 'erp_loc_a101 loaded in % seconds',extract(epoch from duration);


--silver.erp_px_cat_g1v2
start_time :=clock_timestamp();
raise notice '>> Truncating table : silver.erp_px_cat_g1v2 ';
truncate table silver.erp_px_cat_g1v2;
raise notice '## Inserting Data Into: silver.erp_px_cat_g1v2';
insert into silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
select 
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2;

end_time :=clock_timestamp();
duration :=end_time-start_time;
raise notice 'erp_px_cat_g1v2 loaded in % seconds',extract(epoch from duration);
----------------------------------------------------------------------
exception 
when others then 
raise notice 'Error : %',SQLERRM;
RAISE;
end;
$$ language plpgsql ;

select silver.load_silver()
