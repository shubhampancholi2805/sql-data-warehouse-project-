/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

drop table if exists silver.crm_cust_info ;
create table silver.crm_cust_info (
cst_id	int ,
cst_key	varchar(15),
cst_firstname	varchar(15),
cst_lastname	varchar(20),
cst_marital_status	varchar(15),
cst_gndr	varchar(15),
cst_create_date	date ,
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

drop table if exists silver.crm_prd_info;
create table silver.crm_prd_info (
prd_id	int ,
cat_id varchar(20),
prd_key	varchar(20),
prd_nm	varchar(40),
prd_cost	int ,
prd_line	varchar(20),
prd_start_dt	date ,
prd_end_dt	date ,
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

drop table if exists silver.crm_sales_details;
create table silver.crm_sales_details(
sls_ord_num	varchar(10) ,
sls_prd_key	varchar(10) ,
sls_cust_id	int ,
sls_order_dt date, 
sls_ship_dt	date, 
sls_due_dt	date, 
sls_sales	int, 
sls_quantity int, 
sls_price	int,
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

drop table if exists silver.erp_CUST_AZ12;
create table silver.erp_CUST_AZ12 (
CID	varchar(15),
BDATE	date,
GEN	varchar(10),
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

drop table if exists silver.erp_LOC_A101;
create table silver.erp_LOC_A101(
CID	varchar(15),
CNTRY	varchar(15),
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

drop table if exists silver.erp_PX_CAT_G1V2;
create table silver.erp_PX_CAT_G1V2 (
ID	varchar(5),
CAT	varchar(15),
SUBCAT	varchar(20),
MAINTENANCE	varchar(5),
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
