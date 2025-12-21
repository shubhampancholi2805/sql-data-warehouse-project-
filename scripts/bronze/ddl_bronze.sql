/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/
drop table if exists bronze.crm_cust_info ;
create table bronze.crm_cust_info (
cst_id	int ,
cst_key	varchar(15),
cst_firstname	varchar(15),
cst_lastname	varchar(20),
cst_marital_status	varchar(15),
cst_gndr	varchar(15),
cst_create_date	date 
);

drop table if exists bronze.crm_prd_info;
create table bronze.crm_prd_info (
prd_id	int ,
prd_key	varchar(20),
prd_nm	varchar(40),
prd_cost	int ,
prd_line	varchar(5),
prd_start_dt	date ,
prd_end_dt	date 
);

drop table if exists bronze.crm_sales_details;
create table bronze.crm_sales_details(
sls_ord_num	varchar(10) ,
sls_prd_key	varchar(10) ,
sls_cust_id	int ,
sls_order_dt	int, 
sls_ship_dt	int, 
sls_due_dt	int, 
sls_sales	int, 
sls_quantity	int, 
sls_price	int
);

drop table if exists bronze.erp_CUST_AZ12;
create table bronze.erp_CUST_AZ12 (
CID	varchar(15),
BDATE	date,
GEN	varchar(10)
);

drop table if exists bronze.erp_LOC_A101;
create table bronze.erp_LOC_A101(
CID	varchar(15),
CNTRY	varchar(15)
);

drop table if exists bronze.erp_PX_CAT_G1V2;
create table bronze.erp_PX_CAT_G1V2 (
ID	varchar(5),
CAT	varchar(15),
SUBCAT	varchar(20),
MAINTENANCE	varchar(5)
);
