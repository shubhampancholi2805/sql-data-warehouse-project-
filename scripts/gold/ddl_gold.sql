/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/


--gold.dim_customers
create or replace view gold.dim_customers as 
select 
row_number() over(order by ci.cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
case when ci.cst_gndr!='n/a' then ci.cst_gndr
		else coalesce(ca.gen,'n/a')
		end as gender,
ca.bdate as birth_date,
ci.cst_marital_status as marital_status,
cl.cntry as country ,
ci.cst_create_date as create_date 
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ca.cid=ci.cst_key
left join silver.erp_loc_a101 cl
on ci.cst_key=cl.cid;

select * from gold.dim_customers;


--gold.dim_products
create or replace view gold.dim_products as 
select 
row_number() over(order by pi.prd_start_dt,pi.prd_id) as product_key,
pi.prd_id as product_id,
pi.prd_key as product_number,
pi.prd_nm as product_name,
pi.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance,
pi.prd_cost as cost,
pi.prd_line as product_line,
pi.prd_start_dt as start_date
from silver.crm_prd_info pi
left join silver.erp_px_cat_g1v2 pc on pc.id=pi.cat_id
where prd_end_dt is null  --filter out historical data .

select * from gold.dim_products;


--gold.fact_sales
create or replace view gold.fact_sales as 
select 
sl.sls_ord_num as order_number,
gp.product_key ,
gc.customer_key,
sl.sls_order_dt as order_date, 
sl.sls_ship_dt as shipping_date, 
sl.sls_due_dt as due_date , 
sl.sls_sales as sales_amount, 
sl.sls_quantity as quantity, 
sl.sls_price as price
from silver.crm_sales_details sl
left join gold.dim_products gp on gp.product_number=sl.sls_prd_key
left join gold.dim_customers gc on gc.customer_id=sl.sls_cust_id

select * from gold.fact_sales;

