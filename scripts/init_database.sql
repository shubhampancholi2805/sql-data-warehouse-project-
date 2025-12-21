/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.
*/
--creating database 'DataWarehouse'
create database DataWarehouse
--creating schemas 'bronze', 'silver', and 'gold'.
create schema bronze ;
create schema silver ;
create schema gold ;
