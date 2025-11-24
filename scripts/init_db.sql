/*
==========================================================================
Create Database and Schemas
==========================================================================
Script Purpose:
	This script creates a new database named "datawarehouse" after checking if it already exists. If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.

WARNING:
	Running this script will drop the entire 'datawarehouse' database if it exists. All data in the database will be permanently deleted. Proceed with caution and ensure you have proper backups before running this script.
*/


\connect postgres;

DROP DATABASE IF EXISTS datawarehouse;

CREATE DATABASE datawarehouse;

\connect datawarehouse;

CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;