/*
================== SCRIPT PURPOSE ===================
This script will create the database 'DataWarehouse' after checking if it already exists as well as schemas : bronze , silver , gold*/
use master;
go



--Checking the existence of the database , drop and recreate
if exists (select 1 from sys.databases where name='DataWarehouse')
Begin 
	alter database DataWarehouse set single_user  with rollback immediate ;
	drop database DataWarehouse;
end;
go


--Creating the DataWarehouse database 
create database DataWarehouse;

use DataWarehouse;
go

--Creating the schemas for medallion architecture
create schema bronze;
go
create schema silver;
go
create schema gold;

