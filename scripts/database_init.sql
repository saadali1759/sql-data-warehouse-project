
/*
    This Script creates the 'datawarehouse' database and its Schemas:
    - Drops it if it already exists (force disconnects users)
    - Creates a fresh database
    - Adds bronze, silver, and gold schemas
*/


USE master;
GO

-- Check if database exists and drop it
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'datawarehouse')
BEGIN
    ALTER DATABASE datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE datawarehouse;
END
GO

-- Create database
CREATE DATABASE datawarehouse;
GO

-- Use the new database
USE datawarehouse;
GO

-- Create schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
