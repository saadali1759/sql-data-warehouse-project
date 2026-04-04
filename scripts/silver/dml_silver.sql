use datawarehouse;

--=============================================================================================
-- Drop table if it already exists

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

-- Create table
CREATE TABLE silver.crm_cust_info 
(
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(10),
    cst_gndr NVARCHAR(10),
    cst_create_date DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

--=============================================================================================
-- Drop table if it already exists

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

-- Create table
CREATE TABLE silver.crm_prd_info 
(
prd_id INT,
prd_cat NVARCHAR(10),
prd_key NVARCHAR(20),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

--=============================================================================================
-- Drop table if it already exists

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

-- Create table
CREATE TABLE silver.crm_sales_details 
(
sls_ord_num NVARCHAR(20),
sls_prd_key NVARCHAR(20),
sls_cust_id INT,
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE, 
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO



--=============================================================================================
-- Drop table if it already exists

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

-- Create table
CREATE TABLE silver.erp_cust_az12
(
CID NVARCHAR(50),
BDATE DATE,
GEN NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

--=============================================================================================
-- Drop table if it already exists

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

-- Create table
CREATE TABLE silver.erp_loc_a101
(
CID NVARCHAR(50),
CNTRY NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO


--=============================================================================================
-- Drop table if it already exists

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

-- Create table
CREATE TABLE silver.erp_px_cat_g1v2
(
ID NVARCHAR(50),
CAT NVARCHAR(50),
SUBCAT NVARCHAR(50),
MAINTENANCE NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

--======================================================================================================
--====================================== Data Cleaning =================================================
--======================================================================================================


--******************* Data Quality Checks for crm_cust_info *******************

-- 1) Duplicate/Missing Primary Key Vales

select 
    count(*),
    cst_id 
from 
    bronze.crm_cust_info
    group by cst_id 
    having(count(*))>1 or cst_id is NULL;

-- 2) Unwanted Spaces / Easy Readability

select 
    cst_firstname 
from 
    bronze.crm_cust_info
where cst_firstname <> TRIM (cst_firstname);

select 
    cst_lastname
from 
    bronze.crm_cust_info
where cst_lastname <> TRIM (cst_lastname);


insert into silver.crm_cust_info
(cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
select 
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE UPPER(TRIM(cst_marital_status))
    WHEN 'M' THEN 'Married'
    WHEN 'S' THEN 'Single'
    ELSE 'n/a'
    END,
    CASE UPPER(TRIM(cst_gndr))
    WHEN 'M' THEN 'Male'
    WHEN 'F' THEN 'Female'
    ELSE 'n/a'
    END,
    cst_create_date
from 
(
    select *,
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER by cst_create_date desc) as flag 
    from bronze.crm_cust_info
    where cst_id IS NOT NULL
) t 
where flag = 1;

--******************* Data Quality Checks for crm_prd_info *******************

-- 1) 

select 
    prd_id,
    count(*)
from bronze.crm_prd_info 
group by prd_id
having count(*)>1;



insert into silver.crm_prd_info
(prd_id,prd_cat,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt) 
select 
    prd_id,
    replace(substring(prd_key,1,5),'-','_') prd_cat,
    substring(prd_key,7,len(prd_key)) prd_key,
    prd_nm,
    ISNULL(prd_cost,0) prd_cost,
    CASE UPPER(TRIM(prd_line))
    WHEN 'M' THEN 'Mountain'
    WHEN 'R' THEN 'Road'
    WHEN 'T' THEN 'Touring'
    WHEN 'S' THEN 'Other Sales'
    ELSE 'n/a'
    END as prd_line,
    CAST(prd_start_dt as DATE) prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER (Partition by prd_key order by prd_start_dt)-1 AS DATE) as prd_end_dt 
from bronze.crm_prd_info;


--******************* Data Quality Checks for crm_sales_details *******************

-- 1) Duplicate/Missing Primary Key Vales

select count(*) from bronze.crm_sales_details
where sls_ord_num is NULL OR
sls_ord_num <> TRIM (sls_ord_num);

select count(*) from bronze.crm_sales_details
where sls_prd_key is NULL OR
sls_prd_key <> TRIM (sls_prd_key);

select count(*) from bronze.crm_sales_details
where sls_cust_id is NULL OR
sls_cust_id = 0;

-- 2) Checking If Dates have correct length or not.

select * from bronze.crm_sales_details
where sls_order_dt = 0 or len(sls_order_dt)<>8;


-- 3) Checking sales,price and quantity fields.

select * from bronze.crm_sales_details
where sls_quantity <=0 or sls_quantity is NULL;

select sls_sales,sls_quantity,sls_price from bronze.crm_sales_details
where sls_sales <=0 or sls_sales is NULL;

insert into silver.crm_sales_details
(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)

select 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE
    WHEN sls_order_dt = 0 or len(sls_order_dt)<>8 THEN NULL
    ELSE TRY_CAST(CAST(sls_order_dt as VARCHAR) as DATE)
    END sls_ord_date,
    CASE
    WHEN sls_ship_dt = 0 or len(sls_ship_dt)<>8 THEN NULL
    ELSE TRY_CAST(CAST(sls_ship_dt as VARCHAR) as DATE)
    END sls_ship_date,
    CASE
    WHEN sls_due_dt = 0 or len(sls_due_dt)<>8 THEN NULL
    ELSE TRY_CAST(CAST(sls_due_dt as VARCHAR) as DATE)
    END sls_due_date,
    CASE 
    WHEN sls_sales <=0 or sls_sales is NULL or sls_sales <> ABS(sls_quantity)*ABS(sls_price)
    THEN ABS(sls_quantity)*ABS(sls_price)
    ELSE sls_sales
    END sls_sales_new,
    sls_quantity,
    CASE 
    WHEN sls_price<=0 or sls_price is NULL 
    THEN sls_sales/NULLIF(sls_quantity,0)
    ELSE sls_price
    END sls_price_new
from bronze.crm_sales_details;


select * from silver.crm_sales_details;



--******************* Data Quality Checks for erp_cust_az12 *******************

select * from  bronze.erp_cust_az12
where TRIM(CID) <> CID;

--No rows

select BDATE from bronze.erp_cust_az12
where BDATE < '1920-01-01';

--Data Exists. 


select BDATE from bronze.erp_cust_az12
where BDATE > GETDATE();

--Data Exists

select DISTINCT GEN from bronze.erp_cust_az12;

-- Duplicate Formats


insert into silver.erp_cust_az12(CID,BDATE,GEN)
select 
    CASE 
        WHEN CID like 'NAS%' THEN SUBSTRING(CID,4,len(CID))
        ELSE CID 
    END as CID,
    CASE
        WHEN BDATE > GETDATE() THEN NULL 
        ELSE BDATE 
    END as BDATE,
    CASE 
        WHEN TRIM(GEN) IN ('M','Male') THEN 'Male'
        WHEN TRIM(GEN) IN ('F','Female') THEN 'Female'
        ELSE 'Other'
    END as GEN
from bronze.erp_cust_az12;


--******************* Data Quality Checks for erp_loc_a101 *******************

insert into silver.erp_loc_a101(CID,CNTRY)
select 
    REPLACE(CID,'-','') CID ,
    CASE 
        WHEN TRIM(CNTRY) IN ('DE') THEN 'Germany'
        WHEN TRIM(CNTRY) IN ('United States','US','USA') THEN 'United States'
        WHEN (TRIM(CNTRY) = '' or TRIM(CNTRY) IS NULL) THEN 'n/a'
        ELSE TRIM(CNTRY)
    END as CNTRY
from bronze.erp_loc_a101;



--******************* Data Quality Checks for erp_px_cat_g1v2 *******************

insert into silver.erp_px_cat_g1v2(ID,CAT,SUBCAT,MAINTENANCE)
select ID,CAT,SUBCAT,MAINTENANCE
from bronze.erp_px_cat_g1v2;



--******************* Data Transformation Done *******************
