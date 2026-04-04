--=====================================================================
--=============================Gold Layer Views========================
--=====================================================================

--Customer View

CREATE VIEW  gold.dim_customers as
select 
ROW_NUMBER() OVER (order by cst_id)  customer_key,
cci.cst_id customer_id,
cci.cst_key customer_name,
cci.cst_firstname first_name,
cci.cst_lastname last_name,
cci.cst_marital_status,
CASE 
	WHEN cci.cst_gndr = 'n/a' THEN COALESCE(eca.GEN,'Other')
	ELSE cci.cst_gndr
	END as gender,
ela.CNTRY country,
eca.BDATE birth_date,
cci.cst_create_date creation_date
from 
silver.crm_cust_info cci 
left join silver.erp_cust_az12 eca
	on cci.cst_key = eca.CID
left join silver.erp_loc_a101 ela
	on cci.cst_key = ela.CID;


--Products View

create view gold.dim_products as
select 
ROW_NUMBER() OVER (order by pn.prd_id,pn.prd_start_dt) product_key,
pn.prd_id  product_id,
pn.prd_cat category_id,
pc.CAT category,
pc.SUBCAT sub_category,
pn.prd_key product_number,
pn.prd_nm product_name,
pc.MAINTENANCE maintenance,
pn.prd_cost cost,
pn.prd_line product_line,
pn.prd_start_dt product_start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc 
on pn.prd_cat = pc.ID
where pn.prd_end_dt is NULL;   --this will remove historical data of same product


--Sales View

create view gold.fact_sales as
select 
sd.sls_ord_num order_number, 
cu.customer_key,
pr.product_key,
sd.sls_order_dt order_date, 
sd.sls_ship_dt ship_date, 
sd.sls_due_dt due_date, 
sd.sls_sales,
sd.sls_quantity,
sd.sls_price
from 
silver.crm_sales_details sd
left join gold.dim_customers cu  
on sd.sls_cust_id = cu.customer_id
left join gold.dim_products pr 
on sd.sls_prd_key = pr.product_number;


--Foreign Key Integrity 

select * from gold.fact_sales a
left join gold.dim_customers b
on a.customer_key = b.customer_key
where b.customer_key is NULL; 

--No rows 


select * from gold.fact_sales a
left join gold.dim_products b
on a.product_key = b.product_key
where b.product_key is NULL; 

--No rows

