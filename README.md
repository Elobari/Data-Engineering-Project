Starting ingestion process...

Loading: datasets/source_erp/CUST_AZ12.csv
Rows found: 18484
Inserted into table: erp_customers

Loading: datasets/source_erp/LOC_A101.csv
Rows found: 18484
Inserted into table: erp_locations

Loading: datasets/source_erp/PX_CAT_G1V2.csv
Rows found: 37
Inserted into table: erp_product_categories

Loading: datasets/source_crm/cust_info.csv
Rows found: 18493
Inserted into table: crm_customers

Loading: datasets/source_crm/prd_info.csv
Rows found: 397
Inserted into table: crm_products

Loading: datasets/source_crm/sales_details.csv
Rows found: 60398
Inserted into table: crm_sales

Ingestion completed!

----

--- Validating ERP Customers ---
Rows: 18484
Columns: 3

Missing Values:
CID         0
BDATE       0
GEN      1472
dtype: int64

Duplicate Rows: 0

--- Validating ERP Locations ---
Rows: 18484
Columns: 2

Missing Values:
CID        0
CNTRY    332
dtype: int64

Duplicate Rows: 0

--- Validating ERP Product Categories ---
Rows: 37
Columns: 4

Missing Values:
ID             0
CAT            0
SUBCAT         0
MAINTENANCE    0
dtype: int64

Duplicate Rows: 0

--- Validating CRM Customers ---
Rows: 18493
Columns: 7

Missing Values:
cst_id                   3
cst_key                  0
cst_firstname            7
cst_lastname             6
cst_marital_status       6
cst_gndr              4577
cst_create_date          3
dtype: int64

Duplicate Rows: 0

--- Validating CRM Products ---
Rows: 397
Columns: 7

Missing Values:
prd_id            0
prd_key           0
prd_nm            0
prd_cost          2
prd_line         17
prd_start_dt      0
prd_end_dt      197
dtype: int64

Duplicate Rows: 0

--- Validating CRM Sales ---
Rows: 60398
Columns: 9

Missing Values:
sls_ord_num     0
sls_prd_key     0
sls_cust_id     0
sls_order_dt    0
sls_ship_dt     0
sls_due_dt      0
sls_sales       8
sls_quantity    0
sls_price       7
dtype: int64

Duplicate Rows: 0
