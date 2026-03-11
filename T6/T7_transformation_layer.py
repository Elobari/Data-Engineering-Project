import pandas as pd
from sqlalchemy import create_engine
import numpy as np

# --- 1. Updated Connection String ---
DB_USER = 'jonah'
DB_PASS = '7780'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'bike_dwh' 

conn_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(conn_str)

def run_transformations():
    print("--- Starting Transformation Layer ---")

    try:
        # LOAD DATA FROM INGESTION LAYER
        erp_customers = pd.read_sql("SELECT * FROM erp_customers", engine)
        erp_locations = pd.read_sql("SELECT * FROM erp_locations", engine)
        crm_customers = pd.read_sql("SELECT * FROM crm_customers", engine)
        crm_sales = pd.read_sql("SELECT * FROM crm_sales", engine)
        crm_products = pd.read_sql("SELECT * FROM crm_products", engine)
        
        print(f" Data Loaded: {len(crm_sales)} sales records and {len(crm_customers)} customers.")

        # 2. DATA CLEANSING: Handling Missing Values
        # Count nulls before cleaning
        null_marital = crm_customers['cst_marital_status'].isnull().sum()
        null_prod_end = crm_products['prd_end_dt'].isnull().sum()
        
        crm_customers['cst_marital_status'] = crm_customers['cst_marital_status'].fillna('Unknown')
        crm_products['prd_end_dt'] = crm_products['prd_end_dt'].fillna('2099-12-31')

        print(f"Cleaned {null_marital} missing marital status values.")
        print(f"Cleaned {null_prod_end} missing product end dates.")
        
        # 3. STANDARDIZATION: Aligning ERP and CRM Gender
        # Count how many short codes (M/F) exist before replacing
        m_f_count = erp_customers['GEN'].isin(['M', 'F']).sum()
        erp_customers['GEN'] = erp_customers['GEN'].replace({'M': 'Male', 'F': 'Female'})
        print(f"Standardized {m_f_count} gender codes (M/F -> Male/Female).")

        # --- FIX: DATA TYPE CONVERSION ---
        # Convert all ID columns to strings to ensure they match for the merge
        crm_customers['cst_id'] = crm_customers['cst_id'].astype(str)
        erp_customers['CID'] = erp_customers['CID'].astype(str)
        erp_locations['CID'] = erp_locations['CID'].astype(str)

        # --- FIX: DATA TYPE CONVERSION ---
        crm_customers['cst_id'] = crm_customers['cst_id'].astype(str)
        erp_customers['CID'] = erp_customers['CID'].astype(str)
        erp_locations['CID'] = erp_locations['CID'].astype(str)
        
        # 4. CONSOLIDATION: Integrating ERP and CRM Data
        dim_customers = pd.merge(
            crm_customers, 
            erp_customers[['CID', 'BDATE', 'GEN']], 
            left_on='cst_id', 
            right_on='CID', 
            how='left'
        )

        # Adding Location data from ERP
        dim_customers = pd.merge(
            dim_customers, 
            erp_locations[['CID', 'CNTRY']], 
            on='CID', 
            how='left'
        )

        # Check merge success
        unmatched_geo = dim_customers['CNTRY'].isnull().sum()
        print(f"Merged ERP and CRM data. {unmatched_geo} customers had no matching ERP record.")

        # 5. DERIVED ANALYTICS: Age & Fulfillment
        dim_customers['BDATE'] = pd.to_datetime(dim_customers['BDATE'])
        dim_customers['age'] = 2026 - dim_customers['BDATE'].dt.year

        crm_sales['sls_order_dt'] = pd.to_datetime(crm_sales['sls_order_dt'])
        crm_sales['sls_ship_dt'] = pd.to_datetime(crm_sales['sls_ship_dt'])
        crm_sales['fulfillment_days'] = (crm_sales['sls_ship_dt'] - crm_sales['sls_order_dt']).dt.days

        avg_fulfillment = round(crm_sales['fulfillment_days'].mean(), 2)
        print(f"Calculated Age and Fulfillment. Avg fulfillment time: {avg_fulfillment} days.")
        
        # 6. LOAD TO ANALYTICAL LAYER
        dim_customers.to_sql('dim_customers', engine, if_exists='replace', index=False)
        crm_sales.to_sql('fact_sales', engine, if_exists='replace', index=False)
        crm_products.to_sql('dim_products', engine, if_exists='replace', index=False)

        print(" Transformation Complete: 'dim_customers', 'fact_sales', and 'dim_products' are ready.")

    except Exception as e:
        print(f" Transformation Failed: {e}")

if __name__ == "__main__":
    run_transformations()