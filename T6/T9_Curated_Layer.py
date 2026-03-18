# JOnah Knief - i6263747

import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from datetime import datetime

# --- Database Connection ---
DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME = 'jonah', '7780', 'localhost', '5432', 'bike_dwh'
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def run_t9_curated_layer():
    print(f"\n{'='*60}\n STARTING CURATED LAYER (T9)\n{'='*60}")

    try:
        # 1. LOAD DATA
        crm_sales = pd.read_sql("SELECT * FROM crm_sales", engine)
        erp_customers = pd.read_sql("SELECT * FROM erp_customers", engine)
        erp_locations = pd.read_sql("SELECT * FROM erp_locations", engine)
        
        print(f"Data Loaded: {len(crm_sales)} sales records.")
        print(f"Data Loaded: {len(erp_customers)} customer records.")
        print(f"Data Loaded: {len(erp_locations)} location records.\n")

        
        # 2. TRANSFORM SALES
        print(" TRANSFORMING SALES DATA...")
        null_prices = crm_sales['sls_price'].isnull().sum()
        null_dates = crm_sales['sls_order_dt'].isnull().sum()

        crm_sales['sls_order_dt'] = pd.to_datetime(crm_sales['sls_order_dt'], format='%Y%m%d', errors='coerce')
        crm_sales['sls_ship_dt'] = pd.to_datetime(crm_sales['sls_ship_dt'], format='%Y%m%d', errors='coerce')

        # --- FIX: Specific Date Formatting ---
        # The integers (20101229) must be read as YearMonthDay
        #crm_sales['sls_order_dt'] = pd.to_datetime(crm_sales['sls_order_dt'], format='%Y%m%d', errors='coerce')
        #crm_sales['sls_ship_dt'] = pd.to_datetime(crm_sales['sls_ship_dt'], format='%Y%m%d', errors='coerce')
        crm_sales['sls_due_dt'] = pd.to_datetime(crm_sales['sls_due_dt'], format='%Y%m%d', errors='coerce')


        # Order Date Consistency logic
        crm_sales['sls_order_dt'] = crm_sales['sls_order_dt'].fillna(crm_sales['sls_ship_dt'])
        crm_sales['sls_order_dt'] = crm_sales.groupby('sls_ord_num')['sls_order_dt'].transform('first')

        # Price/Sales Calculations
        crm_sales['sls_price'] = crm_sales['sls_price'].fillna(crm_sales['sls_sales'] / crm_sales['sls_quantity'])
        crm_sales['sls_sales'] = crm_sales['sls_sales'].fillna(crm_sales['sls_price'] * crm_sales['sls_quantity'])

        print(f"   Fixed {null_dates} missing order dates.")
        print(f"   Derived {null_prices} missing prices.\n")

        # 3. TRANSFORM ERP CUSTOMERS
        print(" CURATING ERP CUSTOMERS...")
        today = datetime.now()
        future_bday_count = (pd.to_datetime(erp_customers['BDATE'], errors='coerce') > today).sum()
        erp_customers['BDATE'] = pd.to_datetime(erp_customers['BDATE'], errors='coerce')
        erp_customers.loc[erp_customers['BDATE'] > today, 'BDATE'] = pd.NaT
        
        # ID Alignment (Last 5 digits)
        erp_customers['CID'] = erp_customers['CID'].astype(str).str[-5:]
        erp_customers['GEN'] = erp_customers['GEN'].replace(['', None], np.nan).fillna('n/a')

        print(f"   Nullified {future_bday_count} future birthdates.")
        print(f"   Standardized Customer IDs (Example: '{erp_customers['CID'].iloc[0]}').\n")

        # 4. TRANSFORM ERP LOCATIONS
        print(" CLEANING GEOGRAPHIC DATA...")
        country_map = {'DE': 'Germany', 'US': 'United States', 'USA': 'United States'}
        erp_locations['CNTRY'] = erp_locations['CNTRY'].replace(country_map)
        erp_locations['CID'] = erp_locations['CID'].astype(str).str[-5:]

        print(f"   Standardized Country names and Location IDs.\n")

        # 5. VERIFICATION PREVIEW
        print(f"{'-'*25} DATA PREVIEW {'-'*25}")
        
        print("\n--- Fact Sales (Check calculated prices and order dates) ---")
        print(crm_sales[['sls_ord_num', 'sls_order_dt', 'sls_quantity', 'sls_price', 'sls_sales']].head(5).to_string())

        print("\n--- ERP Customers (Check sliced IDs and Gender) ---")
        print(erp_customers[['CID', 'BDATE', 'GEN']].head(5).to_string())

        print("\n--- ERP Locations (Check standardized Countries) ---")
        print(erp_locations[['CID', 'CNTRY']].head(5).to_string())
        #print(f"{'-'*64}")

        # 6. LOAD TO DATABASE
        crm_sales.to_sql('fact_sales', engine, if_exists='replace', index=False)
        erp_customers.to_sql('dim_erp_customers', engine, if_exists='replace', index=False)
        erp_locations.to_sql('dim_erp_locations', engine, if_exists='replace', index=False)

        print(f"{'='*60}\n T9 SUCCESS: Curated tables pushed to 'bike_dwh' database.\n{'='*60}\n")

    except Exception as e:
        print(f" ERROR DURING CURATION: {e}")

if __name__ == "__main__":
    run_t9_curated_layer()