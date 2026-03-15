### JOnah Knief - i6263747

import pandas as pd
from sqlalchemy import create_engine
import numpy as np

# ---------------------------
# DATABASE CONNECTION
# (Update with your credentials)
# ---------------------------
DB_USER = "postgres"
DB_PASSWORD = "password"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "bike_dwh"
 c
engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

def run_product_transformations():
    print("--- Starting Product Transformation Layer ---")

    try:
        # 1. LOAD DATA FROM INGESTION LAYER
        # We start with the original ingested crm_products table
        crm_products = pd.read_sql("SELECT * FROM crm_products", engine)
        print(f"Data Loaded: {len(crm_products)} product records.")

        # Ensure date columns are datetime objects before proceeding
        crm_products['prd_start_dt'] = pd.to_datetime(crm_products['prd_start_dt'])
        crm_products['prd_end_dt'] = pd.to_datetime(crm_products['prd_end_dt'])

        # Create a working copy
        dim_products = crm_products.copy()

        # ---------------------------------------------------------
        # TRANSFORMATION 1: Create 'subcategory' column
        # Extract first 5 characters from 'prd_key'
        # Use .str[:5] to capture these, as requested.
        # Perform the slice from the prd_key as requested:
        dim_products['prd_subcategory'] = dim_products['prd_key'].str[:5]
        
        cols = ['prd_id', 'prd_key', 'prd_subcategory', 'prd_nm', 'prd_cost', 'prd_line', 'prd_start_dt', 'prd_end_dt']
        dim_products = dim_products[cols]
        print("Created subcategory column.")


        # ---------------------------------------------------------
        # TRANSFORMATION 2: Handle NULL values in cost column
        # Replace NaN with 0 integer
        null_cost_count = dim_products['prd_cost'].isnull().sum()
        dim_products['prd_cost'] = dim_products['prd_cost'].fillna(0).astype(int)
        print(f"Replaced {null_cost_count} NULL costs with 0.")


        # ---------------------------------------------------------
        # TRANSFORMATION 3: Transform 'line' to full words
        # Based on product names, T = Touring
        line_mapping = {
            'R': 'Road',
            'S': 'Sport',
            'M': 'Mountain',
            'T': 'Touring'
        }
        
        # Ensure 'prd_line' only contains the first letter/code before mapping
        # Some source data might have full words or short codes. We slice just in case.
        short_lines = dim_products['prd_line'].str[:1]
        dim_products['prd_line'] = short_lines.map(line_mapping).fillna(dim_products['prd_line'])
        print("Standardized Product Line codes to full words.")


        # ---------------------------------------------------------
        # TRANSFORMATION 4: Keep [null] and set date where applicable
        # The complex logic: The current row's end date (if not NULL) should be replaced with:
        # the start date of the NEXT row minus 1 day.
        
        # 1. First, we identify rows where the end date is NOT currently NULL.
        # Those are the historical versions we need to update.
        # We can detect this with notnull().

        # We need to compute the 'next start date - 1 day' globally
        # Shift() brings the 'prd_start_dt' from the row below (i+1) into the current row.
        # Timedelta(days=1) subtracts 1 day from that value.
        next_start_dates_minus_one = dim_products['prd_start_dt'].shift(-1) - pd.Timedelta(days=1)

        # 2. Apply this computed date *only* where an end date *already* existed.
        # We use a lambda function to make this clear. If the original end date wasn't null,
        # replace it. Otherwise, keep it as NaT (null date).
        
        updated_end_dates = dim_products.apply(
            lambda row: next_start_dates_minus_one.loc[row.name] if pd.notnull(row['prd_end_dt']) else pd.NaT,
            axis=1
        )

        dim_products['prd_end_dt'] = updated_end_dates
        
        print("Applied complex historical product end-date logic.")

        # ---------------------------------------------------------
        # NEW: SHOW RESULTS IN TERMINAL
        # ---------------------------------------------------------
        print("\n" + "="*50)
        print("PREVIEW OF TRANSFORMED DATA (First 10 rows):")
        print("="*50)
        # Using to_string() ensures columns don't get cut off in the terminal
        print(dim_products.head(10).to_string()) 
        print("="*50 + "\n")


        # ---------------------------------------------------------
        # 6. LOAD TO ANALYTICAL LAYER
        # Use 'replace' to overwrite any existing dimension table
        dim_products.to_sql('dim_products', engine, if_exists='replace', index=False)

        print(f"Transformation Complete: 'dim_products' is ready in the {DB_NAME} database.")

    except Exception as e:
        print(f"Transformation Failed: {e}")

if __name__ == "__main__":
    run_product_transformations()