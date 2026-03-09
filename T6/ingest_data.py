import pandas as pd
from sqlalchemy import create_engine

# ---------------------------
# DATABASE CONNECTION
# ---------------------------

DB_USER = "postgres"
DB_PASSWORD = "password"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "bike_dwh"

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ---------------------------
# FILE PATHS (MATCH YOUR STRUCTURE)
# ---------------------------

files = {
    "erp_customers": "datasets/source_erp/CUST_AZ12.csv",
    "erp_locations": "datasets/source_erp/LOC_A101.csv",
    "erp_product_categories": "datasets/source_erp/PX_CAT_G1V2.csv",
    "crm_customers": "datasets/source_crm/cust_info.csv",
    "crm_products": "datasets/source_crm/prd_info.csv",
    "crm_sales": "datasets/source_crm/sales_details.csv"
}

# ---------------------------
# LOAD CSV TO POSTGRES
# ---------------------------

def load_csv_to_postgres(table_name, file_path):

    try:
        df = pd.read_csv(file_path)

        print(f"\nLoading: {file_path}")
        print(f"Rows found: {len(df)}")

        df.to_sql(
            table_name,
            engine,
            if_exists="replace",
            index=False
        )

        print(f"Inserted into table: {table_name}")

    except Exception as e:
        print(f"Error processing {file_path}")
        print(e)

# ---------------------------
# RUN INGESTION
# ---------------------------

def run_ingestion():

    print("Starting ingestion process...")

    for table, path in files.items():
        load_csv_to_postgres(table, path)

    print("\nIngestion completed!")

if __name__ == "__main__":
    run_ingestion()