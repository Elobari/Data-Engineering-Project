import pandas as pd

files = {
    "ERP Customers": "datasets/source_erp/CUST_AZ12.csv",
    "ERP Locations": "datasets/source_erp/LOC_A101.csv",
    "ERP Product Categories": "datasets/source_erp/PX_CAT_G1V2.csv",
    "CRM Customers": "datasets/source_crm/cust_info.csv",
    "CRM Products": "datasets/source_crm/prd_info.csv",
    "CRM Sales": "datasets/source_crm/sales_details.csv"
}


def validate_file(name, path):

    print(f"\n--- Validating {name} ---")

    df = pd.read_csv(path)

    print("Rows:", len(df))
    print("Columns:", len(df.columns))

    print("\nMissing Values:")
    print(df.isnull().sum())

    print("\nDuplicate Rows:", df.duplicated().sum())


def run_validation():

    for name, path in files.items():
        validate_file(name, path)


if __name__ == "__main__":
    run_validation()