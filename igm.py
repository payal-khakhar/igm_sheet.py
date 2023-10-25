import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery
import pandas as pd
import pygsheets
PROJECT_ID = "dalfilo-main"
DATASET_ID = "seeds"

SCHEMA = [
    bigquery.SchemaField("talent", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("type", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("product", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("content_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("fee", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("order_nr", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("received", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("brief", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("discount_code", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("link", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("campaign", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("term", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("content", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("utm", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("string_concats", "STRING", mode="NULLABLE"),
]
schema_fields = [field.name for field in SCHEMA]

def load_data_from_gsheets(worksheet_name):
    # Authenticate with Google Sheets
    gc = pygsheets.authorize(service_file="hex_admin.json")
    sh = gc.open("Influencers")  # Fixed sheet_name
    wks = sh.worksheet("title", worksheet_name)
     # Fetch data from the worksheet
    data = wks.get_all_values()
    headers = data[0]
    df = pd.DataFrame(data[1:], columns=headers)
    df = df[schema_fields]
    return df
def check_uniqueness_and_transform_schema(df):
    # Identify unique rows based on specific columns
    unique_combination_columns = ["talent", "date", "campaign", "product"]
    is_duplicate = df.duplicated(subset=unique_combination_columns, keep=False)

    # Filter based on the boolean mask
    df_unique = df[~is_duplicate]
    df_unique = df_unique[schema_fields]

    # Convert specific columns to the desired data types

    def custom_numeric_conversion(value):
        try:
            return pd.to_numeric(value)
        except (ValueError, TypeError):
            return value  # Return the original value if conversion fails

    # Define a custom conversion function for boolean columns
    def custom_bool_conversion(value):
        if value.lower() in [
            "true",
            "1",
            "yes",
        ]:  # You can add more conditions if needed
            return True
        elif value.lower() in [
            "false",
            "0",
            "no",
            "",
        ]:  # You can add more conditions if needed
            return False
        else:
            return value  # Return the original value if not a recognized boolean value

    # Define a custom conversion function for the 'date' column
    def custom_date_conversion(value):
        try:
            return pd.to_datetime(value, format="%Y-%m-%d")
        except (ValueError, TypeError):
            return value  # Return the original value if conversion fails

    # Apply the custom conversion functions to specific columns
    df_unique["fee"] = df_unique["fee"].apply(custom_numeric_conversion)
    df_unique["order_nr"] = df_unique["order_nr"].apply(custom_numeric_conversion)
    df_unique["type"] = df_unique["type"].apply(custom_numeric_conversion)
    df_unique["received"] = df_unique["received"].apply(custom_bool_conversion)
    df_unique["brief"] = df_unique["brief"].apply(custom_bool_conversion)
    df_unique["date"] = df_unique["date"].apply(custom_date_conversion)
    data_types = {
        "talent": str,  # STRING
        "type": int,  # INTEGER
        "date": pd.to_datetime,  # DATE
        "product": str,  # STRING
        "content_type": str,  # STRING
        "fee": float,  # FLOAT
        "order_nr": int,  # INTEGER
        "received": bool,  # BOOLEAN
        "brief": bool,  # BOOLEAN
        "discount_code": str,  # STRING
        "link": str,  # STRING
        "campaign": str,  # STRING
        "term": str,  # STRING
        "content": str,  # STRING
        "utm": str,  # STRING
        "string_concats": str,  # STRING
    }
    class ArrowInvalid(Exception):
     pass
    # Create a function to validate a row against data types
    def validate_row(row):
      for col, validate_func in data_types.items():
         cell_value = row[col]
         if cell_value and not pd.isna(cell_value) and cell_value!='':
            try:
                # Attempt to validate the value
                if not validate_func(cell_value):
                    return False
            except (ValueError, TypeError, ArrowInvalid):
                print(cell_value)
                return False
      return True


    # Filter rows where all cells match the expected data types
    df_unique = df_unique[df_unique.apply(validate_row, axis=1)]
    return df_unique
def create_load_table_into_bigquery(table_id, df):
    # Authenticate with BigQuery
    client = bigquery.Client.from_service_account_json("hex_admin.json", project=PROJECT_ID)
    table_ref = client.dataset(DATASET_ID).table(table_id)
    # Load data into the existing BigQuery table
    job_config = bigquery.LoadJobConfig(schema=SCHEMA,write_disposition='WRITE_TRUNCATE')
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Data loaded into the table {table_id} successfully.")
table_ids = ["igm_italy", "igm_germany", "igm_spain"]
worksheet_names = [
    "it_igm_lightdash_data",
    "de_igm_lightdash_data",
    "es_igm_lightdash_data"
]
for table_id, worksheet_name in zip(table_ids, worksheet_names):
    # Step 1: Load data from Google Sheets
    data = load_data_from_gsheets(worksheet_name)

    # Step 2: Check for uniqueness and schema transformation
    transformed_data = check_uniqueness_and_transform_schema(data)

    # Step 3: Load the transformed data into BigQuery
    create_load_table_into_bigquery(table_id, transformed_data)
