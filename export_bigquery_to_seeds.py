import os
from google.cloud import bigquery
import pandas as pd

# Set your GCP project and dataset
PROJECT_ID = "dataengkestra"
DATASET = "DataEngDataSet"
TABLES = ["green_tripdata", "yellow_tripdata"]
SEEDS_DIR = "seeds"

client = bigquery.Client(project=PROJECT_ID)

os.makedirs(SEEDS_DIR, exist_ok=True)

for table in TABLES:
    table_ref = f"{PROJECT_ID}.{DATASET}.{table}"
    query = f"SELECT * FROM `{table_ref}`"
    df = client.query(query).to_dataframe()
    csv_path = os.path.join(SEEDS_DIR, f"{table}.csv")
    df.to_csv(csv_path, index=False)
    print(f"Exported {table} to {csv_path}")

print("All tables exported to seeds folder.")
