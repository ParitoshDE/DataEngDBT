import os
import gzip
import shutil
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "dataengkestra"
DATASET = "DataEngDataSet"
DATA_DIR = "nyc-taxi-data"

# Explicitly use service account credentials (hardcoded path)
credentials = service_account.Credentials.from_service_account_file(
    r"C:\DataEng\GCPKey\dataengkestra-e43fdbcce97e.json"
)
client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

# Merge all files by taxi type
for taxi_type in ["yellow", "green"]:
    merged_csv = os.path.join(DATA_DIR, f"{taxi_type}_tripdata_merged.csv")
    files = sorted([f for f in os.listdir(DATA_DIR) if f.startswith(taxi_type) and f.endswith(".csv.gz")])
    header_written = False
    with open(merged_csv, "w", encoding="utf-8") as out_f:
        for fname in files:
            fpath = os.path.join(DATA_DIR, fname)
            with gzip.open(fpath, "rt", encoding="utf-8") as in_f:
                header = in_f.readline()
                if not header_written:
                    out_f.write(header)
                    header_written = True
                for line in in_f:
                    out_f.write(line)
    print(f"Merged {len(files)} files into {merged_csv}")

    # Upload merged CSV to BigQuery
    table_id = f"{PROJECT_ID}.{DATASET}.{taxi_type}_tripdata"
    # Drop table if it exists
    try:
        client.delete_table(table_id, not_found_ok=True)
        print(f"Dropped existing table {table_id}")
    except Exception as e:
        print(f"Warning: Could not drop table {table_id}: {e}")
    with open(merged_csv, "rb") as source_file:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
        )
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
        job.result()
    print(f"Uploaded {merged_csv} to {table_id}")

print("All merged files uploaded to BigQuery.")
