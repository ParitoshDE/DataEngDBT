from google.cloud import bigquery
import os
import gzip

PROJECT_ID = "dataengkestra"
DATASET = "DataEngDataSet"
DATA_DIR = "nyc-taxi-data"

client = bigquery.Client(project=PROJECT_ID)

for fname in os.listdir(DATA_DIR):
    if fname.endswith(".csv.gz"):
        table_name = fname.replace(".csv.gz", "").replace("-", "_")
        table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
        csv_path = os.path.join(DATA_DIR, fname)
        print(f"Uploading {csv_path} to {table_id}...")

        with gzip.open(csv_path, "rt") as f_in:
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True,
            )
            job = client.load_table_from_file(f_in, table_id, job_config=job_config)
            job.result()

        print(f"Loaded {csv_path} into {table_id}")

print("All files uploaded to BigQuery.")
