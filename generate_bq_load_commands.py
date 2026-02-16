import os
import glob

# User configuration

BUCKET = 'dataengkestrabucket'  # <-- Change this to your bucket name
FOLDER = '.'  # <-- Use current directory if files are not in a folder
DATASET = 'DataEngDataSet'           # <-- Change to your BigQuery dataset name

# List all CSV.GZ files in the folder
files = sorted(glob.glob(os.path.join(FOLDER, '*.csv.gz')))


# Generate bq load command for yellow files
yellow_pattern = 'yellow_tripdata_20*.csv.gz'
if FOLDER == '.':
    yellow_gcs_path = f'gs://{BUCKET}/{yellow_pattern}'
else:
    yellow_gcs_path = f'gs://{BUCKET}/{FOLDER}/{yellow_pattern}'
yellow_cmd = f"bq load --autodetect --source_format=CSV --skip_leading_rows=1 {DATASET}.yellow_tripdata {yellow_gcs_path}"
print(yellow_cmd)

# Generate bq load command for green files
green_pattern = 'green_tripdata_20*.csv.gz'
if FOLDER == '.':
    green_gcs_path = f'gs://{BUCKET}/{green_pattern}'
else:
    green_gcs_path = f'gs://{BUCKET}/{FOLDER}/{green_pattern}'
green_cmd = f"bq load --autodetect --source_format=CSV --skip_leading_rows=1 {DATASET}.green_tripdata {green_gcs_path}"
print(green_cmd)
