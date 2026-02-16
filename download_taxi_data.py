import os
import requests

os.makedirs('nyc-taxi-data', exist_ok=True)

years = [2019, 2020]
months = [f'{m:02d}' for m in range(1, 13)]
types = ['yellow', 'green']

for taxi_type in types:
    for year in years:
        for month in months:
            url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{taxi_type}_tripdata_{year}-{month}.csv.gz"
            out_path = f"nyc-taxi-data/{taxi_type}_tripdata_{year}-{month}.csv.gz"
            print(f"Downloading {url} ...")
            try:
                r = requests.get(url, stream=True)
                r.raise_for_status()
                with open(out_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"Saved to {out_path}")
            except Exception as e:
                print(f"Failed to download {url}: {e}")
