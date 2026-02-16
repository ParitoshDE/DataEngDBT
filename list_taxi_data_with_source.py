import os

years = [2019, 2020]
months = [f'{m:02d}' for m in range(1, 13)]
types = ['yellow', 'green']

print(f"{'File Name':50} {'Size (MB)':>10}  Source URL")
print('-'*90)
for taxi_type in types:
    for year in years:
        for month in months:
            fname = f"{taxi_type}_tripdata_{year}-{month}.csv.gz"
            fpath = os.path.join('nyc-taxi-data', fname)
            url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{fname}"
            if os.path.exists(fpath):
                size_mb = os.path.getsize(fpath) / 1024 / 1024
                print(f"{fname:50} {size_mb:10.2f}  {url}")
            else:
                print(f"{fname:50} {'MISSING':>10}  {url}")
