# import packages
import cdsapi
import numpy as np
from concurrent.futures import ProcessPoolExecutor
import argparse
from retrying import retry

# decorator for retrying network request
@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=3)

# define function for download
def download_data(year, month):
    # most years       
        c = cdsapi.Client()
        result = c.service("tool.toolbox.orchestrator.workflow",
        params={ 
            "realm": "user-apps",
            "project": "app-c3s-daily-era5-statistics",
            "version": "master",
            "kwargs": {
                "dataset": "reanalysis-era5-single-levels",
                "product_type": "reanalysis",
                "variable": "sea_surface_temperature",
                "statistic": "daily_mean",
                "year": year,
                "month": month,
                "time_zone": "UTC+00:00",
                "frequency": "1-hourly",
                "grid": "0.25/0.25",
                "area": {"lat": [35, 45], "lon": [-20, -5]} 
            },
            "workflow_name": "application"
        })

        c.download(result, [f"../Data.nosync/ERA5/SST/SST_{year}_{int(month):02d}.nc"]) 
        print(f"{year}_{int(month):02d}")


# donwload
def main():
    parser = argparse.ArgumentParser(description='Download data using parallel processing.')
    parser.add_argument('--workers', type=int, default=4, help='Number of workers for parallel processing')
    args = parser.parse_args()

    # define variables for download
    YEARS = np.array(np.arange(1981, 2024, 1), dtype='str')
    MONTHS = np.array(np.arange(1, 13, 1), dtype='str')

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        for year in YEARS:
            for month in MONTHS:
                    executor.submit(download_data, year, month)

if __name__ == "__main__":
    main()






