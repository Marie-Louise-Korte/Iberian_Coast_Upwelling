#!/Users/marie-louisekorte/miniconda3/envs/IbUpPy3.9.12

# activate the conda env
import subprocess
conda_env = "IbUpPy3.9.12"
subprocess.run(f"conda init --users", shell=True)
subprocess.run(f"conda activate {conda_env}", shell=True)

# import packages
import cdsapi
import numpy as np
from concurrent.futures import ProcessPoolExecutor
import argparse
from retrying import retry

# decorator for retrying network request
@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=3)

# define function for download
def download_data(year, month, variable):
    c = cdsapi.Client()
    result = c.service(
        "tool.toolbox.orchestrator.workflow",
        params={ 
            "realm": "user-apps",
            "project": "app-c3s-daily-era5-statistics",
            "version": "master",
            "kwargs": {
                "dataset": "reanalysis-era5-single-levels",
                "product_type": "reanalysis",
                "variable": variable,
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
    if variable == "mean_northward_turbulent_surface_stress":
        c.download(result, [f"../Data.nosync/Surface_stress/Turbulent_mean/N_{year}_{int(month):02d}.nc"]) 
        print(f"N_{year}_{int(month):02d}")
    else:
        c.download(result, [f"../Data.nosync/Surface_stress/Turbulent_mean/E_{year}_{int(month):02d}.nc"])
        print(f"E_{year}_{int(month):02d}")


# donwload
def main():
    parser = argparse.ArgumentParser(description='Download data using parallel processing.')
    parser.add_argument('--workers', type=int, default=4, help='Number of workers for parallel processing')
    args = parser.parse_args()

    # define variables for download
    YEARS = np.array(np.arange(1996, 2001, 1), dtype='str')
    MONTHS = np.array(np.arange(1, 13, 1), dtype='str')
    VARIABLES = ["mean_northward_turbulent_surface_stress", "mean_eastward_turbulent_surface_stress"]

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        for year in YEARS:
            for month in MONTHS:
                for variable in VARIABLES:
                    executor.submit(download_data, year, month, variable)

if __name__ == "__main__":
    main()

