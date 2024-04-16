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
    # most years       
    if year in YEARS[1:-1]:
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

    # exceptions for 1981 I only want December and for 2024 only January
    if year == "1981":
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
                    "year": "1981",
                    "month": "12",
                    "time_zone": "UTC+00:00",
                    "frequency": "1-hourly",
                    "grid": "0.25/0.25",
                    "area": {"lat": [35, 45], "lon": [-20, -5]} 
                },
                "workflow_name": "application"
            })
        if variable == "mean_northward_turbulent_surface_stress":
            c.download(result, [f"../Data.nosync/Surface_stress/Turbulent_mean/N_1981_12.nc"]) 
            print(f"N_1981_12")
        else:
            c.download(result, [f"../Data.nosync/Surface_stress/Turbulent_mean/E_1981_12.nc"])
            print(f"E_1981_12")
            
    if year == "2024":
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
                    "year": "2024",
                    "month": "1",
                    "time_zone": "UTC+00:00",
                    "frequency": "1-hourly",
                    "grid": "0.25/0.25",
                    "area": {"lat": [35, 45], "lon": [-20, -5]} 
                },
                "workflow_name": "application"
            })
        if variable == "mean_northward_turbulent_surface_stress":
            c.download(result, [f"../Data.nosync/Surface_stress/Turbulent_mean/N_2024_01.nc"]) 
            print(f"N_2024_01")
        else:
            c.download(result, [f"../Data.nosync/Surface_stress/Turbulent_mean/E_2024_01.nc"])
            print(f"E_2024_01")

# donwload
def main():
    parser = argparse.ArgumentParser(description='Download data using parallel processing.')
    parser.add_argument('--workers', type=int, default=4, help='Number of workers for parallel processing')
    args = parser.parse_args()

    # define variables for download
    YEARS = np.array(np.arange(1981, 1982, 1), dtype='str')
    MONTHS = np.array(np.arange(1, 13, 1), dtype='str')
    VARIABLES = ["mean_northward_turbulent_surface_stress", "mean_eastward_turbulent_surface_stress"]

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        for year in YEARS:
            for month in MONTHS:
                for variable in VARIABLES:
                    executor.submit(download_data, year, month, variable)

if __name__ == "__main__":
    main()


## I tried to specify conda env in script... but didn't work

#!/Users/marie-louisekorte/miniconda3/bin/python3
##!/Users/marie-louisekorte/miniconda3/envs/IbUpPy3.9.12

## activate the conda env
# import subprocess
# conda_env = "IbUpPy3.9.12"
## check if conda is initialized for the current shell
# init_output = subprocess.run(["conda", "shell.bash", "hook", "--dry-run"], capture_output=True)
# if init_output.returncode != 0:
#     # initialize conda for the current shell
#     subprocess.run(["conda", "init"], shell=True)
# subprocess.run(f"conda activate {conda_env}", shell=True)



