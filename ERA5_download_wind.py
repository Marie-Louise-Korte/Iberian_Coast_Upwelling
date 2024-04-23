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
    if variable == "10m_v_component_of_wind":
        c.download(result, [f"../Data.nosync/Surface_stress/Wind_10m/V_{year}_{int(month):02d}.nc"]) 
        print(f"N_{year}_{int(month):02d}")
    else:
       c.download(result, [f"../Data.nosync/Surface_stress/Wind_10m/U_{year}_{int(month):02d}.nc"])
       print(f"E_{year}_{int(month):02d}")
        
   
# donwload
def main():
    parser = argparse.ArgumentParser(description='Download data using parallel processing.')
    parser.add_argument('--workers', type=int, default=4, help='Number of workers for parallel processing')
    args = parser.parse_args()

    # define variables for download
    YEARS = np.array(np.arange(2020, 2021, 1), dtype='str')
    MONTHS = np.array(np.arange(1, 13, 1), dtype='str')
    VARIABLES = ["10m_v_component_of_wind", "10m_u_component_of_wind"]

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



