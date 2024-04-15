#!/Users/marie-louisekorte/miniconda3/bin/python3

import cdsapi
import numpy as np

c = cdsapi.Client() #(timeout=3600)

YEARS = np.array(np.arange(20015,2020,1), dtype = 'str')
MONTHS = np.array(np.arange(1,13,1), dtype = 'str')
VARIABLES= ["mean_northward_turbulent_surface_stress", "mean_eastward_turbulent_surface_stress"]


for year in YEARS:
    for month in MONTHS:
        for variable in VARIABLES:
     
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
                c.download(result,[f"../Data.nosync/Surface_stress/Turbulent_mean/N_{year}_{int(month):02d}.nc"]) 
                print(f"N_{year}_{int(month):02d}")
            else:
                c.download(result,[f"../Data.nosync/Surface_stress/Turbulent_mean/E_{year}_{int(month):02d}.nc"])
                print(f"E_{year}_{int(month):02d}")

