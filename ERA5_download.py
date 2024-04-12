import cdsapi

c = cdsapi.Client()

YEARS = np.array(np.arange(2021,2024,1), dtype = 'str')
MONTHS = np.array(np.arange(1,13,1), dtype = 'str')


for year in YEARS:
    for month in MONTHS: 
     
        result = c.service(
            "tool.toolbox.orchestrator.workflow",
             params={ 
                "realm": "user-apps",
                "project": "app-c3s-daily-era5-statistics",
                "version": "master",
                "kwargs": {
                         "dataset": "reanalysis-era5-single-levels",
                         "product_type": "reanalysis",
                         "variable": "mean_northward_turbulent_surface_stress",
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
        c.download(result,[f"../Data.nosync/Surface_stress/Turbulent_mean/N_{year}_{month}.nc"])


for year in YEARS:
    for month in MONTHS: 
     
        result = c.service(
            "tool.toolbox.orchestrator.workflow",
             params={ 
                "realm": "user-apps",
                "project": "app-c3s-daily-era5-statistics",
                "version": "master",
                "kwargs": {
                         "dataset": "reanalysis-era5-single-levels",
                         "product_type": "reanalysis",
                         "variable": "mean_eastward_turbulent_surface_stress",
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
        c.download(result,[f"../Data.nosync/Surface_stress/Turbulent_mean/E_{year}_{month}.nc"])
