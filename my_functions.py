## relevant packages
import xarray as xr
import numpy as np
from global_land_mask import globe


##################################################################################################################
## calculate the upwelling Index #################################################################################
##################################################################################################################

def calc_upwelling_index(DS, lat, lon, eastward_stress, northward_stress):
    # this function adds the new variables it creates to the input dataset DS
    
    ## Admin
    # to avoid any weird variable attributes appearing where they don't belong
    xr.set_options(keep_attrs=False)
    
    ## 1. Step
    # method based on Oxford Lecture 7 (maybe find better reference)

    # constants
    phi = 1025                                    # density of seawater -> kg/m^3
    omega = 7.2921159e-5                          # angular velocity -> rad/s
    lat_rad = lat * (np.pi / 180)                 # latitude in radians -> rad

    # calculate the Coriolis parameter f
    f = 2 * omega * np.sin(lat_rad.values)        # Coriolis parameter -> rad/s^2
    # create a cube with f -> to fit 
    DS_shape = eastward_stress.shape
    f_cube = np.ones(DS_shape)
    for i in np.arange(0,41):
        f_cube[:,i,:] = f[i]

    # calculate depth integrated Ekman transport    
    # m^2/s -> eastward transport (u) is calculated from wind stress in northward direction (v)
    DS['ek_trans_u'] = northward_stress / (phi * f_cube)    
    DS.ek_trans_u.attrs.update({"name" : "eastward Ekman transport integrated across Ekman layer (U_Ek)", 
                                "ref" : "m^2/s", "ref" : "Lecture 7, Oxford"})
    # m^2/s -> northward transport (v) is calculated from wind stress in eastward direction (u)
    DS['ek_trans_v'] = eastward_stress / (phi * f_cube)    
    DS.ek_trans_v.attrs.update({"name" : "northward Ekman transport integrated across Ekman layer (V_Ek)", 
                                "ref" : "m^2/s", "ref" : "Lecture 7, Oxford"})

    # calculate the magnitude of the wind stress (? pretty sure that it is that)
    DS['wind_stress'] = np.hypot(eastward_stress, northward_stress)
    DS.wind_stress.attrs.update({"name" : "combined wind stress", "units" : "N/m^2"})

    ## 2. Step
    # calculate upwelling index (based on Gomez-Gesteira et al. 2006 -> maybe find better/original ref)

    # assuming 0 degree angle (= 0 rads) as Portugues coast is pretty well aligned with stratigh south-north direction
    
    # I need the angle of the coast ... as this is a large scale process I will just go with the approximate angle 
        # prehaps refine later ->  I am calculating the UI fro 10°W so I will just stick with an approximation of the 
        # portuguese coast that is perfectly meridional

    # angle of the unitary vector perpendicular to coast pointing landward 
        # (the angle of the unitarian vector is defined relative to 'x-axis' i.e. west-east -> 0°) 
    varphi = 0   
    varphi_rad = varphi * (np.pi/180) 
    
    theta = np.pi/2 + varphi_rad #? why this step?

    # -np.sin (changes the sign of the output) -> positive UI values mean upwelling favourable conditions
    DS['UI'] = -np.sin(theta) * DS.ek_trans_u + np.cos(theta) * DS.ek_trans_v
    DS.UI.attrs.update({"name" : "upwelling index", 
                                     "info" : "positive values upwelling favourable \nassuming Portuguese coast at 0°", 
                                     "ref" : "Gomez-Gesteira et al. 2006"})
    return DS


##################################################################################################################
## add weekly index to my weekly resampled data & use index to assign month ######################################
##################################################################################################################

def add_week_and_month(DS_weekly_mean):
    # creat an index for the weeks in the year, runs from 1 to 52 (53 some years) and then repeats
    DS_weekly_mean['week_of_year'] = DS_weekly_mean.time.dt.isocalendar().week
    DS_weekly_mean.week_of_year.attrs.update({'info' : 'index according to week of the year'})

    # create a variable containing the assigned months of every week (has 53 weeks), the 53 week won't be used later and is therefore called 'Drop_53'
    # this is following the information I got from Joaquim on someone else who used this to remove week 53 from CoRTAD data 
    # the weeks are split up into months like this:
        # Jan (1:4) - Feb (5:8) - Mar (9:13) 
        # Apr (14:17) - May (18:21) - Jun (22:26) 
        # Jul (27:30) - Aug (31:34) - Sep (35:39) 
        # Oct (40:43) - Nov (44:47) - Dec (48:52)
    months_list = ['Jan', 'Jan', 'Jan', 'Jan', 'Feb', 'Feb', 'Feb', 'Feb', 'Mar', 'Mar', 'Mar', 'Mar', 'Mar',
     'Apr', 'Apr', 'Apr', 'Apr', 'May', 'May', 'May', 'May', 'Jun', 'Jun', 'Jun', 'Jun', 'Jun',
     'Jul', 'Jul', 'Jul', 'Jul', 'Aug', 'Aug', 'Aug', 'Aug', 'Sep', 'Sep', 'Sep', 'Sep', 'Sep',
     'Oct', 'Oct', 'Oct', 'Oct', 'Nov', 'Nov', 'Nov', 'Nov', 'Dec', 'Dec', 'Dec', 'Dec', 'Dec', 'Drop_53']

    # create an array the same length as my time coordinate and assign the months to the correct week in year indices
    month_array = np.zeros(len(DS_weekly_mean.time))
    month_array = month_array.astype(str)
    
    for i in np.arange(0, len(DS_weekly_mean.week_of_year)):
        ind = DS_weekly_mean.week_of_year[i].values
        month_array[i] = months_list[ind - 1]
    
    # assign as variable to the dataset -> just to have a reference for later
    DS_weekly_mean['month'] = ('time', month_array)
    DS_weekly_mean.month.attrs.update({'info' : 'gives the month each week in the dataset is assigned to'})
    
    return DS_weekly_mean

##################################################################################################################
# create a summer subset, if I want to change my subset I can change the min and max week ########################
##################################################################################################################

def subset_summer(DS_weekly_mean, min_week = 22, max_week = 39): 

    DS_weekly_mean = add_week_and_month(DS_weekly_mean)
    DS_new = DS_weekly_mean.where(((DS_weekly_mean.week_of_year >= min_week) & (DS_weekly_mean.week_of_year <= max_week)), drop = True)
    
    return DS_new


##################################################################################################################
# calculate the meridional mean ##################################################################################
##################################################################################################################

def calc_meridional_mean(DS, variable = 'UI', min_lat = 37.012264, max_lat = 43.844013, lon = -10):
    
    var = DS[f'{variable}']
    
    # default match the extent of the coast to UI_SST (min 37.012264, max 43.844013)
    mean = var.sel(lon = lon, method = 'nearest').sel(lat = slice(min_lat, max_lat)).mean(dim = 'lat')
    
    DS[f'{variable}_mean'] = mean
    DS[f'{variable}_mean'].attrs.update({'info' : f'meridional mean at 10°W (-10°) between {min_lat}°N and {max_lat}°N'})
    
    return DS
    
##################################################################################################################
## calculate land mask -> has become a bit redundant -> I am using the ERA5 land-sea mask now ####################
##################################################################################################################

def add_land_mask(DS):
    lon_grid, lat_grid = np.meshgrid(DS.lon.values, DS.lat.values)
    land_mask = globe.is_land(lat_grid, lon_grid)                     # needs input with lat first then lon
    DS['land_mask'] = (('lat', 'lon'), land_mask)
    DS.land_mask.attrs.update({"name" : "land mask", "values" : "0 : land \n1 : ocean", "info" : "uses global_land_mask package"})  
    return DS



