import xarray as xr
import numpy as np
from global_land_mask import globe

# write as function for calculating the upwelling Index
def upwelling_index(DS, lat, lon, eastward_stress, northward_stress):
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
   
    # I need the angle of the coast ... as this is a large scale process I will just go with the approximate angle 
    # prehaps refine later

    # constants (?) -> see if I can / need to adjust coast angle
    # assuming 0 degree angle (= 0 rads) as Portugues coast is pretty well aligned with stratigh south-north direction
        # so I think there is an error Gomez... say that varphi is the unitarian vector pointing landward perpendicular to
        # the coast but then they go and add pi/2 (i.e. 90°) to it so maybe it is unclear in how they describe it but I am
        # going to set varphi = the angle of the coast (i.e. 0°)
    varphi = 0  # angle of the unitary vector perpendicular to coast pointing landward (i.e. west-east -> 90° 
    varphi_rad = varphi * (np.pi/180) 
    
    theta = np.pi/2 + varphi_rad #? why this step?

    # -› positive UI values mean upwelling favourable conditions
    DS['upwelling_index'] = -np.sin(theta) * DS.ek_trans_u + np.cos(theta) * DS.ek_trans_v
    DS.upwelling_index.attrs.update({"name" : "upwelling index", 
                                     "info" : "positive values upwelling favourable \nassuming Portuguese coast at 0°", 
                                     "ref" : "Gomez-Gesteira et al. 2006"})
    return DS

def land_mask_add(DS):
    lon_grid, lat_grid = np.meshgrid(DS.lon.values, DS.lat.values)
    land_mask = globe.is_land(lat_grid, lon_grid)                     # needs input with lat first then lon
    DS['land_mask'] = (('lat', 'lon'), land_mask)
    DS.land_mask.attrs.update({"name" : "land mask", "values" : "0 : land \n1 : ocean", "info" : "uses global_land_mask package"})  
    return DS
