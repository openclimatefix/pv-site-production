""" This file loads the NWP data, and saves it in the old format"""

import xarray as xr
from forecast_inference.utils.geospatial import lon_lat_to_osgb


def load_nwp_and_refactor(in_path: str, out_path:str) -> None:
    nwp = xr.open_dataset(in_path)

    # if um-ukv is in the datavars, then this comes from the new new-consumer
    # We need to rename the data variables, and
    # load in lat and lon, ready for regridding later.
    if 'um-ukv' in nwp.data_vars:

        # rename to UKV
        nwp = nwp.rename({"um-ukv": "UKV"})

        variable_coords = nwp.variable.values
        rename = {"cloud_cover_high": "hcc",
                  "cloud_cover_low": "lcc",
                  "cloud_cover_medium": "mcc",
                  "cloud_cover_total": "tcc",
                  "snow_depth_gl": "sde",
                  "direct_shortwave_radiation_flux_gl": "sr",
                  "downward_longwave_radiation_flux_gl": "dlwrf",
                  "downward_shortwave_radiation_flux_gl": "dswrf",
                  "downward_ultraviolet_radiation_flux_gl": "duvrs",
                  "relative_humidity_sl": "r",
                  "temperature_sl": "t",
                  "total_precipitation_rate_gl": "prate",
                  "visibility_sl": "vis",
                  "wind_direction_10m": "wdir10",
                  "wind_speed_10m": "si10",
                  "wind_v_component_10m": "v10",
                  "wind_u_component_10m": "u10"}

        for k, v in rename.items():
            variable_coords[variable_coords == k] = v

        # assign the new variable names
        nwp = nwp.assign_coords(variable=variable_coords)

        # this is all taken from the metoffice website, apart from the x and y values
        lat = xr.open_dataset("forecast_inference/data/nwp-consumer-mo-ukv-lat.nc")
        lon = xr.open_dataset("forecast_inference/data/nwp-consumer-mo-ukv-lon.nc")

        # convert lat, lon to osgb
        x, y = lon_lat_to_osgb(lon.longitude.values, lat.latitude.values)

        # combine with d, and just taking a 1-d array.
        nwp = nwp.assign_coords(x_osgb=x[0])
        nwp = nwp.assign_coords(y_osgb=y[:, 0])

    nwp.to_netcdf(out_path)