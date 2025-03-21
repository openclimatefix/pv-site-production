"""
NWP Data Source
"""
import logging
import pyproj
import xarray as xr
import ocf_blosc2  # noqa

# OSGB is also called "OSGB 1936 / British National Grid -- United
# Kingdom Ordnance Survey".  OSGB is used in many UK electricity
# system maps, and is used by the UK Met Office UKV model.  OSGB is a
# Transverse Mercator projection, using 'easting' and 'northing'
# coordinates which are in meters.  See https://epsg.io/27700
OSGB36 = 27700

# This is the Lambert Azimuthal Equal Area projection used in the UKV data
lambert_aea2 = {
    "proj": "laea",
    "lat_0": 54.9,
    "lon_0": -2.5,
    "x_0": 0.0,
    "y_0": 0.0,
    "ellps": "WGS84",
    "datum": "WGS84",
}

laea = pyproj.Proj(**lambert_aea2)  # type: ignore[arg-type]
osgb = pyproj.Proj(f"+init=EPSG:{OSGB36}")

laea_to_osgb = pyproj.Transformer.from_proj(laea, osgb).transform

logger = logging.getLogger(__name__)


def download_and_add_osgb_to_nwp_data_source(
    from_nwp_path: str, to_nwp_path: str, variables_to_keep: None | list = None
) -> None:
    """
    Download and add OSBG to the NWP data source.
    """

    logger.debug(f"Loading NWP data from {from_nwp_path}")
    nwp = xr.open_zarr(from_nwp_path)

    # if um-ukv is in the datavars, then this comes from the new new-consumer > 1.0.0
    # We need to rename the data variables, and
    # add osgb coordinates to the data source
    if "um-ukv" in nwp.data_vars:
        logger.info("Renaming the UKV variables")

        # rename to UKV
        nwp = nwp.rename({"um-ukv": "UKV"})

        variable_coords = nwp.variable.values
        rename = {
            "cloud_cover_high": "hcc",
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
            "wind_u_component_10m": "u10",
        }

        for k, v in rename.items():
            variable_coords[variable_coords == k] = v

        # assign the new variable names
        nwp = nwp.assign_coords(variable=variable_coords)

        # rename x_laea and y_laea to x and y
        nwp = nwp.rename({"x_laea": "x", "y_laea": "y"})

        # copy x to x_laea and y to y_laea
        nwp = nwp.assign_coords(x_laea=nwp.x)
        nwp = nwp.assign_coords(y_laea=nwp.y)

        # calculate latitude and longitude from x_laea and y_laea
        # x is an array of 455, and y is an array of 639
        # we need to change x to a 2d array of shape (455, 639)
        # and y to a 2d array of shape (455, 639)
        x, y = nwp.x_laea.values, nwp.y_laea.values
        x = x.reshape(1, -1).repeat(len(nwp.y_laea.values), axis=0)
        y = y.reshape(-1, 1).repeat(len(nwp.x_laea.values), axis=1)

        # calculate latitude and longitude from x and y
        x_osgb, y_osgb = laea_to_osgb(xx=x, yy=y)

        # we just take 1-d versions of x_osgb and y_osgb, and reassign
        nwp = nwp.assign_coords(x=x_osgb[0])
        nwp = nwp.assign_coords(y=y_osgb[:, 0])

    # keep only the variables we need
    if variables_to_keep is not None:
        nwp = nwp.sel(variable=variables_to_keep)

    # save to zarr
    logger.debug(f"Saving NWP data from {to_nwp_path}")
    for v in list(nwp.coords.keys()):
        if nwp.coords[v].dtype == object:
            nwp.coords[v] = nwp.coords[v].astype("unicode")

    # re order to (variable, init_time, step, y, x)
    nwp = nwp.transpose("variable", "init_time", "step", "y", "x")

    # adjust chunk size to (1,1,43,639,455)
    nwp = nwp.chunk({"variable": 1, "init_time": 1, "step": 43, "y": 100, "x": 100})

    # save to zarr
    nwp.to_zarr(to_nwp_path, mode="w", safe_chunks=False)
