"""
Models from the `pv-site-prediction` repo.
"""
import logging
import os
from typing import Any

from psp.data_sources.pv import PvDataSource
from psp.models.base import PvSiteModel
from psp.serialization import load_model

from forecast_inference.data.nwp_data_sources import download_and_add_osgb_to_nwp_data_source
from forecast_inference.utils.imports import instantiate
from forecast_inference.utils.profiling import profile

_log = logging.getLogger(__name__)


def get_model(config: dict[str, Any], pv_data_source: PvDataSource) -> PvSiteModel:
    """Get a serialized pv-site-prediction model."""

    with profile(f'Loading model: {config["model_path"]}'):
        model = load_model(config["model_path"])

    # download and add osbg to nwp datasource
    if os.getenv("NWP_ZARR_PATH") is not None:
        download_and_add_osgb_to_nwp_data_source(os.getenv("NWP_ZARR_PATH"), "nwp.zarr")

    with profile(f'Getting NWP data: {config["nwp"]}'):
        nwp_data_sources = instantiate(**config["nwp"])

    # TODO Make the setup step uniform across all `psp` models. In other words it should be defined
    # directly in `PvSiteModel`.
    with profile("Set data sources"):
        model.set_data_sources(
            pv_data_source=pv_data_source,
            nwp_data_sources={"ukv": nwp_data_sources},
        )

    return model
