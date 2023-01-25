"""
Models from the `pv-site-prediction` repo.
"""

from typing import Any

from psp.data.data_sources.pv import PvDataSource
from psp.ml.models.base import PvSiteModel
from psp.ml.models.recent_history import SetupConfig
from psp.ml.serialization import load_model

from pv_site_production.utils.imports import instantiate


def get_model(config: dict[str, Any], pv_data_source: PvDataSource) -> PvSiteModel:
    """Get a serialized pv-site-prediction model."""
    model = load_model(config["model_path"])

    nwp_data_source = instantiate(**config["nwp"])

    # TODO Make the setup step uniform across all `psp` models. In other words it should be defined
    # directly in `PvSiteModel`.
    model.setup(
        SetupConfig(pv_data_source=pv_data_source, nwp_data_source=nwp_data_source)
    )

    return model
