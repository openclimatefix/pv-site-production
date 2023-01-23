from typing import Any

import pandas as pd
from psp.data.data_sources.pv import PvDataSource

# TODO This should live in psp.ml.models.base
from psp.ml.models.recent_history import SetupConfig
from psp.ml.serialization import load_model

from pv_site_production.utils.imports import instantiate


def get_model(config: dict[str, Any], pv_data_source: PvDataSource) -> pd.DataFrame:
    model = load_model(config["model_path"])

    nwp_data_source = instantiate(**config["nwp"])

    # TODO Make the setup step uniform across all `psp` models. In other words it should be defined
    # directly in `PvSiteModel`.
    model.setup(
        SetupConfig(pv_data_source=pv_data_source, nwp_data_source=nwp_data_source)
    )

    return model
