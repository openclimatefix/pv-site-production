import pickle
from typing import Any

import pandas as pd
from psp.data.data_sources.nwp import NwpDataSource
from psp.data.data_sources.pv import PvDataSource
from psp.ml.models.base import PvSiteModel

# TODO This should live in psp.ml.models.base
from psp.ml.models.recent_history import SetupConfig


def get_model(config: dict[str, Any], pv_data_source: PvDataSource) -> pd.DataFrame:

    model: PvSiteModel = pickle.load(open(config["model_path"], "rb"))

    nwp_data_source = NwpDataSource(config["nwp_path"])

    # TODO Make the setup step uniform across all `psp` models. In other words it should be defined
    # directly in `PvSiteModel`.
    model.setup(
        SetupConfig(pv_data_source=pv_data_source, nwp_data_source=nwp_data_source)
    )

    return model
