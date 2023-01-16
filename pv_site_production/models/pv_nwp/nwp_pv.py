from typing import Optional

import pandas as pd
from ocf_datapipes.utils.consts import BatchKey

from pv_site_production.models.pv_nwp.load import load_model


def run_one_batch(batch: dict, model: Optional = None) -> pd.DataFrame:

    if model is None:
        model = load_model()

    pv_ids = batch[BatchKey.pv_id]
    batch_size = pv_ids.shape[0]

    results_df = []
    # loop over examples from batch
    for i in range(batch_size):

        # get pv_id
        # shape: (batch_size, n_pv_systems)
        pv_id = pv_ids[i, 0]

        # make xarray from model history
        # shape: (batch_size, time, n_pv_systems)
        pv_history = batch[BatchKey.pv][i, :, 0]
        # TODO change to xarray

        # pass timestamp, pv_id and xarray of history into model
        # TODO
        # results = model.predict()

        # format results into dataframe, columns will be
        # "target_datetime_utc"
        # "forecast_kw"
        # "pv_uuid"
        results = pd.DataFrame()  # update

        results_df.append(results)

    # join all example together
    results_df = pd.concat(results_df)

    return results_df
