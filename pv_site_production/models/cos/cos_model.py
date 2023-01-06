r"""
This is a simple fake model, that just uses the time of day.

Daily profile
       _-_
     /    \
___/       \____
0  6   12  18  24
    (Time)

"""

import os
import sys
from typing import List, Optional

import pandas as pd
from ocf_datapipes.training.simple_pv import simple_pv_datapipe
from ocf_datapipes.utils.consts import BatchKey

from pv_site_production.models.cos.intensities import make_fake_intensity

TOTAL_MINUTES_IN_ONE_DAY = 24 * 60


def run_cos_model(configuration_filename: Optional[str] = None) -> pd.DataFrame:
    """
    Running the cos model.

    This model take the time of day and makes curve that is zdero at night, and non zero in the day

    :param configuration_filename: The configuration file.
        This is optional, configuration.yaml is loaded by default
    :return: A dataframe with the following columns
        - forecast_kw, the forecast value
        - pv_uuid, the pv uuid value
        - target_datetime_utc, the target time for the forecast
    """

    # set up datapipes
    if configuration_filename is None:
        configuration_filename = os.path.join(sys.path[0], "configuration.yaml")
    data_pipeline = simple_pv_datapipe(configuration_filename=configuration_filename)

    # set up dataloader
    predict_dataloader = iter(data_pipeline)

    # run through batches for all PV sites
    # TODO change to actual number of batches
    results = []
    for _ in range(2):
        batch = next(predict_dataloader)

        y = run_one_batch(batch)
        results.append(y)

    # format results into dataframe and validate,
    # change list of dict to dataframe
    results_df = pd.DataFrame(results, columns=["pv_uuid", "target_datetime_utc", "forecast_kw"])

    return results_df


def run_one_batch(batch) -> List[dict]:
    """
    Run on batch

    :param batch: batch from ocf_datapipes
    :return: List of dictionary of results. The dictionary hows the follow keys
        - forecast_kw, the forecast value
        - pv_uuid, the pv uuid value
        - target_datetime_utc, the target time for the forecast
    """

    pv_t0_idx = batch[BatchKey.pv_t0_idx]
    datetimes_utc = batch[BatchKey.pv_time_utc][:, pv_t0_idx:]
    # this has shale [b, time]
    ids = batch[BatchKey.pv_id]

    intensities = []
    for example_idx in range(len(ids)):
        datetimes_utc_one_example = datetimes_utc[example_idx]
        id_one_example = ids[example_idx]

        # TODO would be good to vectorise this
        intensities_one_batch = [
            {
                "forecast_kw": make_fake_intensity(pd.to_datetime(datetime)),
                "pv_uuid": id_one_example,
                "target_datetime_utc": datetime,
            }
            for datetime in datetimes_utc_one_example
        ]
        intensities = intensities + intensities_one_batch

    return intensities
