"""
Common stuff related to models.
"""

from datetime import timedelta

import pandas as pd
from psp.models.base import PvSiteModel
from psp.typings import PvId, Timestamp, X


def apply_model(model: PvSiteModel, pv_ids: list[PvId], ts: Timestamp) -> pd.DataFrame:
    """Run a model on PVs at a given timestamp.

    Returns a dataframe with the following columns
        "forecast_kw"
        "pv_uuid"
        "target_start_utc"
        "target_end_utc"
    """
    records: list[dict] = []

    for pv_id in pv_ids:
        pred = model.predict(X(pv_id=pv_id, ts=ts))
        for horizon, power in zip(model.config.horizons, pred.powers):
            # TODO Make sure we have consistent units for the forecast.
            # We might want something like "energy" for the given time interval.
            records.append(
                {
                    # TODO Make sure we use the same units everywhere.
                    "forecast_kw": power / 1000 * 12,
                    "pv_uuid": pv_id,
                    "target_start_utc": ts + timedelta(minutes=horizon[0]),
                    "target_end_utc": ts + timedelta(minutes=horizon[1]),
                }
            )

    return pd.DataFrame.from_records(records)
