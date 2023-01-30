r"""
Simple fake model, that just uses the time of day.

Daily profile
       _-_
     /    \
___/       \____
0  6   12  18  24
    (Time)

"""

from datetime import timedelta
from typing import Any

import numpy as np
from psp.data.data_sources.pv import PvDataSource
from psp.ml.models.base import PvSiteModel, PvSiteModelConfig
from psp.ml.typings import Features, X, Y

from pv_site_production.models.cos.intensities import make_fake_intensity


class CosModel(PvSiteModel):
    """Baseline model using a cosine function."""

    def get_features(self, x: X) -> Features:
        """Get a features dictionary from the input."""
        return {"ts": x.ts}

    def predict_from_features(self, features: Features) -> Y:
        """Get the output from features."""
        ts = features["ts"]
        tss = [ts + timedelta(minutes=f[1] - f[0]) for f in self.config.future_intervals]
        powers = np.array([make_fake_intensity(ts) for ts in tss])
        return Y(powers=powers)


def get_model(config: dict[str, Any], pv_data_source: PvDataSource | None) -> PvSiteModel:
    """Get a ready cosine model."""
    model_config = PvSiteModelConfig(
        # 15 minute itervervals for 48 hours.
        future_intervals=[(i * 15, (i + 1) * 15) for i in range(4 * 48)],
        blackout=0,
    )
    # TODO make the setup argument optional in pv-site-prediction.
    return CosModel(model_config, setup_config=None)
