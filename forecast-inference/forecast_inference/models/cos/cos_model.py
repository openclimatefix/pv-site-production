r"""
This is a simple fake model, that just uses the time of day.

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
from psp.data_sources.pv import PvDataSource
from psp.models.base import PvSiteModel, PvSiteModelConfig
from psp.typings import Features, Horizons, X, Y

from forecast_inference.models.cos.intensities import make_fake_intensity


class CosModel(PvSiteModel):
    """Baseline model using a cosine function."""

    def get_features(self, x: X, is_training: bool = False) -> Features:
        """Get a features dictionary from the input."""
        # Features are supposed to be ndarrays but we know this won't actually break anything.
        return {"ts": x.ts}  # type: ignore

    def predict_from_features(self, x: X, features: Features) -> Y:
        """Get the output from features."""
        ts = features["ts"]
        tss = [ts + timedelta(minutes=f[1] - f[0]) for f in self.config.horizons]
        powers = np.array([make_fake_intensity(ts) for ts in tss])
        return Y(powers=powers)


def get_model(config: dict[str, Any], pv_data_source: PvDataSource | None) -> PvSiteModel:
    """Get a ready cosine model."""
    model_config = PvSiteModelConfig(
        # 15 minute itervervals for 48 hours.
        horizons=Horizons(duration=15, num_horizons=4 * 48),
    )
    # TODO make the setup argument optional in pv-site-prediction.
    return CosModel(model_config)
