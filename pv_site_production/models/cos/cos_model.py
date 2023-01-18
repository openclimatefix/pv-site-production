r"""
This is a simple fake model, that just uses the time of day.

Daily profile
       _-_
     /    \
___/       \____
0  6   12  18  24
    (Time)

"""

import pathlib
from datetime import timedelta

from psp.ml.models.base import PvSiteModel, PvSiteModelConfig
from psp.ml.typings import Features, X, Y

from pv_site_production.models.cos.intensities import make_fake_intensity


class CosModel(PvSiteModel):
    def get_features(self, x: X) -> Features:
        return {"ts": x.ts}

    def predict_from_features(self, features: Features) -> Y:
        ts = features["ts"]
        tss = [
            ts + timedelta(minutes=f[1] - f[0]) for f in self.config.future_intervals
        ]
        return Y(powers=[make_fake_intensity(ts) for ts in tss])


def get_model(config: pathlib.Path) -> PvSiteModel:
    model_config = PvSiteModelConfig(
        # 15 minute itervervals for 48 hours.
        future_intervals=[(i * 15, (i + 1) * 15) for i in range(4 * 48)],
        blackout=0,
    )
    # TODO make the setup argument optional in pv-site-prediction.
    return CosModel(model_config, setup_config=None)
