from datetime import datetime

import yaml
from psp.ml.typings import X

from pv_site_production.data.pv_data_sources import DbPvDataSource
from pv_site_production.models.psp.psp_model import get_model


def test_get_model(db_connection):
    with open("tests/fixtures/model_configs/psp.yaml") as f:
        config = yaml.safe_load(f)

    pv_data_source = DbPvDataSource(db_connection, config["pv_metadata_path"])

    model = get_model(config, pv_data_source)

    y = model.predict(X(pv_id=1, ts=datetime(2022, 1, 1, 6)))
    # The fixture model was trained with 13 horizons.
    assert y.powers.shape == (13,)
