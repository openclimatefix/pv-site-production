from datetime import datetime

import yaml
from nowcasting_datamodel.connection import DatabaseConnection
from nowcasting_datamodel.models.base import Base_PV
from psp.ml.typings import X

from pv_site_production.data.pv_data_sources import DbPvDataSource
from pv_site_production.models.psp import get_model


def test_get_model():
    with open("tests/fixtures/model_configs/psp.yaml") as f:
        config = yaml.safe_load(f)

    db_connection = DatabaseConnection(
        url=config["pv_db_url"], base=Base_PV, echo=False
    )
    pv_data_source = DbPvDataSource(db_connection, config["pv_metadata_path"])

    model = get_model(config, pv_data_source)

    y = model.predict(X(pv_id=1, ts=datetime(2022, 1, 1, 6)))
    # The fixture model was trained with 13 horizons.
    assert y.powers.shape == (13,)
