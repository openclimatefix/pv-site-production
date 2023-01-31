from datetime import datetime

import yaml

# from nowcasting_datamodel.connection import DatabaseConnection
# from nowcasting_datamodel.models.base import Base_PV
from psp.ml.typings import X
from pvsite_datamodel.sqlmodels import SiteSQL
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from pv_site_production.data.pv_data_sources import DbPvDataSource
from pv_site_production.models.psp import get_model


def test_get_model(db_session):
    with open("tests/fixtures/model_configs/psp.yaml") as f:
        config = yaml.safe_load(f)

    # We could use the sqlalchemy objects from the fixtures but we can also do everything from
    # scratch using the config.
    pv_data_source = DbPvDataSource(db_session, config["pv_metadata_path"])

    model = get_model(config, pv_data_source)

    site = db_session.query(SiteSQL).first()

    y = model.predict(X(pv_id=str(site.site_uuid), ts=datetime(2022, 1, 1, 11, 50)))
    # The fixture model was trained with 13 horizons.
    assert y.powers.shape == (13,)
