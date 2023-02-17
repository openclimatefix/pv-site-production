from datetime import datetime

import yaml
from psp.typings import X
from pvsite_datamodel.connection import DatabaseConnection
from pvsite_datamodel.sqlmodels import SiteSQL

from pv_site_production.data.pv_data_sources import DbPvDataSource
from pv_site_production.models.psp import get_model


def test_get_model():
    with open("tests/fixtures/model_configs/psp.yaml") as f:
        config = yaml.safe_load(f)

    # We could use the sqlalchemy objects from the fixtures but we can also do everything from
    # scratch using the config.
    database_connection = DatabaseConnection(config["pv_db_url"])
    pv_data_source = DbPvDataSource(database_connection, config["pv_metadata_path"])

    model = get_model(config, pv_data_source)

    with database_connection.get_session() as session:  # type: ignore
        site = session.query(SiteSQL).first()

    y = model.predict(X(pv_id=str(site.site_uuid), ts=datetime(2022, 1, 1, 11, 50)))
    # The fixture model was trained with 48 * 4 horizons.
    assert y.powers.shape == (48 * 4,)
