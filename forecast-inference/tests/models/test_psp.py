import sqlalchemy as sa
import yaml
from psp.typings import X
from pvsite_datamodel.connection import DatabaseConnection
from pvsite_datamodel.sqlmodels import SiteSQL

from forecast_inference.data.pv_data_sources import DbPvDataSource
from forecast_inference.models.psp import get_model


def test_get_model(now):
    with open("tests/fixtures/model_configs/psp.yaml") as f:
        config = yaml.safe_load(f)

    # We could use the sqlalchemy objects from the fixtures but we can also do everything from
    # scratch using the config.
    database_connection = DatabaseConnection(config["pv_db_url"])
    pv_data_source = DbPvDataSource(database_connection)

    model = get_model(config, pv_data_source)

    with database_connection.get_session() as session:
        site: SiteSQL = session.scalars(sa.select(SiteSQL).limit(1)).one()

    y = model.predict(X(pv_id=str(site.site_uuid), ts=now))
    # The fixture model was trained with 48 * 4 horizons.
    assert y.powers.shape == (48 * 4,)
