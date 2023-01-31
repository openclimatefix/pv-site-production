from datetime import datetime

from pvsite_datamodel.sqlmodels import SiteSQL

from pv_site_production.data.pv_data_sources import DbPvDataSource
from pv_site_production.models.common import apply_model
from pv_site_production.models.psp import get_model

# from pv_site_production.models.cos.cos_model import get_model


def test_common(db_session):

    pv_ids = [str(row.site_uuid) for row in db_session.query(SiteSQL)]

    assert len(pv_ids) > 0

    pv_data_source = DbPvDataSource(db_session, "tests/fixtures/pv_metadata.csv")

    model = get_model(
        {
            "model_path": "tests/fixtures/psp_model_fixture.pkl",
            "nwp": {
                "cls": "pv_site_production.data.nwp_data_sources.NwpDataSource",
                "kwargs": {"path": "tests/fixtures/nwp_fixture.zarr"},
            },
        },
        pv_data_source=pv_data_source,
    )

    df = apply_model(model, pv_ids, datetime(2022, 1, 1, 12))

    print(df)
    print(len(df))

    assert len(df) > 0
