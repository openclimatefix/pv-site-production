import datetime as dt
import os

import pytest
import sqlalchemy as sa
from pvsite_datamodel.sqlmodels import ForecastSQL, ForecastValueSQL, SiteSQL

from forecast_inference.scripts.live_eval_model import main
from forecast_inference.utils.testing import run_click_script


@pytest.fixture
def forecasts(database_connection, now):
    num_forecasts = 3
    num_forecast_values = 4

    with database_connection.get_session() as db_session:
        site_uuids = db_session.scalars(sa.select(SiteSQL.site_uuid)).all()

        for site_uuid in site_uuids:
            for i in range(num_forecasts):

                timestamp_utc = now - dt.timedelta(minutes=i + 1)

                forecast = ForecastSQL(
                    site_uuid=site_uuid,
                    timestamp_utc=timestamp_utc,
                    forecast_version="0",
                )

                db_session.add(forecast)
                db_session.commit()

                for j in range(num_forecast_values):
                    fv = ForecastValueSQL(
                        forecast_uuid=forecast.forecast_uuid,
                        forecast_power_kw=123.0, # type: ignore
                        start_utc=timestamp_utc + dt.timedelta(minutes=j),
                        end_utc=timestamp_utc + dt.timedelta(minutes=j + 1),
                        horizon_minutes=0,
                    )
                    db_session.add(fv)
                    db_session.commit()


def test_live_eval_model(forecasts, database_connection):
    """Make sure the script properly runs."""
    args = ["--db-url", os.environ["OCF_PV_DB_URL"], "--max-sites", "2", "--resample-minutes", "1"]

    result = run_click_script(main, args, catch_exceptions=False)

    assert result.exit_code == 0
    assert "Confidence interval" in result.output
