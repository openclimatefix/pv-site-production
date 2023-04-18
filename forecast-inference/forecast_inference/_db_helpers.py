import datetime as dt
from typing import Any

import pandas as pd
import pvsite_datamodel.sqlmodels as models
import sqlalchemy as sa
from sqlalchemy.orm.session import Session

Row = Any


def get_site_uuids(session: Session) -> list[str]:
    """Get all the sites."""
    return session.scalars(sa.select(models.SiteSQL.site_uuid)).all()


def get_generation(
    session: Session,
    *,
    start_utc: dt.datetime,
    end_utc: dt.datetime,
    site_uuids: list[str],
) -> list[Row]:
    """Get generation rows for a given time window and site uuids."""
    stmt = (
        sa.select(
            models.GenerationSQL.site_uuid,
            models.GenerationSQL.start_utc,
            models.GenerationSQL.end_utc,
            models.GenerationSQL.generation_power_kw,
        )
        .where(models.GenerationSQL.site_uuid.in_(site_uuids))
        .where(models.GenerationSQL.start_utc >= start_utc)
        .where(models.GenerationSQL.start_utc < end_utc)
    )
    return session.execute(stmt).all()


def get_forecasts(
    session: Session,
    *,
    start_utc: dt.datetime,
    end_utc: dt.datetime,
    site_uuids: list[str],
    horizon_minutes: int,
) -> list[Row]:
    """Get forecast rows for a given time window, site uuids and horizon."""
    stmt = (
        sa.select(
            models.ForecastSQL.site_uuid,
            models.ForecastValueSQL.start_utc,
            models.ForecastValueSQL.end_utc,
            models.ForecastValueSQL.forecast_power_kw,
        )
        .select_from(models.ForecastValueSQL)
        .join(models.ForecastSQL)
        .where(models.ForecastSQL.site_uuid.in_(site_uuids))
        .where(models.ForecastValueSQL.horizon_minutes == horizon_minutes)
        .where(models.ForecastValueSQL.start_utc >= start_utc)
        .where(models.ForecastValueSQL.start_utc < end_utc)
    )

    return session.execute(stmt).all()


def rows_to_df(rows: list[Row]) -> pd.DataFrame:
    """Util to turn database rows into pandas dataframe."""
    columns = list(rows[0]._mapping.keys())

    data = [{key: getattr(row, key) for key in columns} for row in rows]

    df = pd.DataFrame.from_records(data)

    # Change the types on some columns based on the name.
    for col in columns:
        if col.endswith("_utc"):
            df[col] = pd.to_datetime(df[col])
        elif col.endswith("_uuid"):
            df[col] = df[col].astype(str)

    return df
