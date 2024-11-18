import logging
import os
import uuid
import fsspec
from typing import Optional

from pvsite_datamodel.sqlmodels import ForecastSQL, ForecastValueSQL, SiteGroupSQL
from sqlalchemy.orm import Session
import pandas as pd


logging.basicConfig(
    level=getattr(logging, os.getenv("LOGLEVEL", "INFO")),
    format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
)
_log = logging.getLogger(__name__)


def get_site_uuids(session: Session, site_group_names: [str]) -> list[uuid.UUID]:
    """
    Get the site uuids for the site group names

    :param session:
    :param site_group_names: list of site group names
    :return:
    """

    site_group_names = site_group_names

    site_uuids_all_sites = []
    for site_group_name in site_group_names:
        # get the site group
        site_group = (
            session.query(SiteGroupSQL)
            .filter(SiteGroupSQL.site_group_name == site_group_name)
            .first()
        )

        if site_group is None:
            _log.error(f"Site group {site_group_name} not found in the database")
        else:
            # get the site uuids
            sites = site_group.sites
            site_uuids = [site.site_uuid for site in sites]

            # reduce down to 100 if needed
            if len(site_uuids) > 100:
                _log.error(
                    f"Site group {site_group_name} has more than 100 sites, " f"only saving 100"
                )
                site_uuids = site_uuids[:100]

            site_uuids_all_sites.extend(site_uuids)

    return site_uuids_all_sites


def save_forecast_and_values(
    session: Session,
    forecast_uuids: list[uuid.UUID],
    directory: str,
    index: int = 0,
    site_uuids: Optional[list[uuid.UUID]] = None,
):
    """
    Save forecast and forecast values to csv
    :param session: database session
    :param forecast_uuids: list of forecast uuids
    :param directory: the directory where they should be saved
    :param index: the index of the file, we delete the forecasts in batches,
        so there will be several files to save
    :param site_uuids: list of site uuids to save, if its None, then we ignore this
    """
    _log.info(f"Saving data to {directory}")

    fs = fsspec.open(directory).fs
    # check folder exists, if it doesnt, add it
    if not fs.exists(directory):
        fs.mkdir(directory)

    if site_uuids is not None:
        forecast_uuids = (
            session.query(ForecastSQL.forecast_uuid)
            .filter(ForecastSQL.site_uuid.in_(site_uuids))
            .all()
        )

    # loop over both forecast and forecast_values tables
    for table in ["forecast", "forecast_value"]:
        model = ForecastSQL if table == "forecast" else ForecastValueSQL

        # get data
        query = session.query(model).where(model.forecast_uuid.in_(forecast_uuids))
        forecasts_sql = query.all()
        forecasts_df = pd.DataFrame([f.__dict__ for f in forecasts_sql])

        # drop column _sa_instance_state if it is there
        if "_sa_instance_state" in forecasts_df.columns:
            forecasts_df = forecasts_df.drop(columns="_sa_instance_state")

        # drop forecast_value_uuid as we dont need it
        if table == "forecast_value":
            forecasts_df = forecasts_df.drop(columns="forecast_value_uuid")

        # save to csv
        _log.info(f"saving to {directory}, Saving {len(forecasts_df)} rows to {table}.csv")
        forecasts_df.to_csv(f"{directory}/{table}_{index}.csv", index=False)
