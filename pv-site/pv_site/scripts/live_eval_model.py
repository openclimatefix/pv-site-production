import datetime as dt
import logging

import click
import numpy as np
import pandas as pd
import sqlalchemy as sa
from pv_site._db_helpers import get_forecasts, get_generation, get_site_uuids, rows_to_df

_log = logging.getLogger(__name__)


def _resample_df(df: pd.DataFrame, resample_minutes: int) -> pd.DataFrame:
    return df.groupby(
        [
            pd.Grouper(level="site_uuid"),
            pd.Grouper(
                freq=f"{resample_minutes}min",
                level="start_utc",
                convention="start",
                closed="left",
            ),
        ]
    ).mean()


@click.command()
@click.option("--db-url", help="Database that looks like: postgresql://your_url")
@click.option(
    "--history",
    "history_hours",
    type=int,
    show_default=True,
    default=8,
    help="How many hours to look back in time",
)
@click.option(
    "--horizon",
    "horizon_minutes",
    default=0,
    help="Horizon in minutes",
    show_default=True,
)
@click.option(
    "--log-level",
    default="info",
    show_default=True,
    help="logging level",
)
@click.option(
    "--max-sites",
    type=int,
    default=None,
    help="Limit the number of sites. This is useful for quick debugging.",
)
@click.option(
    "--resample-minutes",
    type=int,
    default=15,
    help="Resample both forecast and generation data to <resample-minutes> minutes before comparing"
    " the two.",
)
def main(
    db_url: str,
    history_hours: int,
    horizon_minutes: int,
    log_level: str,
    max_sites: int,
    resample_minutes: int,
):
    logging.basicConfig(level=log_level.upper())

    end_utc = dt.datetime.utcnow()
    start_utc = end_utc - dt.timedelta(hours=history_hours)

    engine = sa.create_engine(db_url, future=True)
    Session = sa.orm.sessionmaker(engine)

    with Session() as session:
        # Get the list of sites
        _log.info("Querying all sites.")
        site_uuids = get_site_uuids(session)

        print(f"We found {len(site_uuids)} sites.")

        if max_sites is not None:
            print(f"\nOnly keeping {max_sites}")
            site_uuids = site_uuids[:max_sites]

        # Get the ground truth for those sites.
        _log.info("Querying generation data.")
        generation_rows = get_generation(
            session,
            start_utc=start_utc,
            end_utc=end_utc,
            site_uuids=site_uuids,
        )

        _log.info("We got {len(generation_rows)} rows")

        df_generation = rows_to_df(generation_rows)

        df_generation = df_generation[["site_uuid", "start_utc", "generation_power_kw"]]

        df_generation = df_generation.set_index(["site_uuid", "start_utc"])

        df_generation = df_generation[df_generation["generation_power_kw"] > 0.001]

        site_uuids = df_generation.index.unique(level="site_uuid")

        print(f"\nThere were {len(site_uuids)} sites with non-trivial generation data.")

        _log.info("Querying the forecast data (this might take a few seconds)")

        forecast_rows = get_forecasts(
            session,
            start_utc=start_utc,
            end_utc=end_utc,
            site_uuids=site_uuids,
            horizon_minutes=horizon_minutes,
        )

        _log.info("We got {len(forecast_rows)} rows")

        df_forecasts = rows_to_df(forecast_rows)
        df_forecasts = df_forecasts[["site_uuid", "start_utc", "forecast_power_kw"]]
        df_forecasts = df_forecasts.set_index(["site_uuid", "start_utc"])

        _log.info(f"Resampling generation to {resample_minutes} minutes")

        # Resample both dataframes to have the same sampling rate.
        df_generation = _resample_df(df_generation, resample_minutes)
        df_forecasts = _resample_df(df_forecasts, resample_minutes)

        _log.info("Merging both dataframes")

        # print(df_generation.head())

        df = pd.concat([df_generation, df_forecasts], axis=1)

        num_nan_forecasts = (df["forecast_power_kw"].isna() * 1).sum()
        print(
            f"\nNum of NaN forecast values: {num_nan_forecasts}"
            f" ({num_nan_forecasts / len(df) * 100:.1f} %)"
        )

        # Remove the rows with NaN forecasts.
        df = df[~df["forecast_power_kw"].isna()]

        # We define the error as the (absolute) area between the two curves, divided by the area
        # under the ground truth.
        # This gives us a nice and interpretable pourcent that pretty much corresponds of our
        # intuitive idea of percent error.
        abs_diff = abs(df["forecast_power_kw"] - df["generation_power_kw"])
        sum_abs_diff = abs_diff.groupby(pd.Grouper(level="site_uuid")).sum()
        sum_generation = df["generation_power_kw"].groupby(pd.Grouper(level="site_uuid")).sum()

        # We get one error value per site.
        error = sum_abs_diff / sum_generation * 100.0

        print("\nWorse performing sites:")
        print(error.sort_values(ascending=False)[:10])

        print("\nStats on the errors per site")
        for stat in ["min", "max", "mean", "median", "std"]:
            print(f"{stat.capitalize()} error: {getattr(error, stat)():.1f} %")

        std = error.std()
        mean = error.mean()
        count = len(error)
        err = 1.96 * std / np.sqrt(count)

        print(
            f"\nConfidence interval (95%) on the mean: [{mean - err: .1f} %, { mean + err : .1f} %]"
        )


if __name__ == "__main__":
    main()
