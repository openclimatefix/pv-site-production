import pathlib
import traceback

import pytest
from click.testing import CliRunner
from pvsite_datamodel.sqlmodels import ForecastSQL, ForecastValueSQL

from pv_site_production.app import main

CONFIG_FIXTURES = [
    x for x in pathlib.Path("tests/fixtures/model_configs").iterdir() if x.suffix == ".yaml"
]


@pytest.mark.parametrize("config_file", CONFIG_FIXTURES)
@pytest.mark.parametrize("write_to_db", [True, False])
def test_app(config_file: pathlib.Path, write_to_db: bool, db_session):

    # The script creates its own Database Connection object so it's not possible to use the
    # `db_session` defined in `conftest.py` that automatically removes the rows.
    # Because of this we compare rows before and rows after.
    def get_num_rows() -> dict[str, int]:
        return {
            table.__table__.name: db_session.query(table).count()  # type: ignore
            for table in [ForecastSQL, ForecastValueSQL]
            # TODO Currently nothing is written in LatestForecastValueSQL
            # , LatestForecastValueSQL]
        }

    num_rows_before = get_num_rows()

    runner = CliRunner()

    cmd_args = ["--config", str(config_file), "--date", "2022-1-1-11-50"]
    if write_to_db:
        cmd_args.append("--write-to-db")

    result = runner.invoke(main, cmd_args)
    assert result.exit_code == 0, traceback.print_exception(result.exception)

    # Without this the output to stdout/stderr is grabbed by click's test runner.
    print(result.output)

    num_rows_after = get_num_rows()

    # Make sure forecast are written in the DB when we passe the --write-to-db option, and that
    # none are written otherwise.
    for table_name, num_rows in num_rows_after.items():
        if write_to_db:
            assert num_rows > num_rows_before[table_name]
        else:
            assert num_rows == num_rows_before[table_name]
