import pathlib
import traceback

import pytest
from click.testing import CliRunner

from pv_site_production.app import main

CONFIG_FIXTURES = [
    x for x in pathlib.Path("tests/fixtures/model_configs").iterdir() if x.suffix == ".yaml"
]


@pytest.mark.parametrize("config_file", CONFIG_FIXTURES)
def test_app(config_file: pathlib.Path):
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["--config", str(config_file), "--date", "2022-1-1-11-50"],
    )
    assert result.exit_code == 0, traceback.print_exception(result.exception)
