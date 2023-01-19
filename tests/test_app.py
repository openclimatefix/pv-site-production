import pathlib

import pytest
from click.testing import CliRunner

from pv_site_production.app import run

CONFIG_FIXTURES = [
    x
    for x in pathlib.Path("tests/fixtures/model_configs").iterdir()
    if x.suffix == ".yaml"
]


@pytest.mark.parametrize("config_file", CONFIG_FIXTURES)
def test_app(config_file: pathlib.Path):
    runner = CliRunner()
    result = runner.invoke(run, ["--config", config_file], catch_exceptions=False)
    print(result.output)
    assert result.exit_code == 0
