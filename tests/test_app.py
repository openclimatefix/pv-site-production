import pathlib
import traceback

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
    result = runner.invoke(
        run,
        ["--config", config_file, "--date", "2022-1-1-06-00"],
        catch_exceptions=True,
    )
    # print('OUTPUT')
    # print(result.output)
    # print('EXC INFO')
    # print(result.exc_info)
    # print('EXCEPTION')
    if result.exception:
        traceback.print_exception(result.exception)
    assert result.exit_code == 0
