import tempfile
from datetime import datetime

import pytest
from ocf_datapipes.batch.fake.fake_batch import make_fake_batch
from ocf_datapipes.config.model import PV, Configuration, PVFiles
from ocf_datapipes.config.save import save_yaml_configuration

from pv_site_production.models.cos.cos_fake_model import (
    make_fake_intensity,
    run_cos_model,
    run_one_batch,
)


def test_make_fake_intensities():

    datetimes = [datetime(2021, 6, 1, hour) for hour in range(0, 24)]

    assert make_fake_intensity(datetimes[0]) == 0
    assert make_fake_intensity(datetimes[12]) == 1
    assert make_fake_intensity(datetimes[-1]) == 0


# export PYTHONPATH=${PYTHONPATH}:/pv_site_production


def test_run_one_batch():
    configuration = Configuration()
    configuration.input_data.pv = PV(history_minutes=60, forecast_minutes=60 * 24)

    batch = make_fake_batch(configuration=configuration)

    results = run_one_batch(batch)

    assert len(results) == 288 * 32


@pytest.mark.skip("Need to out fake database of PV database")
def test_run_cos_model():

    configuration = Configuration()
    configuration.input_data.pv = PV(
        history_minutes=60, forecast_minutes=60 * 24, pv_files_groups=[PVFiles()], is_live=True
    )

    # TODO need to add fake PV database, thate
    with tempfile.NamedTemporaryFile(suffix=".yaml") as fp:

        filename = fp.name

        # save default config to file
        save_yaml_configuration(Configuration(), filename)

        run_cos_model(configuration_filename=filename)
