from datetime import datetime

from psp.typings import X

from pv_site_production.models.cos.cos_model import get_model, make_fake_intensity


def test_make_fake_intensities():

    datetimes = [datetime(2021, 6, 1, hour) for hour in range(0, 24)]

    assert make_fake_intensity(datetimes[0]) == 0
    assert make_fake_intensity(datetimes[12]) == 1
    assert make_fake_intensity(datetimes[-1]) == 0


def test_run_cos_model():
    model = get_model(config={}, pv_data_source=None)
    y = model.predict(X(pv_id="1", ts=datetime(2022, 1, 1, 6)))
    assert len(y.powers) == 48 * 4
