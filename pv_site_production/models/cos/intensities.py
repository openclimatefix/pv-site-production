""" Function to make a solar intensity from one datetime """
from datetime import datetime

import numpy as np

from pv_site_production.models.cos.cos_model import TOTAL_MINUTES_IN_ONE_DAY


def make_fake_intensity(datetime_utc: datetime) -> float:
    """
    Make a fake intesnity value based on the time of the day

    :param datetime_utc:
    :return: intensity, between 0 and 1
    """

    fraction_of_day = (datetime_utc.hour * 60 + datetime_utc.minute) / TOTAL_MINUTES_IN_ONE_DAY
    # use single cos**2 wave for intensity, but set night time to zero
    if (fraction_of_day > 0.25) & (fraction_of_day < 0.75):
        intensity = np.cos(2 * np.pi * fraction_of_day) ** 2
    else:
        intensity = 0.0
    return intensity
