from pv_site_production.models.pv_nwp import nwp_pv
from ocf_datapipes.batch.fake.fake_batch import make_fake_batch
from ocf_datapipes.config.load import load_yaml_configuration
import os


def get_batch():
    configuration_file = os.path.dirname(nwp_pv.__file__) + "/configuration.yaml"
    configuration = load_yaml_configuration(configuration_file)
    return make_fake_batch(configuration=configuration)


def test_run_one_batch():

    batch = get_batch()
    results = nwp_pv.run_one_batch(batch=batch)

    # TODO add asserts
