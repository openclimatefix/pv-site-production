from pv_site_production.models.pv_nwp.load import load_model


def test_load_model():
    model = load_model()
    # TODO
    print(model)
    # assert model is not None
