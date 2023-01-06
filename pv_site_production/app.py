import click
from pv_site_production.models.cos.cos_fake_model import run_cos_model


# choose model

# run model

# save to datbase

@click.command()
@click.option(
    "--model-name",
    default="cnn",
    envvar="MODEL_NAME",
    help="Select which model to use",
    type=click.STRING,
)
def run(
    model_name: str = "nwp_simple",
):

    # choose model
    if model_name == 'cos':
        run_model = run_cos_model
    else:
        Exception(f'Could not find model {model_name}')

    # run model, this returns a list of dataframes with the following columns
    # "t0_datetime_utc"
    # "target_datetime_utc"
    # "forecast_kw"
    # "pv_uuid"
    results_df = run_model()

    # save model
    # TODO


if __name__ == "__main__":
    run()



