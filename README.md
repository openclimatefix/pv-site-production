# PV Site Production

## Repo structure

[`./infrastructure`][inf]: Docker files for the services


At the core of each service is a self-contained python package. See the individual directories:

[`./forecast-inference`][infe]: Service that runs the site forecasting model in inference

[`./database-cleanup`][arc]: Service that archives the forecasts to keep the database small


## Development

### Run the CI for the whole repo

To make sure that everything builds, passes the lint and tests, you can run

    make all

Optionally run everything in parallel:

    make all -j 8


[inf]: ./infrastructure
[infe]: ./forecast-inference
[arc]: ./database-cleanup
