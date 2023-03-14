FROM python:3.10-slim

# install extra requirements
RUN apt-get clean
RUN apt-get update -y
RUN apt-get install gcc g++ libgeos-dev -y git -y pkg-config -y libhdf5-dev

# copy files requirements
COPY ./pyproject.toml /app/pyproject.toml
COPY ./poetry.lock /app/poetry.lock
COPY ./README.md /app/README.md
COPY ./configurations/aws_config.yaml /app/configuration.yaml
RUN pip install -U pip poetry


# set working directory
WORKDIR /app

# Submodules
COPY ./.git /.git
COPY ./.gitmodules /app/.gitmodules
RUN git submodule update --init --recursive
RUN mv /pv-site-prediction /app/pv-site-prediction

# Install dependencies
RUN poetry install --without dev --no-root

# Install the package itself in a different step because it changes more often than the
# dependencies.
COPY ./pv_site_production /app/pv_site_production
RUN poetry install --only-root

# make sure 'src' is in python path - this is so imports work
ENV PYTHONPATH=${PYTHONPATH}:/app/pv_site_production

# Note that we round down the time at which we make the prediction to 15 minutes.
# This means that if we don't want predictions to be (up-to) 15 minute late,
# we need to make sure this runs *right after* these "round" times.
ENTRYPOINT ["poetry", "run", "python", "pv_site_production/app.py", "-c", "configuration.yaml", "--write-to-db", "--round-date-to-minutes", "15", "--log-level", "info"]
