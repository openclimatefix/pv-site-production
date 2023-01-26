FROM python:3.10-slim

# install extra requirements
RUN apt-get clean
RUN apt-get update -y
RUN apt-get install gcc g++ libgeos-dev -y git

# copy files requirements
COPY ./pyproject.toml /app/pyproject.toml
COPY ./poetry.lock /app/poetry.lock
COPY ./README.md /app/README.md
RUN pip install -U pip poetry

# copy main files
COPY ./pv_site_production /app/pv_site_production
COPY ./tests /app/tests

# set working directory
WORKDIR /app

# pull sub module, this should make the folder 'pv-site-prediction'
COPY ./.git /.git
COPY ./.gitmodules /app/.gitmodules
RUN git submodule update --init --recursive

# verbose
COPY ./pv-site-prediction /app/pv-site-prediction

# install files
RUN poetry install

# make sure 'src' is in python path - this is so imports work
ENV PYTHONPATH=${PYTHONPATH}:/app/pv_site_production
