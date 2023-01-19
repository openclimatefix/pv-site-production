FROM python:3.10-slim

# install extra requirements
RUN apt-get clean
RUN apt-get update -y
RUN apt-get install gcc g++ libgeos-dev -y

# copy files requirements
COPY ./pyproject.toml /app/pyproject.toml
COPY ./poetry.lock /app/poetry.lock
RUN pip install -U pip poetry

# set working directory
WORKDIR /app

COPY ./pv-site-prediction /app/pv-site-prediction
COPY ./pv_site_production /app/pv_site_production
COPY ./tests /app/tests

# install files
RUN ls
RUN ls
RUN poetry install

# make sure 'src' is in python path - this is so imports work
ENV PYTHONPATH=${PYTHONPATH}:/app/pv_site_production
