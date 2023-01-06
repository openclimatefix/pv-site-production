FROM python:3.10-slim

# copy files requirements
COPY ./requirements.txt /app/requirements.txt

# install requirements
RUN apt-get clean
RUN apt-get update -y
RUN apt-get install gcc g++ libgeos-dev -y
RUN pip install -U pip
RUN pip install -r /app/requirements.txt

# set working directory
WORKDIR /app

# copy files over
COPY ./pv_site_production /app/pv_site_production
COPY ./tests /app/tests

# make sure 'src' is in python path - this is so imports work
ENV PYTHONPATH=${PYTHONPATH}:/app/pv_site_production
