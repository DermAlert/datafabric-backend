# pull official base image
FROM python:3.11-slim-bookworm

# set working directory
WORKDIR /usr/src/app

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PYSPARK_HADOOP_VERSION 3
ENV PYSPARK_PYTHON=python3.11
ENV PYSPARK_DRIVER_PYTHON=python3.11

RUN apt-get update && apt-get install -y openjdk-17-jdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# install system dependencies
RUN apt-get update \
  && apt-get -y install netcat-openbsd gcc postgresql curl \
  && apt-get clean

# install poetry
RUN curl -sSL https://install.python-poetry.org | python3 -
# Add poetry to PATH
ENV PATH="/root/.local/bin:$PATH"

# copy the poetry configuration files
COPY ./pyproject.toml ./poetry.lock* /usr/src/app/

# install python dependencies (including dev dependencies)
RUN poetry install --no-root  # This installs both regular and dev dependencies

# add app
COPY . .
