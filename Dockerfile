FROM apache/airflow:2.6.0

# User root to install necessary libraries
USER root
RUN apt-get update && apt-get install -y --no-install-recommends apt-utils
RUN apt-get -y install curl
RUN apt-get install libgomp1


# Changing to airflow user
USER airflow

# Generating necessary environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV C_FORCE_ROOT="true"

# Installing python libraries
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt


# Copying dags and plugins
WORKDIR ${AIRFLOW_HOME}