# #! Previous well-working version without chrome and chromedriver
# FROM apache/airflow:2.6.0

# # User root to install necessary libraries
# USER root
# RUN apt-get update && apt-get install -y --no-install-recommends apt-utils
# RUN apt-get -y install curl
# RUN apt-get install libgomp1


# # Changing to airflow user
# USER airflow

# # Generating necessary environment variables
# ENV AIRFLOW_HOME=/opt/airflow
# ENV C_FORCE_ROOT="true"

# # Installing python libraries
# COPY requirements.txt /requirements.txt
# RUN pip install --user --upgrade pip
# RUN pip install --no-cache-dir --user -r /requirements.txt


# # Copying dags and plugins
# WORKDIR ${AIRFLOW_HOME}


#! New version

FROM apache/airflow:2.6.0

# User root to install necessary libraries
USER root

# Set up environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV C_FORCE_ROOT="true"

# Set Chrome and Chromedriver versions
# Check available versions of Chromedriver here: https://chromedriver.chromium.org/downloads
# Check available versions of Chrome here: https://www.ubuntuupdates.org/package/google_chrome/stable/main/base/google-chrome-stable
ARG CHROME_DRIVER_VERSION=113.0.5672.63
ARG CHROME_VERSION=113.0.5672.126-1

# Update and install required packages
RUN apt-get update && apt-get install -y --no-install-recommends apt-utils
RUN apt-get -y install curl
RUN apt-get install libgomp1
RUN apt-get -y install wget

# Install Chrome and Chromedriver
RUN apt -y update && apt install --no-install-recommends --yes unzip && \
    wget https://chromedriver.storage.googleapis.com/${CHROME_DRIVER_VERSION}/chromedriver_linux64.zip && \
    unzip chromedriver_linux64.zip && \
    chmod +x chromedriver && \
    mv chromedriver /usr/local/bin/ && \
    rm chromedriver_linux64.zip && \
    wget --no-verbose -O /tmp/chrome.deb https://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_${CHROME_VERSION}_amd64.deb && \
    apt install -y /tmp/chrome.deb && \
    rm /tmp/chrome.deb

# Changing to airflow user
USER airflow

# Installing python libraries
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
# If I get some errors with the installation of libraries, remove '--user' for both previous lines

# Copying dags and plugins
WORKDIR ${AIRFLOW_HOME}


###########
# #! Daniel's code:

# # Check available versions here: https://chromedriver.chromium.org/downloads
# ARG CHROME_DRIVER_VERSION=107.0.5304.62
# # Check available versions here: https://www.ubuntuupdates.org/package/google_chrome/stable/main/base/google-chrome-stable
# ARG CHROME_VERSION=107.0.5304.121-1

# COPY requirements.txt /tmp/requirements.txt


# RUN apt -y update && apt install --no-install-recommends --yes unzip && \
#     wget https://chromedriver.storage.googleapis.com/${CHROME_DRIVER_VERSION}/chromedriver_linux64.zip && \
#     unzip chromedriver_linux64.zip && \
#     chmod +x chromedriver && \
#     rm chromedriver_linux64.zip && \
#     wget --no-verbose -O /tmp/chrome.deb https://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_${CHROME_VERSION}_amd64.deb && \
#     apt install -y /tmp/chrome.deb && \
#     apt install -y libcurl4-openssl-dev libssl-dev gcc && \
#     rm /tmp/chrome.deb && \
#     pip install -r /tmp/requirements.txt && \
#     apt remove --purge -y python3-pip python-pip curl unzip && \
#     apt-get clean autoclean && \
#     apt-get autoremove --yes && \
#     rm -rf /var/lib/{apt,dpkg,cache,log}