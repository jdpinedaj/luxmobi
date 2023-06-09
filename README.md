# Data Portal

![badge1](https://img.shields.io/badge/language-Python-blue.svg)
![badge2](https://img.shields.io/badge/orchestrator-Airflow-brightgreen.svg)
![badge3](https://img.shields.io/badge/containerization-Docker-red.svg)

## General

This repository is associated to the data portal of the University of Luxembourg managed by the [MobiLab](https://mobilab.lu/) research group.


## Framework

The data portal is based on the following framework:

![Diagram showing the framework of the data portal](readme-resources/framework.png)

- **Data sources**:
The data sources are the different data providers that are used to feed the data portal. 
    - `Bike data`: The bike data is provided by the [jcdecaux](https://api.jcdecaux.com/vls/v1/stations?contract=Luxembourg&apiKey=4507a17cda9135dd36b8ff13d8a4102ab3aa44a0) service.
    - `Charging stations data`: The charging stations data is provided by the [data public lu](https://data.public.lu) service.
    - `Traffic counters data`: The traffic counters data is provided by the [cita lu](http://www.cita.lu) service.
    - `Parking data`: The parking data is provided by the [Ville de Luxembourg](https://www.vdl.lu) website.
    - `Google Popular Times data`: The Google popular Times data is provided by the [Google Maps](https://www.google.com/maps) service.
<br/>
     
- **Data Extraction**:
The data extraction is performed using Python as main programming language, using libraries such as `requests`, and `selenium` with `chrome driver` and `chrome browser`. 
<br/>

- **Data Transformation**:
The data transformation is performed using Python as main programming language, using libraries such as `pandas`.
<br/>

- **Data Loading**:
Finally, the data is loaded into a [PostgreSQL](https://www.postgresql.org/) database.
<br/>

- **DB Management**:
[Ngrok](https://ngrok.com/) is used to expose the PostgreSQL database to the internet.
Then, [DBeaver](https://dbeaver.io/) is used to manage the database, even though it can be managed using other tools such as [DataGrip](https://www.jetbrains.com/datagrip/).
<br/>

The entire pipeline is containerized using [Docker](https://www.docker.com/). and it is orchestrated using [Apache Airflow](https://airflow.apache.org/).
Finally, all the resources are stored in a [GitHub](https://github.com/jdpinedaj/luxmobi) repository.



<!-- ---
## Connecting to the PostgreSQL database

Using ngrok to expose the PostgreSQL database to the internet:

```
ngrok tcp 5432
```

Then, use the host and port provided by ngrok to connect to the database using DBeaver or any other tool.

Example of ngrok output:
![ngrok output](readme-resources/ngrok_example.jpeg)

Example of DBeaver connection:
![DBeaver connection](readme-resources/dbeaver_example.png)


 -->

---

## Running Airflow on Docker

#### 1. docker-compose.yaml

To deploy Airflow on Docker Compose, you should fetch docker-compose.yaml.

```
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.6.0/docker-compose.yaml'
```

In addition, please include the following element in the postgres service:

```
    ports:
        - "5432:5432"
```

#### 2. Initializing environment

On Linux, the quick-start needs to know your host user id and needs to have group id set to 0. Otherwise the files created in dags, logs and plugins will be created with root user. You have to make sure to configure them for the docker-compose:

```
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```


#### 3. Initialize the database

On all operating systems, you need to run database migrations and create the first user account. To do it, run.

```
docker-compose up airflow-init -d
```

#### 4. Cleaning-up the environment

The docker-compose we prepare is a “Quick-start” one. It is not intended to be used in production and it has a number of caveats - one of them being that the best way to recover from any problem is to clean it up and restart from the scratch.

The best way to do it is to:

- Run docker-compose down --volumes --remove-orphans command in the directory you downloaded the docker-compose.yaml file
- remove the whole directory where you downloaded the docker-compose.yaml file rm -rf '<DIRECTORY>'
- re-download the docker-compose.yaml file
- re-start following the instructions from the very beginning in this guide

#### 5. Running Airflow

Now you can start all services:

```
docker-compose up -d
```

#### 6. Extending the image to include custom packages after initialization of the database

If you want to install python requirements to the airflow container after the initialization of the database, you can do it by:

- adding the requirements to the requirements.txt file
- rebuilding the image docker-compose build by running `docker-compose build`
- restarting the containers by running `docker-compose up -d`

#### 7. Accessing the environment via a browser using the web interface

Check http://localhost:8080

#### 8. Airflow connection and Postgres Operator

In Airflow/Admin/Connections

- Conncection ID: postgres_default
- Connection type: postgres
- Host: postgres # or host.docker.internal
- Schema: luxmobi
- Login: nipi
- Password: MobiLab1
- Port: 5432

#### 9. Reseting Docker to start again

In case you need to reinstall everything again, you just need to run:

```
docker-compose down --volumes --rmi all
```

And then start again...

#### More info

For more info, please visit:
https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html
