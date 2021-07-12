# covid19-vaccination-data-pipeline
Main repository for Udacity Data Engineering Nanodegree Capstone Project - COVID-19 Vaccination in Brazil

## Setup
### Infrastructure
1. Install local Python env
    ```bash
    make install-infra
    ```

1. Define the following enviroment variables
    ```bash
    # Tip: add this snippet to your bash profile (e.g. ~/.bashrc or ~/.zshrc if you use ZSH)
    export UDACITY_AWS_KEY="..."
    export UDACITY_AWS_SECRET="..."
    export UDACITY_AWS_REGION="..."
    export UDACITY_AWS_PROFILE="..."
    export UDACITY_CAPSTONE_PROJECT_BUCKET="my-bucket"
    export UDACITY_REDSHIFT_HOST="my-host.something-else.redshift.amazonaws.com"
    export UDACITY_REDSHIFT_DB_NAME="..."
    export UDACITY_REDSHIFT_DB_USER="..."
    export UDACITY_REDSHIFT_DB_PASSWORD="..."
    export UDACITY_REDSHIFT_DB_PORT="..."
    export AIRFLOW_UID=1000
    export AIRFLOW_GID=0
    ```

1. Create Redshift cluster
    ```bash
    make create-cluster
    ```

1. Setup Airflow (see respective session)

>When finished using the cluster, remember to delete it to avoid unexpected costs:
>
>Delete Redshift Cluster
>```bash
>make delete-cluster
>```


## Airflow setup
Airflow was configured using Docker following [this reference](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#running-airflow).

Build customized image:
```bash
make image
```

Start Airflow services:
```bash
make airflow
```

Open UI at [http://localhost:8080/home](http://localhost:8080/home) using login credentials as user `airflow` and password `airflow`.

> Note 1: this Airflow setup is for development purposes. For production deployment some additional configurations would be needed (see [this reference](https://airflow.apache.org/docs/apache-airflow/stable/production-deployment.html)).

> Note 2: some Airflow environment variables were customized in `docker-compose.yaml`, namely:
>```bash
>AIRFLOW__CORE__LOAD_EXAMPLES: 'false'  # don't load dag examples
>AIRFLOW__WEBSERVER__RELOAD_ON_PLUGIN_CHANGE: 'true'  # reload plugins as soon as they are saved
>AIRFLOW_CONN_REDSHIFT: postgres://...  # custom connection URI using the env vars instead of using UI
>AWS_ACCESS_KEY_ID: ${UDACITY_AWS_KEY}
>AWS_SECRET_ACCESS_KEY: ${UDACITY_AWS_SECRET}
>AWS_DEFAULT_REGION: ${UDACITY_AWS_REGION}
>UDACITY_AWS_PROFILE: ${UDACITY_AWS_PROFILE}
>UDACITY_CAPSTONE_PROJECT_BUCKET: ${UDACITY_CAPSTONE_PROJECT_BUCKET}
>```

## Ad-hoc scripts
### Extract population data
Small CSV is extracted from a public GitHub repository and loaded into the project's S3 bucket.
```
bash dags/scripts/extract/population/run.sh
```

Population estimates by city (2020 census).

> Source: [IBGE](https://www.ibge.gov.br/), data treated and shared by Álvaro Justen/[Brasil.IO](https://brasil.io/)
