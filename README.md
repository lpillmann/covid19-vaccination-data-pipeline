# covid19-vaccination-data-pipeline
Main repository for Udacity Data Engineering Nanodegree Capstone Project - COVID-19 Vaccination in Brazil

## Python scripts setup
Create virtual environment and install dependencies
```bash
python3 -m venv venv
source venv/bin/activate
# main dependencies
pip install -r requirements.txt
# development dependencies
pip install -r requirements-dev.txt
deactivate
```

## Airflow setup
Airflow was configured following [this reference](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#running-airflow).

Start Airflow services:
```bash
docker-compose up
```

Open UI at [http://localhost:8080/home](http://localhost:8080/home) using login credentials as user `airflow` and password `airflow`.

Setup a new connection with **Conn Id** as `redshift` and **Conn Type** as `Postgres` with the credentials of your cluster.

> Note: this Airflow setup is for development purposes. For production deployment some additional configurations would be needed (see [this reference](https://airflow.apache.org/docs/apache-airflow/stable/production-deployment.html)).

## Ad-hoc scripts
### Extract population data
Small CSV is extracted from a public GitHub repository and loaded into the project's S3 bucket.
```
bash scripts/extract/population/run.sh
```

Population estimates by city (2020 census).

> Source: [IBGE](https://www.ibge.gov.br/), data treated and shared by Álvaro Justen/[Brasil.IO](https://brasil.io/)
