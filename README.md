# covid19-vaccination-data-pipeline
Main repository for Udacity Data Engineering Nanodegree Capstone Project - COVID-19 Vaccination in Brazil

## Airflow setup
Airflow was configured following [this reference](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#running-airflow).

Start Airflow services
```bash
docker-compose up
```

Open UI at [http://localhost:8080/home](http://localhost:8080/home) and fill the credentials with user `airflow` and password `airflow`.

Setup a new connection with `Conn Id` as `redshift` and `Conn Type` as `Postgres` with the credentials of your cluster.

Note: this Airflow setup is for development purposes. For production deployment some additional configurations would be needed (see [this reference](https://airflow.apache.org/docs/apache-airflow/stable/production-deployment.html)).

## Ad-hoc scripts
### Extract population data
Small CSV is extracted from a public GitHub repository.
```
bash scripts/extract/population/run.sh
```

Population estimates by city (2020 census).

> Source: [IBGE](https://www.ibge.gov.br/), data treated and shared by Álvaro Justen/[Brasil.IO](https://brasil.io/)
