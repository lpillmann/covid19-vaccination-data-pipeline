# covid19-vaccination-data-pipeline
Main repository for Udacity Data Engineering Nanodegree Capstone Project - COVID-19 Vaccination in Brazil

## Overview

### Dimensional model

The model is comprised of one fact table for *vaccinations* and dimensions for *patients*, *facilities*, *vaccines*, *cities*, *cities* and *calendar*.

<details>
  <summary>Expand to all tables with column types </summary>
    
| fact_vaccinations |  |
|---|---|
| vaccination_sk | text |
| patient_sk | text |
| facility_sk | text |
| vaccine_sk | text |
| city_sk | text |
| vaccination_date | timestamptz |
| vaccinations_count | integer |

| dim_patients |  |
|---|---|
| patient_sk | text |
| patient_id | text |
| patient_age | integer |
| patient_birth_date | text |
| patient_biological_gender_enum | text |
| patient_skin_color_code | text |
| patient_skin_color_value | text |
| patient_address_city_ibge_code | text |
| patient_address_city_name | text |
| patient_address_state_abbrev | text |
| patient_address_country_code | text |
| patient_address_country_name | text |
| patient_address_postal_code | text |
| patient_nationality_enum | text |
| vaccination_category_code | text |
| vaccination_category_name | text |
| vaccination_subcategory_code | text |
| vaccination_subcategory_name | text |

| dim_facilities |  |
|---|---|
| facility_sk | text |
| facility_code | text |
| facility_registration_name | text |
| facility_fantasy_name | text |
| facility_city_code | text |
| facility_city_name | text |
| facility_state_abbrev | text |

| dim_vaccines |  |
|---|---|
| vaccine_sk | text |
| vaccination_dose_description | text |
| vaccine_type_code | text |
| vaccine_batch_code | text |
| vaccine_type_name | text |
| vaccine_manufacturer_name | text |
| vaccine_manufacturer_reference_code | text |

| dim_cities |  |
|---|---|
| city_sk | text |
| state | text |
| state_ibge_code | text |
| city_ibge_code | text |
| city | text |
| estimated_population | integer |
| cropped_city_ibge_code | text |

| dim_cities |  |
|---|---|
| city_sk | text |
| state | text |
| state_ibge_code | text |
| city_ibge_code | text |
| city | text |
| estimated_population | integer |
| cropped_city_ibge_code | text |

| dim_calendar |  |
|---|---|
| full_date | timestamptz |
| day | integer |
| week | integer |
| month | integer |
| year | integer |
| weekday | integer |

</details>

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
