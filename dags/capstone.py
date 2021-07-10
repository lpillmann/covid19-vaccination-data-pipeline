
from datetime import timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# Custom operators
from operators import (
    CopyCsvToRedshiftOperator, 
    DropCreateTableOperator, 
    DataQualityOperator
)


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'lui',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    'capstone',
    default_args=default_args,
    description='The DAG for the Capstone Project',
    schedule_interval=None,#timedelta(days=1),
    start_date=days_ago(0),
    tags=['udacity'],
) as dag:

    # Macro steps
    begin_execution = DummyOperator(task_id='begin_execution',  dag=dag)
    load_raw_completed = DummyOperator(task_id='load_raw_completed',  dag=dag)
    drop_create_completed = DummyOperator(task_id='drop_create_completed',  dag=dag)
    data_quality_checks_completed = DummyOperator(task_id='data_quality_checks_completed',  dag=dag)
    execution_completed = DummyOperator(task_id='execution_completed',  dag=dag)
    
    copy_population_to_redshift = CopyCsvToRedshiftOperator(
        task_id='copy_population_data',
        dag=dag,
        conn_id='redshift',
        table_name='raw_population',
        s3_from_path='s3://udacity-capstone-project-opendatasus/raw/population',
        iam_role='arn:aws:iam::301426828416:role/dwhRole',
        region='us-west-2',
        compression=None,
    )

    copy_vaccinations_to_redshift = CopyCsvToRedshiftOperator(
        task_id='copy_vaccinations_data',
        dag=dag,
        conn_id='redshift',
        table_name='raw_vaccinations',
        s3_from_path='s3://udacity-capstone-project-opendatasus/raw/vaccinations',
        iam_role='arn:aws:iam::301426828416:role/dwhRole',
        region='us-west-2',
        compression='gzip',
    )

    drop_create_staging_vaccinations = DropCreateTableOperator(
        task_id='staging_vaccinations',
        dag=dag,
        conn_id='redshift',
        sql_filepath='plugins/sql/staging/staging_vaccinations.sql'
    )

    drop_create_dim_patients = DropCreateTableOperator(
        task_id='dim_patients',
        dag=dag,
        conn_id='redshift',
        sql_filepath='plugins/sql/dimensional/dim_patients.sql'
    )

    drop_create_dim_facilities = DropCreateTableOperator(
        task_id='dim_facilities',
        dag=dag,
        conn_id='redshift',
        sql_filepath='plugins/sql/dimensional/dim_facilities.sql'
    )

    drop_create_dim_vaccines = DropCreateTableOperator(
        task_id='dim_vaccines',
        dag=dag,
        conn_id='redshift',
        sql_filepath='plugins/sql/dimensional/dim_vaccines.sql'
    )

    drop_create_dim_cities = DropCreateTableOperator(
        task_id='dim_cities',
        dag=dag,
        conn_id='redshift',
        sql_filepath='plugins/sql/dimensional/dim_cities.sql'
    )

    drop_create_fact_vaccinations = DropCreateTableOperator(
        task_id='fact_vaccinations',
        dag=dag,
        conn_id='redshift',
        sql_filepath='plugins/sql/dimensional/fact_vaccinations.sql'
    )

    drop_create_dim_calendar = DropCreateTableOperator(
        task_id='dim_calendar',
        dag=dag,
        conn_id='redshift',
        sql_filepath='plugins/sql/dimensional/dim_calendar.sql'
    )

    assert_fact_vaccinations_unique_vaccination_sk = DataQualityOperator(
        task_id='assert_fact_vaccinations_unique_vaccination_sk',
        dag=dag,
        conn_id='redshift',
        sql_query="""
            select count(*) from (
                select
                    vaccination_sk,
                    count(*)
                from
                    fact_vaccinations
                group by 
                    1
                having
                    count(*) > 1
            )
        """,
        expected_result=0,
    )

    assert_dim_patients_unique_patient_sk = DataQualityOperator(
        task_id='assert_dim_patients_unique_patient_sk',
        dag=dag,
        conn_id='redshift',
        sql_query="""
            select count(*) from (
                select
                    patient_sk,
                    count(*)
                from
                    dim_patients
                group by 
                    1
                having
                    count(*) > 1
            )
        """,
        expected_result=0,
    )
    
    # Dependencies
    (
        begin_execution 
        >> [copy_population_to_redshift, copy_vaccinations_to_redshift]
        >> load_raw_completed
        >> drop_create_staging_vaccinations
        >> [
            drop_create_dim_patients,
            drop_create_dim_facilities,
            drop_create_dim_vaccines,
            drop_create_dim_cities,
        ]
        >> drop_create_fact_vaccinations
        >> drop_create_dim_calendar
        >> drop_create_completed
        >> [
            assert_fact_vaccinations_unique_vaccination_sk,
            assert_dim_patients_unique_patient_sk,
        ]
        >> data_quality_checks_completed
        >> execution_completed
    )