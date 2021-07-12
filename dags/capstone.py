from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Custom operators
from operators import (
    CopyCsvToRedshiftOperator,
    CopyCsvToRedshiftPartionedOperator,
    RedshiftQueryOperator,
    DataQualityOperator,
)

CURRENT_YEAR_MONTH = datetime.now().strftime(
    "%Y-%m-01"
)  # TODO: pass it using Airflow context

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "lui",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
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
    "capstone",
    default_args=default_args,
    description="The DAG for the Capstone Project",
    schedule_interval=None,  # timedelta(days=1),
    start_date=days_ago(0),
    tags=["udacity"],
) as dag:

    # Macro steps
    begin_execution = DummyOperator(task_id="begin_execution", dag=dag)
    create_raw_tables_completed = DummyOperator(
        task_id="create_raw_tables_completed", dag=dag
    )
    load_raw_completed = DummyOperator(task_id="load_raw_completed", dag=dag)
    rebuild_completed = DummyOperator(task_id="rebuild_completed", dag=dag)
    data_quality_checks_completed = DummyOperator(
        task_id="data_quality_checks_completed", dag=dag
    )
    execution_completed = DummyOperator(task_id="execution_completed", dag=dag)

    # Create raw tables to receive data from S3
    create_raw_population = RedshiftQueryOperator(
        task_id="raw_population",
        dag=dag,
        conn_id="redshift",
        sql_filepath="plugins/sql/raw/raw_population.sql",
    )

    create_raw_vaccinations = RedshiftQueryOperator(
        task_id="raw_vaccinations",
        dag=dag,
        conn_id="redshift",
        sql_filepath="plugins/sql/raw/raw_vaccinations.sql",
    )

    create_raw_vaccinations_tmp = RedshiftQueryOperator(
        task_id="raw_vaccinations_tmp",
        dag=dag,
        conn_id="redshift",
        sql_filepath="plugins/sql/raw/raw_vaccinations_tmp.sql",
    )

    # Load raw data from S3
    copy_population_to_redshift = CopyCsvToRedshiftOperator(
        task_id="copy_population_data",
        dag=dag,
        conn_id="redshift",
        table_name="raw_population",
        s3_from_path="s3://udacity-capstone-project-opendatasus/raw/population",
        iam_role="arn:aws:iam::301426828416:role/dwhRole",
        region="us-west-2",
        compression=None,
    )

    copy_vaccinations_to_redshift = CopyCsvToRedshiftPartionedOperator(
        task_id="copy_vaccinations_data",
        dag=dag,
        conn_id="redshift",
        table_name="raw_vaccinations",
        s3_from_path="s3://udacity-capstone-project-opendatasus/raw/vaccinations",
        iam_role="arn:aws:iam::301426828416:role/dwhRole",
        region="us-west-2",
        compression="gzip",
        temporary_table_name="raw_vaccinations_tmp",
        primary_key="document_id",
        partition_column_name="year_month",
        partition_column_value=CURRENT_YEAR_MONTH,
    )

    # Transform into dimensional model
    rebuild_staging_vaccinations = RedshiftQueryOperator(
        task_id="staging_vaccinations",
        dag=dag,
        conn_id="redshift",
        sql_filepath="plugins/sql/staging/staging_vaccinations.sql",
    )

    rebuild_dim_patients = RedshiftQueryOperator(
        task_id="dim_patients",
        dag=dag,
        conn_id="redshift",
        sql_filepath="plugins/sql/dimensional/dim_patients.sql",
    )

    rebuild_dim_facilities = RedshiftQueryOperator(
        task_id="dim_facilities",
        dag=dag,
        conn_id="redshift",
        sql_filepath="plugins/sql/dimensional/dim_facilities.sql",
    )

    rebuild_dim_vaccines = RedshiftQueryOperator(
        task_id="dim_vaccines",
        dag=dag,
        conn_id="redshift",
        sql_filepath="plugins/sql/dimensional/dim_vaccines.sql",
    )

    rebuild_dim_cities = RedshiftQueryOperator(
        task_id="dim_cities",
        dag=dag,
        conn_id="redshift",
        sql_filepath="plugins/sql/dimensional/dim_cities.sql",
    )

    rebuild_fact_vaccinations = RedshiftQueryOperator(
        task_id="fact_vaccinations",
        dag=dag,
        conn_id="redshift",
        sql_filepath="plugins/sql/dimensional/fact_vaccinations.sql",
    )

    rebuild_dim_calendar = RedshiftQueryOperator(
        task_id="dim_calendar",
        dag=dag,
        conn_id="redshift",
        sql_filepath="plugins/sql/dimensional/dim_calendar.sql",
    )

    # Perform data quality checks
    assert_fact_vaccinations_unique_vaccination_sk = DataQualityOperator(
        task_id="assert_fact_vaccinations_unique_vaccination_sk",
        dag=dag,
        conn_id="redshift",
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
        task_id="assert_dim_patients_unique_patient_sk",
        dag=dag,
        conn_id="redshift",
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
        >> [
            create_raw_population,
            create_raw_vaccinations,
            create_raw_vaccinations_tmp,
        ]
        >> create_raw_tables_completed
        >> [copy_population_to_redshift, copy_vaccinations_to_redshift]
        >> load_raw_completed
        >> rebuild_staging_vaccinations
        >> [
            rebuild_dim_patients,
            rebuild_dim_facilities,
            rebuild_dim_vaccines,
            rebuild_dim_cities,
        ]
        >> rebuild_fact_vaccinations
        >> rebuild_dim_calendar
        >> rebuild_completed
        >> [
            assert_fact_vaccinations_unique_vaccination_sk,
            assert_dim_patients_unique_patient_sk,
        ]
        >> data_quality_checks_completed
        >> execution_completed
    )
