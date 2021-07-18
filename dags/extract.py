import calendar
from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


SCRIPTS_BASE_PATH = "/opt/airflow/dags/scripts"
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
CURRENT_YEAR_MONTH = datetime.now().strftime("%Y-%m-01")
STATE_ABBREVIATIONS = ["AC", "MA", "PE", "PR", "RS", "SC"]


def is_month_end_date() -> bool:
    """Returns True if current date is the last day of the month"""
    year, month, _ = CURRENT_YEAR_MONTH.split("-")
    _, last_day = calendar.monthrange(int(year), int(month))
    return f"{year}-{month}-{last_day}" == CURRENT_DATE


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
}
with DAG(
    "extract",
    default_args=default_args,
    description="The DAG for extracting data using taps and targets",
    schedule_interval="0 0 * * *",
    max_active_runs=1,
    start_date=days_ago(2),  # Always runs from previous two days to make sure
    tags=["udacity"],
) as dag:

    # Macro steps
    begin_execution = DummyOperator(task_id="begin_execution", dag=dag)
    
    extraction_completed = DummyOperator(task_id="extraction_completed", dag=dag)
    execution_completed = DummyOperator(task_id="execution_completed", dag=dag)

    # Extract data from different sources to S3

    # Open Data SUS (vaccinations) - paralelize extraction by Brazilian states
    extraction_tasks = []
    for state_abbrev in STATE_ABBREVIATIONS:
        script_path = f"{SCRIPTS_BASE_PATH}/extract/vaccinations/run.sh"
        load_mode = 'replace' if is_month_end_date() else ''  # Do a full reload on the last day of the month
        extract_opendatasus = BashOperator(
            task_id=f"extract_opendatasus_{state_abbrev}",
            dag=dag,
            bash_command=f"{script_path} {{{{ ds }}}} {state_abbrev} {load_mode} ",
        )
        extraction_tasks.append(extract_opendatasus)

    # Note: Static source - no need to extract every day
    # Population data from curated source (CSV hosted in GitHub)
    # extract_population = BashOperator(
    #     task_id=f"extract_population",
    #     dag=dag,
    #     bash_command=f"{SCRIPTS_BASE_PATH}/extract/population/run.sh replace ",
    # )
    # extraction_tasks.append(extract_population)

    # Dependencies
    (
        begin_execution
        >> extraction_tasks
        >> extraction_completed
        >> execution_completed
    )
