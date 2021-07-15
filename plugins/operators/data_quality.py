from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class DataQualityOperator(BaseOperator):
    """
    Data quality check expects a query that would return the amount of rows matching a test.
    That quantity must match the passed expected result for the test to pass.
    """

    ui_color = "#89DA59"

    def __init__(self, conn_id, sql_query, expected_result, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.sql_query = sql_query
        self.expected_result = expected_result

    def execute(self, context):
        self.log.info(f"DataQualityOperator: Starting")
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)
        result = redshift_hook.get_records(self.sql_query)[0][0]
        assert (
            result == self.expected_result
        ), f"ERROR: Expected {self.expected_result} rows but got {result}"
        self.log.info("DataQualityOperator: Done")
