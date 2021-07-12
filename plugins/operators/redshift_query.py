from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class RedshiftQueryOperator(BaseOperator):

    ui_color = "#8fb3ff"

    def __init__(self, conn_id, sql_filepath, **kwargs):

        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.sql_filepath = sql_filepath

    @property
    def drop_create_query(self):
        with open(self.sql_filepath, "r") as f:
            contents = f.read()
        return "\n" + contents

    def execute(self, context):
        self.log.info(
            f"RedshiftQueryOperator: Starting for SQL filepath {self.sql_filepath}"
        )
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)
        redshift_hook.run(self.drop_create_query)
        self.log.info("RedshiftQueryOperator: Done")
