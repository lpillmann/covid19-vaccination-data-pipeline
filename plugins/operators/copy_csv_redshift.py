from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class CopyCsvToRedshiftOperator(BaseOperator):
    ui_color = "#fcc379"

    def __init__(
        self, conn_id, table_name, s3_from_path, iam_role, region, compression, **kwargs
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.table_name = table_name
        self.s3_from_path = s3_from_path
        self.iam_role = iam_role
        self.region = region
        self.compression = compression

    @property
    def copy_query(self):
        return f"""
            copy {self.table_name}
            from '{self.s3_from_path}'
            iam_role '{self.iam_role}'
            region '{self.region}'
            csv
            quote '"'
            {self.compression or ''}
            ignoreheader 1
            """

    def execute(self, context):
        self.log.info(
            "CopyCsvToRedshiftOperator: Starting execution for table {}".format(
                self.table_name
            )
        )
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)
        rows = redshift_hook.get_records(f"select count(*) from {self.table_name}")[0][
            0
        ]
        # Skip if table is already populated
        if rows > 0:
            self.log.info(
                f"CopyCsvToRedshiftOperator: Skipping copy execution. Table is already populated with {rows} rows."
            )
            return
        redshift_hook.run(self.copy_query)
        self.log.info("CopyCsvToRedshiftOperator: Done")
