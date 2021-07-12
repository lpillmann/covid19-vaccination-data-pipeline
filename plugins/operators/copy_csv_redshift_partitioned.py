from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator

class CopyCsvToRedshiftPartionedOperator(BaseOperator):
    """
    Load single partition for S3 and replace contents in target table

    Expects S3 key to have partition column name, e.g.:
        s3://my-bucket/my-data-path/year_month=2021-01-01/*.csv

        where `year_month` is the partition column name and `2021-01-01` is the partition column value
    """
    
    ui_color = '#fcc379'

    def __init__(
            self,
            conn_id,
            table_name,
            s3_from_path,
            iam_role,
            region,
            compression,
            temporary_table_name,
            primary_key,
            partition_column_name,
            partition_column_value,
            **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.table_name = table_name
        self.s3_from_path = s3_from_path
        self.iam_role = iam_role
        self.region = region
        self.compression = compression
        self.temporary_table_name = temporary_table_name
        self.primary_key = primary_key
        self.partition_column_name = partition_column_name
        self.partition_column_value = partition_column_value
    
    @property
    def merge_query(self):
        return (
            f"""
            /* Merge operation using tmp table as source and raw table as target */
            
            -- Load temporary table with whole current month of data
            copy {self.temporary_table_name}
            from '{self.s3_from_path + '/' + self.partition_column_name + '=' + self.partition_column_value}'
            iam_role '{self.iam_role}'
            region '{self.region}'
            csv
            quote '"'
            {self.compression or ''}
            ignoreheader 1;

            -- Start a new transaction
            begin transaction;

            -- Delete any rows from target that exist in source, because they are updates
            delete from {self.table_name}
            using {self.temporary_table_name}
            where {self.table_name}.{self.primary_key} = {self.temporary_table_name}.{self.primary_key}
            and {self.table_name}.{self.partition_column_name} = '{self.partition_column_value}';

            -- Insert all the rows from the raw table into the target table
            insert into {self.table_name}
            select * from {self.temporary_table_name};

            -- End transaction and commit
            end transaction;

            -- Empty the temporary table
            truncate table {self.temporary_table_name};
            """
        )

    def execute(self, context):
        self.log.info('CopyCsvToRedshiftPartionedOperator: Starting execution for table {}'.format(self.table_name))
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)
        redshift_hook.run(self.merge_query)
        self.log.info('CopyCsvToRedshiftPartionedOperator: Done')





