from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 fields=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.fields = fields

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for field in self.fields:
            num_rows_query = f"""SELECT COUNT(*) FROM {self.table} WHERE {field} IS NULL;"""
            records = redshift.get_records(num_rows_query)
            num_records = records[0][0]
            
            if num_records != 0:
                raise ValueError(f"Data quality check failed. {self.table}.{field} contained NULL values")
                
            self.log.info(f"Data quality on {self.table}.{field} check passed with {num_records} NULL record")
        