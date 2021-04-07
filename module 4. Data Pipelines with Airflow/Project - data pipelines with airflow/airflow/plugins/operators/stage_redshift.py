from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key", )

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 log_json_path="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_json_path = log_json_path
        

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id, resource_type='s3')
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        create_table_stmt = getattr(SqlQueries, f'{self.table}_table_create')
        redshift.run(create_table_stmt, autocommit=True)

        rendered_key = self.s3_key.format(**context)
        s3_data_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        log_json_path = self.log_json_path
        if log_json_path != 'auto':
            log_json_path = "s3://{}/{}".format(self.s3_bucket, self.log_json_path)
        copy_table_stmt = SqlQueries.copy_staging_table.format(self.table,
                                                s3_data_path,
                                                credentials.access_key,
                                                credentials.secret_key,
                                                log_json_path)
        redshift.run(copy_table_stmt, autocommit=True)
        
        self.log.info(f'StageToRedshiftOperator for [{self.table}] has been done')