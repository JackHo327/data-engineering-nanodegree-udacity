from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
                
        create_table_stmt = getattr(SqlQueries, f'{self.table}_table_create')
        redshift.run(create_table_stmt, autocommit=True)
        
        insert_table_stmt = getattr(SqlQueries, f'{self.table}_table_insert')
        redshift.run(insert_table_stmt, autocommit=True)
        
        self.log.info(f'LoadFactOperator for [{self.table}] has been done')
