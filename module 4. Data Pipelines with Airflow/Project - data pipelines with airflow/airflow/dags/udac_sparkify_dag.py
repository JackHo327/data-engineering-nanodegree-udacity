from datetime import datetime, timedelta
import logging
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers import SqlQueries

default_args = {
    'owner': 'airflow',
    # 'start_date': datetime.now(),
    'start_date': datetime(2019, 1, 12),
}

dag = DAG('udac_sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval="@daily",
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=Variable.get('s3_bucket'),
    s3_key=Variable.get('s3_key_log_data'),
    log_json_path=Variable.get('s3_key_log_data_json_path'),
    depends_on_past=False,
    retries=3,
    retry_delay=timedelta(minutes=5),
    email_on_retry=False
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=Variable.get('s3_bucket'),
    s3_key=Variable.get('s3_key_song_data'),
    log_json_path='auto',
    depends_on_past=False,
    retries=3,
    retry_delay=timedelta(minutes=5),
    email_on_retry=False
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="songplays",
    redshift_conn_id='redshift',
    depends_on_past=False,
    retries=3,
    retry_delay=timedelta(minutes=5),
    email_on_retry=False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    redshift_conn_id='redshift',
    depends_on_past=False,
    retries=3,
    retry_delay=timedelta(minutes=5),
    email_on_retry=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    redshift_conn_id='redshift',
    depends_on_past=False,
    retries=3,
    retry_delay=timedelta(minutes=5),
    email_on_retry=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    redshift_conn_id='redshift',
    depends_on_past=False,
    retries=3,
    retry_delay=timedelta(minutes=5),
    email_on_retry=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",
    redshift_conn_id='redshift',
    depends_on_past=False,
    retries=3,
    retry_delay=timedelta(minutes=5),
    email_on_retry=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    table='songplays',
    fields=['user_id', 'song_id', 'artist_id', 'start_time'],
    redshift_conn_id='redshift',
    depends_on_past=False,
    retries=3,
    retry_delay=timedelta(minutes=5),
    email_on_retry=False
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
