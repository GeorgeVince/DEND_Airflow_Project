from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
BUCKET = 'udacity-dend'
SONG_KEY = 'song_data/A/A/*'
EVENT_KEY = 'log_data/'


default_args = {
    'owner': 'GeorgeVince',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
}

dag = DAG('s3_to_redshift_v2',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval= '@hourly',
          catchup=False,
        )

#Ensure we have the required tables set up in Redshift
start_operator = PostgresOperator(task_id='Begin_execution',
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql")

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    table="staging_events",
    s3_bucket=BUCKET,
    s3_key = EVENT_KEY,

    create_query = SqlQueries.staging_events_table_create,
    json_params = "'s3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    table="staging_songs",
    s3_bucket=BUCKET,
    s3_key = SONG_KEY,
    copy_query = SqlQueries.s3_copy,
    create_query = SqlQueries.staging_songs_table_create,
    json_params = "'auto'"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    create_query=SqlQueries.songplay_table_create,
    sql_insert=SqlQueries.songplay_table_insert,
    table="song_plays",
    dag=dag
)

# load_user_dimension_table = LoadDimensionOperator(
#     task_id='Load_user_dim_table',
#     dag=dag
# )

# load_song_dimension_table = LoadDimensionOperator(
#     task_id='Load_song_dim_table',
#     dag=dag
# )

# load_artist_dimension_table = LoadDimensionOperator(
#     task_id='Load_artist_dim_table',
#     dag=dag
# )

# load_time_dimension_table = LoadDimensionOperator(
#     task_id='Load_time_dim_table',
#     dag=dag
# )

# run_quality_checks = DataQualityOperator(
#     task_id='Run_data_quality_checks',
#     dag=dag
# )

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> end_operator