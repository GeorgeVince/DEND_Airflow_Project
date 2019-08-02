from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

BUCKET = 'udacity-dend'
SONG_KEY = 'song_data/'
EVENT_KEY = 'log_data/'


default_args = {
    'owner': 'GeorgeVince',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
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

#####STAGING#####
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    s3_bucket=BUCKET,
    s3_key = EVENT_KEY,
    aws_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    table="staging_events",
    copy_query = SqlQueries.s3_copy,
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
    json_params = "'auto'"
)

#####LOAD FACT TABLE#####
load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id= "redshift",
    dst_table = 'songplays',
    truncate=False,
    sql_insert=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    dst_table="users",
    sql_insert=SqlQueries.user_table_insert,
    truncate=True,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dst_table="songs",
    sql_insert=SqlQueries.song_table_insert,
    truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dst_table="artists",
    sql_insert=SqlQueries.artist_table_insert,
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dst_table="time",
    sql_insert=SqlQueries.time_table_insert,
    truncate=True
)

#####DATA QUALITY CHECKS#####
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables_to_check=['users', 'songs', 'artists','time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#Create tables and stage them to redshift
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

#Load the fact table
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

#Create dimension tables
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

#Run data quality checks
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator