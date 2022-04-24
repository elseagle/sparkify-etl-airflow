from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator, 
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator
)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries


AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME')


default_args = {
    'owner': 'Sogo Dowole',
    'start_date': datetime.now(),
    'end_date': datetime.now(),
    'email_on_retry': False,
    'Catchup': False,
    'retries': 15,
    'retry_delay': timedelta(minutes=2),
    'depends_on_past':False
}

dag = DAG('sparkify-etl-44',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
        )

""" Create DAG Operators """ 

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_conn_id='aws_credentials',
    table='public.staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data/',
    jsonpath="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    table='public.staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    conn_id="redshift",
    dest_tbl='public.songplays',
    primary_key='playid',
    query=SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    conn_id="redshift",
    dest_tbl='public.users',
    mode="APPEND",
    primary_key='userid',
    query=SqlQueries.user_table_insert,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    conn_id="redshift",
    dest_tbl='public.songs',
    mode="TRUNCATE",
    query=SqlQueries.song_table_insert,

)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    conn_id="redshift",
    dest_tbl='public.artists',
    mode="TRUNCATE",
    query=SqlQueries.artist_table_insert,

)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    conn_id="redshift",
    dest_tbl='public.time',
    mode="TRUNCATE",
    query=SqlQueries.time_table_insert,

)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id="redshift",
    fmt=["public.songplays", 
            "public.songs",
            "public.users",
            "public.artists",
            "public.time"],
    query="SELECT count(*) FROM {}",
    failure_value=0
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


""" Create the DAG """

start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator

