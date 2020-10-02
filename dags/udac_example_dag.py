from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from load_dimension_table_dag import get_load_dimension_table_dag

from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
    PostgresOperator
)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Sparkify',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# create_tables = PostgresOperator(
#     task_id="create_tables",
#     dag=dag,
#     postgres_conn_id="redshift",
#     sql="create_tables.sql"
# )

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data/{execution_date.year}/{execution_date.month}',
    json_format='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    json_format='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    select_sql=SqlQueries.songplay_table_insert
)

# # Attempt at SubDAGs
# load_user_dimension_table = LoadDimensionOperator(
#     task_id='Load_user_dim_table',
#     dag=dag,
#     redshift_conn_id='redshift',
#     table='users',
#     append_insert=True,
#     select_sql=SqlQueries.user_table_insert
# )

# load_song_dimension_table = LoadDimensionOperator(
#     task_id='Load_song_dim_table',
#     dag=dag,
#     redshift_conn_id='redshift',
#     table='songs',
#     append_insert=True,
#     select_sql=SqlQueries.song_table_insert
# )

# load_artist_dimension_table = LoadDimensionOperator(
#     task_id='Load_artist_dim_table',
#     dag=dag,
#     redshift_conn_id='redshift',
#     table='artists',
#     append_insert=True,
#     select_sql=SqlQueries.artist_table_insert
# )

# load_time_dimension_table = LoadDimensionOperator(
#     task_id='Load_time_dim_table',
#     dag=dag,
#     redshift_conn_id='redshift',
#     table='time',
#     append_insert=True,
#     select_sql=SqlQueries.time_table_insert
# )
# 

dimension_task_id = "Load_dimension_table_subdag"
load_dimension_table = SubDagOperator(
    subdag=get_load_dimension_table_dag(
        parent_dag_name='udac_example_dag',
        task_id=dimension_task_id,
        default_args=default_args,
        redshift_conn_id='redshift',
        tables=['users', 'songs', 'artists', 'time'],
        append_insert=[True,True,True,True],
        select_queries=[SqlQueries.user_table_insert, SqlQueries.song_table_insert, SqlQueries.artist_table_insert, SqlQueries.time_table_insert],
    ),
    task_id=dimension_task_id,
    dag=dag,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables = ['staging_events', 'staging_songs', 'songplays', 'users', 'songs', 'artists', 'time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# start_operator >> create_tables
# create_tables >> stage_events_to_redshift >> load_songplays_table
# create_tables >> stage_songs_to_redshift >> load_songplays_table
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
# load_songplays_table >> load_user_dimension_table >> run_quality_checks
# load_songplays_table >> load_song_dimension_table >> run_quality_checks
# load_songplays_table >> load_artist_dimension_table >> run_quality_checks
# load_songplays_table >> load_time_dimension_table >> run_quality_checks
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_dimension_table
load_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
