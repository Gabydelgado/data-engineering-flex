from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from scripts.main import main

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

with DAG(
    dag_id = 'dag_etl',
    start_date = datetime(2023, 12, 4),
    catchup = False,
    schedule_interval = '@daily',
    default_args = default_args
    ) as dag:

    dummy_start = DummyOperator(
        task_id = 'start'
        )

    create_table = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id = 'redshift',
        sql = 'sql/create_table.sql',
        hook_params = {'options': '-c search_path=gaby_delgado11_coderhouse'}
        )
    
    main_task = PythonOperator(
        task_id = 'main_task',
        python_callable = main,
        op_kwargs = {'config': '/opt/airflow/config/config.ini'}
        )

    dummy_end = DummyOperator(
        task_id = 'end'
        )
    
    dummy_start >> create_table >> main_task >> dummy_end
