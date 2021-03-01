from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from include.helpers.astro import download_dataset,read_rmse,check_dataset
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.subdag import SubDagOperator
from airflow.operators.sql import BranchSQLOperator
from plugins.notebook_plugin.operators.notebook_to_keep_operator import NotebookToKeepOperator
from plugins.notebook_plugin.operators.notebook_to_git_operator import NotebookToGitOperator

from include.subdags.training_subdag import subdag_factory
from include.groups.training_group import training_groups
from datetime import datetime, timedelta

default_args = {
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2020, 1, 1)
}

with DAG('avocado_dag', default_args=default_args, 
    description='Forecasting avocado prices', 
    schedule_interval='*/10 * * * *', catchup=False) as dag:

    creating_table = PostgresOperator(
        task_id='creating_table',
        sql='sql/CREATE_TABLE_ACCURACIES.sql',
        postgres_conn_id='postgres'
    )

    downloading_data = PythonOperator(
        task_id='downloading_data',
        python_callable=download_dataset
    )

    sanity_check = PythonOperator(
        task_id="sanity_check",
        python_callable=check_dataset
    )

    waiting_for_data = FileSensor(
        task_id='waiting_for_data',
        fs_conn_id='fs_default',
        filepath='avocado.csv',
        poke_interval=15
    )

    training_model_tasks = training_groups()

    evaluating_rmse = BranchSQLOperator(
        task_id='evaluating_rmse',
        sql='sql/FETCH_MIN_RMSE.sql',
        conn_id='postgres',
        follow_task_ids_if_true='accurate',
        follow_task_ids_if_false='inaccurate'
    )

    accurate = DummyOperator(
        task_id='accurate'
    )

    fetch_best_model = NotebookToKeepOperator(
        task_id="fetch_best_model",
        sql="sql/FETCH_BEST_MODEL.sql",
        postgres_conn_id="postgres"
    )

    publish_notebook = NotebookToGitOperator(
        task_id="publish_notebook",
        conn_id="git",
        nb_path="/tmp",
        nb_name="out-model-avocado-prediction-{{ task_instance.xcom_pull(task_ids='fetch_best_model') }}.ipynb"
    )

    inaccurate = DummyOperator(
        task_id='inaccurate'
    )

    creating_table >> downloading_data >> sanity_check >> waiting_for_data >> training_model_tasks >> evaluating_rmse
    evaluating_rmse >> [ accurate, inaccurate ]
    accurate >> fetch_best_model >> publish_notebook


	