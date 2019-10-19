from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    'description': 'Use of the DockerOperator',
    'depend_on_past': False,
    'start_date': datetime(2018, 1, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('docker_dag_spark', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    start = DummyOperator(task_id='start', dag=dag)

    t_docker = DockerOperator(
        task_id='docker_command',
        image='altr/spark:latest',
        container_name='spark-master',
        api_version='auto',
        auto_remove=True,
        environment={
            'PYSPARK_PYTHON': "python3",
            'SPARK_HOME': "/spark"
        },
        volumes=['/home/altieris/master-thesis/airflow/simple-app:/simple-app'
            , '/home/altieris/master-thesis/airflow/data:/data'],
        command='/spark/bin/spark-submit --master local[*] /simple-app/SimpleApp.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )

    start >> t_docker
