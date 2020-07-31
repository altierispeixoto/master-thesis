import ast
from datetime import timedelta

import airflow
import yaml
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.dummy_operator import DummyOperator

config = yaml.load(open('./dags/config/data.yml'), Loader=yaml.FullLoader)
date_range = ast.literal_eval(Variable.get("date_range"))

args = {
    'owner': 'airflow',
    'description': 'Use of the DockerOperator',
    'depend_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'pool': 'trust-processing',
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='trust-processing', default_args=args, schedule_interval=None, catchup=False)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

spark_submit = "/spark/bin/spark-submit --master local[*] --executor-memory 7g --driver-memory 8g --conf " \
               "spark.network.timeout=600s "

load_to_processed = f"{spark_submit} /spark-urbs-processing/trust-processing.py "


task = DockerOperator(
    task_id=f"trust_ingestion",
    image='bde2020/spark-master:2.4.4-hadoop2.7',
    api_version='auto',
    auto_remove=True,
    environment={
        'PYSPARK_PYTHON': "python3",
        'SPARK_HOME': "/spark"
    },
    volumes=['/work/master-thesis/airflow/spark-urbs-processing:/spark-urbs-processing',
             '/work/datalake:/data'],
    command=load_to_processed,
    docker_url='unix://var/run/docker.sock',
    network_mode='host', dag=dag
)

start >> task >> end
