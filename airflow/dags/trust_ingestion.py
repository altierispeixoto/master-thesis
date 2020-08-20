import ast
from datetime import timedelta

import airflow
import yaml
from airflow.models import DAG
from airflow.models import Variable

from lib.utils import docker_task, dummy_task

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

dag = DAG(dag_id='trust-ingestion', default_args=args, schedule_interval=None, catchup=False)

spark_submit = "/spark/bin/spark-submit --master local[*] --executor-memory 7g --driver-memory 8g --conf " \
               "spark.network.timeout=600s "

load_to_processed = f"{spark_submit} /dataprocessing/job/trust_ingestion.py "

dummy_task("start", dag=dag) >> docker_task(f"trust_ingestion", command=load_to_processed, dag=dag) >> dummy_task("end", dag=dag)
