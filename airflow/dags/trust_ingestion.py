import ast
from datetime import timedelta

import airflow
import yaml
from airflow.models import DAG
from airflow.models import Variable
import pandas as pd
from lib.utils import docker_task, dummy_task
from datetime import timedelta, datetime

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

sdate = datetime.strptime(date_range['date_start'], "%Y-%m-%d")
edate = datetime.strptime(date_range['date_end'], "%Y-%m-%d")

dates = pd.date_range(sdate, edate, freq='MS').strftime("%Y-%m").tolist()

start = dummy_task("start", dag=dag)
end = dummy_task("end", dag=dag)

tasks = []
for job_date in dates:
    load_to_processed = f"{spark_submit} /dataprocessing/job/trust_ingestion.py -d {job_date}"

    tasks.append(docker_task(f"trust_ingestion-{job_date}", command=load_to_processed, dag=dag))

start.set_downstream(tasks[0])

for j in range(0, len(tasks) - 1):
    tasks[j].set_downstream(tasks[j + 1])

tasks[len(tasks) - 1].set_downstream(end)
