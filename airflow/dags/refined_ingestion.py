import airflow
from airflow.models import DAG
from airflow.models import Variable
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
import yaml
import ast

from lib.utils import docker_task

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depend_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'pool': 'prepare-data-to-neo4j'
}

config = yaml.load(open('./dags/config/data.yml'), Loader=yaml.FullLoader)
date_range = ast.literal_eval(Variable.get("date_range"))

sdate = datetime.strptime(date_range['date_start'], "%Y-%m-%d")
edate = datetime.strptime(date_range['date_end'], "%Y-%m-%d")

delta = edate - sdate

"""Build DAG."""
dag = DAG('prepare-data-to-neo4j', default_args=DEFAULT_ARGS, schedule_interval=None, catchup=False)
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

spark_load_from_pg = []


for t in config['etl_queries']:
    query = config['etl_queries'][t]
    tasks = []

    spark_submit = "/spark/bin/spark-submit --master local[*]"
    spark_submit_params = "--executor-memory 6g --driver-memory 10g --conf spark.network.timeout=600s"

    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        datareferencia = day.strftime("%Y-%m-%d")

        query = query.format(datareferencia=datareferencia)
        task = f" {spark_submit} {spark_submit_params} /data-processing/load_from_prestodb.py -q \"{query}\" -f {t} -d {datareferencia}"

        tasks.append(docker_task(f"spark_etl_{t}_{datareferencia}", task, dag))

    start >> tasks[0]
    for j in range(0, len(tasks) - 1):
        tasks[j] >> tasks[j + 1]
    tasks[len(tasks) - 1] >> end

jobs = [
    {
        'task_name': 'event-stop-edges',
        'task': '/data-processing/event-stop-edges.py'
    },
    {
        'task_name': 'tracking-data',
        'task': '/data-processing/tracking-data.py'
    }
]

for job in jobs:
    tasks = []

    spark_submit = "/spark/bin/spark-submit --master local[*]"
    spark_submit_params = "--executor-memory 6g --driver-memory 10g --conf spark.network.timeout=600s"

    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        datareferencia = day.strftime("%Y-%m-%d")

        task = f"{spark_submit} {spark_submit_params} {job['task']} -d {datareferencia}"
        tasks.append(docker_task(f"spark_etl_{job['task_name']}_data-{datareferencia}", task, dag))

    start >> tasks[0]
    for j in range(0, len(tasks) - 1):
        tasks[j] >> tasks[j + 1]
    tasks[len(tasks) - 1] >> end
