# https://github.com/blockchain-etl/bitcoin-etl-airflow-neo4j/blob/master/dags/dag_btc_to_neo4j.py
from airflow.models import DAG
import airflow
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from datetime import timedelta, datetime
import ast
import yaml

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
    'retry_delay': timedelta(minutes=5),
    'pool': 'move-to-neo4j-folder'
}

dag = DAG(dag_id='move-to-neo4j-folder', default_args=args, schedule_interval=None, catchup=False)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

sdate = datetime.strptime(date_range['date_start'], "%Y-%m-%d")
edate = datetime.strptime(date_range['date_end'], "%Y-%m-%d")

delta = edate - sdate


def move_to_neo4j_folder(file, datareferencia, dag):
    root_path = "/usr/local/airflow"
    cmd = f"mkdir -p {root_path}/neo4j/import/{file}/{datareferencia} &&  cp {root_path}/data/neo4j/{file}/{datareferencia}/*.csv {root_path}/neo4j/import/{file}/{datareferencia}/{file}.csv"

    task = BashOperator(
        task_id=f"move-file-{file}-{datareferencia}",
        bash_command=cmd,
        dag=dag,
    )
    return task


for t in config['etl_queries']:
    tasks = []
    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        datareferencia = day.strftime("%Y-%m-%d")

        tasks.append(move_to_neo4j_folder(t, datareferencia, dag))

    start >> tasks[0]
    for j in range(0, len(tasks) - 1):
        tasks[j] >> tasks[j + 1]
    tasks[len(tasks) - 1] >> end

for t in ['trackingdata', 'event-stop-edges']:
    tasks = []
    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        datareferencia = day.strftime("%Y-%m-%d")

        tasks.append(move_to_neo4j_folder(t, datareferencia, dag))

    start >> tasks[0]
    for j in range(0, len(tasks) - 1):
        tasks[j] >> tasks[j + 1]
    tasks[len(tasks) - 1] >> end
