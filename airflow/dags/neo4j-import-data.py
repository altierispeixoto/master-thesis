import ast
import airflow
import yaml
from airflow.models import DAG
from airflow.models import Variable
import pandas as pd
from lib.utils import docker_task, dummy_task, load_into_neo4j, create_task
from datetime import timedelta, datetime
from airflow.operators.bash_operator import BashOperator

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

dag = DAG(dag_id='neo4j-data-ingestion', default_args=args, schedule_interval=None, catchup=False)

spark_submit = "/spark/bin/spark-submit --master local[*] --executor-memory 7g --driver-memory 8g --conf " \
               "spark.network.timeout=600s "

sdate = datetime.strptime(date_range['date_start'], "%Y-%m-%d")
edate = datetime.strptime(date_range['date_end'], "%Y-%m-%d")

dates = pd.date_range(sdate, edate, freq='MS').strftime("%Y-%m").tolist()

start = dummy_task("start", dag=dag)
end = dummy_task("end", dag=dag)

delta = edate - sdate


def move_to_neo4j_folder(datareferencia, dag):
    cmd = f"scripts/neo4j-data-import.sh"
    task = BashOperator(
        task_id=f"move-file-{datareferencia}",
        bash_command=cmd,
        params={"datareferencia": datareferencia},
        dag=dag,
    )
    return task


def load_data_to_neo4j(datareferencia, dag):
    load_into_neo4j_tasks = []
    for neo in config['neo4j_import']:
        task = create_task(f"load-into-neo4j-{neo}-{datareferencia}",
                           op_kwargs={'cypher_query': config['neo4j_import'][neo]['cypher_query'],
                                      'datareferencia': datareferencia}, python_callable=load_into_neo4j, _dag=dag)
        load_into_neo4j_tasks.append(task)
    return load_into_neo4j_tasks


docker_tasks = []
move_tasks = []
load_into_neo4j_tasks = []
prepare_load_tasks = []
end_tasks = []

for i in range(delta.days + 1):
    day = sdate + timedelta(days=i)
    job_date = day.strftime("%Y-%m-%d")

    load_to_processed = f"{spark_submit} /dataprocessing/job/neo4j_ingestion.py -d {job_date}"
    docker_tasks.append(docker_task(f"neo4j_ingestion-{job_date}", command=load_to_processed, dag=dag))

    prepare_load_task = dummy_task(f"prepare_load-{job_date}", dag)
    prepare_load_tasks.append(prepare_load_task)

    end_task = dummy_task(f"end_task-{job_date}", dag)
    end_tasks.append(end_task)

    for t in load_data_to_neo4j(job_date, dag):
        load_into_neo4j_tasks.append(prepare_load_task >> t >> end_task)

    move_tasks.append(move_to_neo4j_folder(job_date, dag))

start.set_downstream(docker_tasks[0])

for j in range(0, len(docker_tasks) - 1):
    docker_tasks[j].set_downstream(move_tasks[j])
    move_tasks[j].set_downstream(prepare_load_tasks[j])

    end_tasks[j].set_downstream(docker_tasks[j + 1])

docker_tasks[len(docker_tasks) - 1].set_downstream(move_tasks[len(move_tasks) - 1])

move_tasks[len(docker_tasks) - 1].set_downstream(prepare_load_tasks[len(move_tasks) - 1])

end_tasks[len(end_tasks) - 1].set_downstream(end)
