import ast
from datetime import timedelta, datetime
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
    'pool': 'refined-ingestion',
    'retry_delay': timedelta(minutes=5)
}

sdate = datetime.strptime(date_range['date_start'], "%Y-%m-%d")
edate = datetime.strptime(date_range['date_end'], "%Y-%m-%d")

delta = edate - sdate

dag = DAG(dag_id='test-ingestion', default_args=args, schedule_interval=None, catchup=False)

spark_submit = "/spark/bin/spark-submit --master local[*] --executor-memory 12g --driver-memory 12g --conf " \
               "spark.network.timeout=600s "

line_tasks = []
bus_stop_tasks = []
timetable_tasks = []
tracking_tasks = []

for i in range(delta.days + 1):
    day = sdate + timedelta(days=i)
    job_date = day.strftime("%Y-%m-%d")

    line_job = f"{spark_submit} /dataprocessing/job/refined_ingestion.py -j line -d {job_date}"
    line_tasks.append(docker_task(f"refined-ingestion-line-{job_date}", command=line_job, dag=dag))

    bus_stop_job = f"{spark_submit} /dataprocessing/job/refined_ingestion.py -j bus-stop -d {job_date}"
    bus_stop_tasks.append(docker_task(f"refined-ingestion-bus-stop-{job_date}", command=bus_stop_job, dag=dag))

    timetable_job = f"{spark_submit} /dataprocessing/job/refined_ingestion.py -j timetable -d {job_date}"
    timetable_tasks.append(docker_task(f"refined-ingestion-timetable-{job_date}", command=timetable_job, dag=dag))

    tracking_job = f"{spark_submit} /dataprocessing/job/refined_ingestion.py -j tracking -d {job_date}"
    tracking_tasks.append(docker_task(f"refined-ingestion-tracking-{job_date}", command=tracking_job, dag=dag))


start = dummy_task("start", dag=dag)
end = dummy_task("end", dag=dag)

start.set_downstream(line_tasks[0])
start.set_downstream(bus_stop_tasks[0])
start.set_downstream(timetable_tasks[0])
start.set_downstream(tracking_tasks[0])

for j in range(0, len(line_tasks) - 1):
    line_tasks[j].set_downstream(line_tasks[j + 1])
    bus_stop_tasks[j].set_downstream(bus_stop_tasks[j + 1])
    timetable_tasks[j].set_downstream(timetable_tasks[j + 1])
    tracking_tasks[j].set_downstream(tracking_tasks[j + 1])

line_tasks[len(line_tasks) - 1].set_downstream(end)
bus_stop_tasks[len(bus_stop_tasks) - 1].set_downstream(end)
timetable_tasks[len(timetable_tasks) - 1].set_downstream(end)
tracking_tasks[len(tracking_tasks) - 1].set_downstream(end)
