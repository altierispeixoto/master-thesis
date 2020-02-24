#https://github.com/blockchain-etl/bitcoin-etl-airflow-neo4j/blob/master/dags/dag_btc_to_neo4j.py
import logging
import time
from airflow.models import DAG
import airflow
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from neo4j import GraphDatabase
from airflow.models import Variable
from datetime import timedelta, datetime
import ast
from pprint import pprint
import yaml
import glob


NEO4J_URI = 'bolt://10.5.0.9:7687' #Variable.get('NEO4J_URI')
NEO4J_USER = "neo4j" #Variable.get('NEO4J_USER')
NEO4J_PASSWORD = "h4ck3r" #Variable.get('NEO4J_PASSWORD')


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
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='import-data-to-neo4j', default_args=args, schedule_interval=None, catchup=False)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)


fmt = "%Y-%m-%d"
sdate = datetime.strptime(date_range['date_start'], fmt)
edate = datetime.strptime(date_range['date_end'], fmt)

delta = edate - sdate


def load_into_neo4j(ds, cypher_query, file, datareferencia, **kwargs):
    pprint(kwargs)
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with neo4j_driver.session() as session:
        
        cypher_query = cypher_query.replace('{datareferencia}', datareferencia) 
        print(cypher_query)
        print('--' * 30)
        result = session.run(cypher_query)
        logging.info("Execution: %s", result.summary().counters)


def move_to_neo4j_folder(file, datareferencia,dag):
    cmd = "mkdir -p /usr/local/airflow/neo4j/import/{file}/{datareferencia} &&  cp /usr/local/airflow/data/neo4j/{file}/{datareferencia}/*.csv /usr/local/airflow/neo4j/import/{file}/{datareferencia}/{file}.csv".format(
                    file=file, datareferencia=datareferencia)

    start >> BashOperator(
                task_id='move-file-{folder}-{datareferencia}'.format(folder = file, datareferencia= datareferencia),
                bash_command=cmd,
                dag=dag,
            ) >> end


for t in config['etl_queries']:
    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        datareferencia = day.strftime("%Y-%m-%d")

        move_to_neo4j_folder(t,datareferencia, dag)
        move_to_neo4j_folder("stopevents", datareferencia, dag)
        move_to_neo4j_folder("trackingdata", datareferencia, dag)
        move_to_neo4j_folder("event-stop-edges", datareferencia, dag)

for i in range(delta.days + 1):
    day = sdate + timedelta(days=i)
    datareferencia = day.strftime("%Y-%m-%d")

    load_into_neo4j_tasks = []
    for neo in config['neo4j_import']:

        load_into_neo4j_tasks.append(PythonOperator(
            task_id="load-into-neo4j-{folder}-{datareferencia}".format(folder=neo, datareferencia=datareferencia),
            provide_context=True,
            op_kwargs={'file': neo, 'cypher_query': config['neo4j_import'][neo]['cypher_query'],'datareferencia': datareferencia},
            python_callable=load_into_neo4j,
            dag=dag
        ))
    end >> load_into_neo4j_tasks[0]
    for i in range(0, len(load_into_neo4j_tasks)-1):
        load_into_neo4j_tasks[i] >> load_into_neo4j_tasks[i+1]

# load_into_neo4j_tasks = []
# for neo in config['neo4j_import']:
#     load_into_neo4j_tasks.append(PythonOperator(
#         task_id="load_into_neo4j_{}".format(neo),
#         provide_context=True,
#         op_kwargs={'file': neo, 'cypher_query': config['neo4j_import'][neo]['cypher_query']},
#         python_callable=load_into_neo4j,
#         dag=dag
#     ))
