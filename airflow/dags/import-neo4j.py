import logging
from datetime import timedelta
import time
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from neo4j import GraphDatabase
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
import yaml
import glob
from pprint import pprint

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depend_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
NEO4J_URI = 'bolt://10.5.0.9:7687' #Variable.get('NEO4J_URI')
NEO4J_USER = "neo4j" #Variable.get('NEO4J_USER')
NEO4J_PASSWORD = "h4ck3r" #Variable.get('NEO4J_PASSWORD')

config = yaml.load(open('./dags/config/data.yml'), Loader=yaml.FullLoader)


def load_into_neo4j(ds, cypher_query, file, **kwargs):
    pprint(kwargs)
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with neo4j_driver.session() as session:
        if file == 'trackingdata':
            files = [f.split('/')[-1] for f in glob.glob("/usr/local/airflow/neo4j/import/trackingdata*", recursive=False) if f.endswith(".csv")]
            for i in files:
                print(i)
                cypher_query_ex = cypher_query.replace('template', i)
                print(cypher_query_ex)
                print('--'*30)
                result = session.run(cypher_query_ex)
                logging.info("Execution: %s", result.summary().counters)
                time.sleep(30)

        elif file == 'event-stop-edges':
            files = [f.split('/')[-1] for f in glob.glob("/usr/local/airflow/neo4j/import/event-stop-edges*", recursive=False) if f.endswith(".csv")]
            for i in files:
                cypher_query_ex = cypher_query.replace('template', i)
                print(cypher_query_ex)
                print('--'*30)
                result = session.run(cypher_query_ex)
                logging.info("Execution: %s", result.summary().counters)
                time.sleep(30)
        else:
            print(cypher_query)
            print('--' * 30)
            result = session.run(cypher_query)
            logging.info("Execution: %s", result.summary().counters)



"""Build DAG."""
dag = DAG('import-neo4j', default_args=DEFAULT_ARGS, schedule_interval=None, catchup=False)
start = DummyOperator(task_id='start', dag=dag)
dummy0 = DummyOperator(task_id='wait0', dag=dag)
dummy1 = DummyOperator(task_id='wait1', dag=dag)

spark_load_from_pg = []
rename_files = []
load_into_neo4j_tasks = []



for t in config['etl_queries']:

    query = config['etl_queries'][t]

    query  = query.format(datareferencia="2019-01-01")
    load_from_pg = '/spark/bin/spark-submit --master local[*] --driver-class-path ' \
                   '/spark-urbs-processing/jars/postgresql-42.2.8.jar  /spark-urbs-processing/load_from_postgresql.py -q "{}" -f {}' \
        .format(query, t)

    spark_load_from_pg.append(DockerOperator(
        task_id='spark_etl_from_pg_{}'.format(t),
        image='altr/spark',
        api_version='auto',
        auto_remove=True,
        environment={
            'PYSPARK_PYTHON': "python3",
            'SPARK_HOME': "/spark"
        },
        volumes=['/home/altieris/master-thesis/airflow/spark-urbs-processing:/spark-urbs-processing'
            , '/home/altieris/master-thesis/airflow/data:/data'],
        command=load_from_pg,
        docker_url='unix://var/run/docker.sock',
        network_mode='host', dag=dag
    ))

    rename_files.append(BashOperator(
        task_id='move_file_{}'.format(t),
        bash_command="cp /usr/local/airflow/data/processed/{file}/*.csv /usr/local/airflow/neo4j/import/{file}.csv".format(
            file=t),
        dag=dag,
    ))

for neo in config['neo4j_import']:
    load_into_neo4j_tasks.append(PythonOperator(
        task_id="load_into_neo4j_{}".format(neo),
        provide_context=True,
        op_kwargs={'file': neo, 'cypher_query': config['neo4j_import'][neo]['cypher_query']},
        python_callable=load_into_neo4j,
        dag=dag
    ))



stopevents = '/spark/bin/spark-submit --master local[*] --driver-class-path ' \
                   '/spark-urbs-processing/jars/postgresql-42.2.8.jar  /spark-urbs-processing/stop-events.py '

stopevents_processing = DockerOperator(
        task_id='spark_etl_stop_events',
        image='altr/spark',
        api_version='auto',
        auto_remove=True,
        environment={
            'PYSPARK_PYTHON': "python3",
            'SPARK_HOME': "/spark"
        },
        volumes=['/home/altieris/master-thesis/airflow/spark-urbs-processing:/spark-urbs-processing'
            , '/home/altieris/master-thesis/airflow/data:/data'],
        command=stopevents,
        docker_url='unix://var/run/docker.sock',
        network_mode='host', dag=dag
    )


tracking_data = '/spark/bin/spark-submit --master local[*] --driver-class-path ' \
                   '/spark-urbs-processing/jars/postgresql-42.2.8.jar  /spark-urbs-processing/tracking-data.py '

tracking_data_processing = DockerOperator(
        task_id='spark_etl_tracking_data',
        image='altr/spark',
        api_version='auto',
        auto_remove=True,
        environment={
            'PYSPARK_PYTHON': "python3",
            'SPARK_HOME': "/spark"
        },
        volumes=['/home/altieris/master-thesis/airflow/spark-urbs-processing:/spark-urbs-processing'
            , '/home/altieris/master-thesis/airflow/data:/data'],
        command=tracking_data,
        docker_url='unix://var/run/docker.sock',
        network_mode='host', dag=dag
    )


event_stop_edges = '/spark/bin/spark-submit --master local[*] --driver-class-path ' \
                   '/spark-urbs-processing/jars/postgresql-42.2.8.jar  /spark-urbs-processing/event-stop-edges.py '

event_stop_edges_processing = DockerOperator(
        task_id='spark_etl_event_stop_edges',
        image='altr/spark',
        api_version='auto',
        auto_remove=True,
        environment={
            'PYSPARK_PYTHON': "python3",
            'SPARK_HOME': "/spark"
        },
        volumes=['/home/altieris/master-thesis/airflow/spark-urbs-processing:/spark-urbs-processing'
            , '/home/altieris/master-thesis/airflow/data:/data'],
        command=event_stop_edges,
        docker_url='unix://var/run/docker.sock',
        network_mode='host', dag=dag
    )


move_event_files = []

move_event_files.append(BashOperator(
    task_id='move_file_stopevents',
    bash_command="cp /usr/local/airflow/data/processed/{file}/*.csv /usr/local/airflow/neo4j/import/{file}.csv".format(
        file="stopevents"),
    dag=dag,
))

cmd = 'ls -v /usr/local/airflow/data/processed/trackingdata/ | cat -n | while read n f; do mv -n "/usr/local/airflow/data/processed/trackingdata/$f" "/usr/local/airflow/neo4j/import/trackingdata$n.csv"; done'

move_event_files.append(BashOperator(
    task_id='move_file_trackingdata',
    bash_command=cmd,
    dag=dag,
))

cmd2 = 'ls -v /usr/local/airflow/data/processed/event-stop-edges/ | cat -n | while read n f; do mv -n "/usr/local/airflow/data/processed/event-stop-edges/$f" "/usr/local/airflow/neo4j/import/event-stop-edges$n.csv"; done'

move_event_files.append(BashOperator(
    task_id='move_file_event-stop-edges',
    bash_command=cmd2,
    dag=dag,
))


start >> stopevents_processing >> tracking_data_processing >> event_stop_edges_processing

for i in range(0, len(move_event_files)):
    event_stop_edges_processing >> move_event_files[i] >> dummy0

for j in range(0, len(spark_load_from_pg)):
    dummy0 >> spark_load_from_pg[j] >> rename_files[j] >> dummy1

dummy1 >> load_into_neo4j_tasks[0]

for i in range(0, len(load_into_neo4j_tasks)-1):
    load_into_neo4j_tasks[i] >> load_into_neo4j_tasks[i+1]


#https://github.com/blockchain-etl/bitcoin-etl-airflow-neo4j/blob/master/dags/dag_btc_to_neo4j.py