import lzma
import os
import shutil
from datetime import timedelta, datetime
from pprint import pprint
import logging
from neo4j import GraphDatabase
import urllib3
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

NEO4J_URI = 'bolt://10.5.0.9:7687'  # Variable.get('NEO4J_URI')
NEO4J_USER = "neo4j"  # Variable.get('NEO4J_USER')
NEO4J_PASSWORD = "h4ck3r"  # Variable.get('NEO4J_PASSWORD')


def dummy_task(task_id, dag):
    dummy = DummyOperator(task_id=task_id, dag=dag)
    return dummy


def docker_task(task_id, command, dag, image='bde2020/spark-master:2.4.4-hadoop2.7', environment='', volumes=[]):
    if image == 'bde2020/spark-master:2.4.4-hadoop2.7':
        environment = {
            'PYSPARK_PYTHON': "python3",
            'SPARK_HOME': "/spark"
        }
        volumes = ['/work/master-thesis/dataprocessing:/dataprocessing', '/work/datalake:/data']

    task = DockerOperator(
        task_id=task_id,
        image=image,
        api_version='auto',
        auto_remove=True,
        environment=environment,
        volumes=volumes,
        command=command,
        docker_url='unix://var/run/docker.sock',
        network_mode='host',
        dag=dag
    )

    return task


def create_task(task_id, op_kwargs, python_callable, _dag):
    return PythonOperator(
        task_id=task_id,
        provide_context=True,
        op_kwargs=op_kwargs,
        python_callable=python_callable,
        dag=_dag,
    )


def load_into_neo4j(ds, cypher_query, datareferencia, **kwargs):
    pprint(kwargs)
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with neo4j_driver.session() as session:
        cypher_query = cypher_query.replace('{datareferencia}', datareferencia)
        print(cypher_query)
        print('--' * 30)
        result = session.run(cypher_query)
        logging.info("Execution: %s", result.single())


def download_files(ds, folder, file, date_range, base_url, **kwargs):
    print(f"Date range: {date_range}")
    print(f"Base URL: {base_url}")
    print(f"Filename: {file}")

    sdate = datetime.strptime(date_range['date_start'], "%Y-%m-%d")
    edate = datetime.strptime(date_range['date_end'], "%Y-%m-%d")

    delta = edate - sdate

    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        download_file_day = day.strftime("%Y_%m_%d")

        datareferencia = day.replace(day=1).strftime("%Y-%m")

        url = f"{base_url}{download_file_day}_{file}"
        print(f"Downloading: {url}")

        base_folder = f"data/staging/{datareferencia}/{folder}"

        os.makedirs(base_folder, exist_ok=True)

        fd = f"data/staging/{datareferencia}/{folder}/{download_file_day}_{file}"

        http = urllib3.PoolManager()
        r = http.request('GET', url, preload_content=False)

        with open(fd, 'wb') as out:
            while True:
                data = r.read()
                if not data:
                    break
                out.write(data)

        r.release_conn()

    return "download realizado com sucesso"


def decompress_files(ds, folder, file, date_range, base_url, **kwargs):
    print(f"Date range: {date_range}")
    print(f"Base URL: {base_url}")
    print(f"Filename: {file}")

    sdate = datetime.strptime(date_range['date_start'], "%Y-%m-%d")
    edate = datetime.strptime(date_range['date_end'], "%Y-%m-%d")

    delta = edate - sdate

    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        download_file_day = day.strftime("%Y_%m_%d")

        datareferencia = day.replace(day=1).strftime("%Y-%m")

        base_folder = f"data/raw/{datareferencia}/{folder}"

        os.makedirs(base_folder, exist_ok=True)

        try:
            fstaging = f"data/staging/{datareferencia}/{folder}/{download_file_day}_{file}"
            fraw = f"{base_folder}/{download_file_day}_{file.replace('.xz', '')}"

            binary_data_buffer = lzma.open(fstaging, mode='rt', encoding='utf-8').read()

            with open(fraw, 'w') as a:
                a.write(binary_data_buffer)

        except Exception as err:
            print(f"Can't open file: {file} for date {download_file_day}")


def delete_files(ds, **kwargs):
    base_folder = f"data/staging/"
    shutil.rmtree(base_folder, ignore_errors=True)
