#!/usr/bin/env bash
docker run -d --name jupyter-spark -p 8888:8888 -p 4040:4040 \
     -v /home/altieris/datascience/data:/home/jovyan/data \
     -v /home/altieris/docker/urbs-graph-model/neo4j/import:/home/jovyan/neo4j \
     -v /home/altieris/master-thesis:/home/jovyan/work jupyter/all-spark-notebook start.sh jupyter lab --NotebookApp.token=''