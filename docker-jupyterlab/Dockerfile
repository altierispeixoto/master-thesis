ARG BASE_CONTAINER=jupyter/pyspark-notebook
FROM $BASE_CONTAINER

MAINTAINER Altieris Peixoto altieris.marcelino@gmail.com

USER root
RUN apt-get -qq update && apt-get install -y --no-install-recommends apt-utils openssh-client

USER $NB_UID

WORKDIR /opt/work/
RUN python -m pip install --upgrade --force-reinstall --ignore-installed pip
ADD requirements.txt .
RUN pip install --quiet -r requirements.txt
RUN pip install --quiet pytest pytest-cov parameterized

USER root
COPY ./docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh"]
#RUN echo 'c.NotebookApp.contents_manager_class = "notedown.NotedownContentsManager"' >> ~/.jupyter/jupyter_notebook_config.py
