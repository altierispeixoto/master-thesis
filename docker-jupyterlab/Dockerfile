ARG BASE_CONTAINER=jupyter/pyspark-notebook
FROM $BASE_CONTAINER

MAINTAINER Altieris Peixoto altieris.marcelino@gmail.com

USER root
RUN apt-get -qq update && apt-get install -y --no-install-recommends apt-utils openssh-client   \
    && apt-get update -yq \
    && apt-get install curl gnupg -yq \
    && curl -sL https://deb.nodesource.com/setup_14.x | bash \
    && apt-get install nodejs -yq \
    && apt-get install jq -yq

USER $NB_UID

WORKDIR /opt/work/
RUN python -m pip install --upgrade --force-reinstall --ignore-installed pip
ADD requirements.txt .
RUN pip install --quiet -r requirements.txt
RUN pip install --quiet pytest pytest-cov parameterized

ENV NODE_OPTIONS="--max-old-space-size=4096"

RUN pip install jupyterlab && \
    jupyter serverextension enable --py jupyterlab --sys-prefix && \
    pip install pydeck && \
    pip install bqplot && \
    jupyter labextension install @jupyter-widgets/jupyterlab-manager --no-build && \
    jupyter labextension install @deck.gl/jupyter-widget --no-build && \
    jupyter labextension install nbdime-jupyterlab --no-build  && \
    jupyter lab build && \
    jupyter lab clean && \
    jlpm cache clean && \
    npm cache clean --force && \
    rm -rf $HOME/.node-gyp && \
    rm -rf $HOME/.local && \
    fix-permissions $CONDA_DIR $HOME

USER root
COPY ./docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh"]
