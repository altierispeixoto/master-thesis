#FROM bde2020/spark-submit:2.4.4-hadoop2.7
FROM bde2020/spark-master:2.4.4-hadoop2.7

LABEL maintainer="Altieris Peixoto <altieris.marcelino@gmail.com>"

#COPY template.sh /

# Copy the requirements.txt first, for separate dependency resolving and downloading
COPY requirements.txt /app/
#RUN pip3 install --upgrade pip
RUN apk update && apk add libpq
RUN apk add --no-cache --virtual .build-deps \
    gcc \
    python3-dev \
    musl-dev \
    postgresql-dev \
    && pip3 install --no-cache-dir psycopg2==2.7.1 \
    && apk del --no-cache .build-deps
#RUN cd /app \
#      && pip3 install -r requirements.txt

# Copy the source code
#ONBUILD COPY ../airflow /app

#CMD ["/bin/bash", "/template.sh"]