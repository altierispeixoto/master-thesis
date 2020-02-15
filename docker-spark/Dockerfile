#FROM bde2020/spark-submit:2.4.4-hadoop2.7
FROM bde2020/spark-master:2.4.4-hadoop2.7

LABEL maintainer="Altieris Peixoto <altieris.marcelino@gmail.com>"

# Copy the requirements.txt first, for separate dependency resolving and downloading
COPY requirements.txt /app/
RUN apk update && apk add libpq
RUN apk add --no-cache --virtual .build-deps \
    gcc \
    python3-dev \
    musl-dev \
    postgresql-dev \
    && pip3 install --no-cache-dir psycopg2==2.7.1 \
    && apk del --no-cache .build-deps

ENV USER=spark
ENV UID=1000
ENV GID=1000

RUN addgroup --gid "$GID" "$USER" \
    && adduser \
    --disabled-password \
    --gecos "" \
    --home "$(pwd)" \
    --ingroup "$USER" \
    --no-create-home \
    --uid "$UID" \
    "$USER"

USER $USER

#docker build -t altr/spark .