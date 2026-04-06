FROM jupyter/pyspark-notebook:latest

USER root

RUN pip install --no-cache-dir \
    psycopg2-binary \
    sqlalchemy \
    tenacity

USER ${NB_UID}

WORKDIR /workspace

EXPOSE 8888 4040
