FROM python:3.9.13

# install useful utilities
RUN apt update \
    && apt update -y \
    && apt install git jq curl wget tar bash vim telnet dnsutils libsasl2-dev -y

# install datahub cli (ensure it is the same version as your datahub install)
RUN pip install --upgrade pip wheel setuptools \
    && pip cache purge \
    && pip install acryl-datahub==0.10.4.2 \
    && datahub version

# install datahub source/sink plugins
RUN pip install \
    "acryl-datahub[athena, bigquery, dbt, glue, hive, kafka, mongodb, mssql, mysql, postgres, redshift, redshift-usage, s3, snowflake, great-expectations, superset]" \
    && datahub check plugins

RUN pip install acryl-datahub-airflow-plugin[datahub-kafka]


FROM postgres

COPY db-setup/*.sql /docker-entrypoint-initdb.d/

ADD db-setup/setup.sql /docker-entrypoint-initdb.d

COPY data/* /data/

ADD data/table_product_demand.csv /data

RUN chmod a+r /docker-entrypoint-initdb.d/*

# FROM quay.io/astronomer/astro-runtime:8.2.0
# RUN pip install astronomer-providers

