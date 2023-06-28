FROM apache/airflow:latest

USER airflow

RUN pip install acryl-datahub-airflow-plugin==0.10.4.3