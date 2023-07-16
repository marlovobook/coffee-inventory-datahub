"""s3 DataHub Ingest DAG

This example demonstrates how to ingest metadata from s3 into DataHub
from within an Airflow DAG. In contrast to the MySQL example, this DAG
pulls the DB connection configuration from Airflow's connection store.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonVirtualenvOperator


def ingest_from_s3(s3_credentials, datahub_gms_server):
    from datahub.ingestion.run.pipeline import Pipeline

    pipeline = Pipeline.create(
        # This configuration is analogous to a recipe configuration.
        {
            "source": {
                "type": "s3",
                "config": {
                    **s3_credentials,
                    # Other s3 config can be added here.
                    "path_specs": {
                        "include": 's3://datalake/*.*'},
                    "aws_endpoint_url": 'http://minio:9000',
                    "aws_region": "us-east-2",
                    "env": "PROD",
                    "profiling": {"enabled": False},
                },
            },
            # Other ingestion features, like transformers, are also supported.
            # "transformers": [
            #     {
            #         "type": "simple_add_dataset_ownership",
            #         "config": {
            #             "owner_urns": [
            #                 "urn:li:corpuser:example",
            #             ]
            #         },
            #     }
            # ],
            "sink": {
                "type": "datahub-rest",
                "config": {"server": datahub_gms_server},
            },
        }
    )
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status()


with DAG(
    "datahub_s3_ingest",
    default_args={
        "owner": "airflow",
    },
    description="An example DAG which ingests metadata from s3 to DataHub",
    start_date=datetime(2022, 1, 1),
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    # This example pulls credentials from Airflow's connection store.
    # For this to work, you must have previously configured these connections in Airflow.
    # See the Airflow docs for details: https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html
    s3_conn = BaseHook.get_connection("minio")
    datahub_conn = BaseHook.get_connection("datahub_rest_default")

    # While it is also possible to use the PythonOperator, we recommend using
    # the PythonVirtualenvOperator to ensure that there are no dependency
    # conflicts between DataHub and the rest of your Airflow environment.
    ingest_task = PythonVirtualenvOperator(
        task_id="ingest_from_s3",
        requirements=[
            "acryl-datahub[s3]",
        ],
        system_site_packages=False,
        python_callable=ingest_from_s3,
        op_kwargs={
            "s3_credentials": {
                "aws_access_key_id": s3_conn.login,
                "aws_secret_access_key": s3_conn.password,
                "aws_endpoint_url": "http://minio:9000",
                
                
            },
            "datahub_gms_server": datahub_conn.host,
        },
    )


# "username": s3_conn.login,
#"password": s3_conn.password,       
# "account_id": s3_conn.extra_dejson["account"],
# "warehouse": s3_conn.extra_dejson.get("warehouse"),
# "role": s3_conn.extra_dejson.get("role"),


# AWS Access Key ID