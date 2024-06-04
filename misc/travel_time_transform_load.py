from jinja2 import Environment, FileSystemLoader
from connectors.postgresql import PostgreSqlClient
from dotenv import load_dotenv
import os
from assets.extract_load_transform import (
    extract_load_from_source,
    transform,
    SQLTransform,
)
from graphlib import TopologicalSorter

if __name__ == "__main__":
    load_dotenv()
    SOURCE_DATABASE_NAME = os.environ.get("DATABASE_NAME")
    SOURCE_SERVER_NAME = os.environ.get("SERVER_NAME")
    SOURCE_DB_USERNAME = os.environ.get("DB_USERNAME")
    SOURCE_DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SOURCE_PORT = os.environ.get("PORT")
    TARGET_DATABASE_NAME = os.environ.get("TARGET_DATABASE_NAME")
    TARGET_SERVER_NAME = os.environ.get("TARGET_SERVER_NAME")
    TARGET_DB_USERNAME = os.environ.get("TARGET_DB_USERNAME")
    TARGET_DB_PASSWORD = os.environ.get("TARGET_DB_PASSWORD")
    TARGET_PORT = os.environ.get("TARGET_PORT")
    LOGGING_DATABASE_NAME = os.environ.get("LOGGING_DATABASE_NAME")
    LOGGING_SERVER_NAME = os.environ.get("LOGGING_SERVER_NAME")
    LOGGING_DB_USERNAME = os.environ.get("LOGGING_DB_USERNAME")
    LOGGING_DB_PASSWORD = os.environ.get("LOGGING_DB_PASSWORD")
    LOGGING_PORT = os.environ.get("LOGGING_PORT")

    source_postgresql_client = PostgreSqlClient(
        server_name=SOURCE_SERVER_NAME,
        database_name=SOURCE_DATABASE_NAME,
        username=SOURCE_DB_USERNAME,
        password=SOURCE_DB_PASSWORD,
        port=SOURCE_PORT,
    )

    target_postgresql_client = PostgreSqlClient(
        server_name=SOURCE_SERVER_NAME,
        database_name=SOURCE_DATABASE_NAME,
        username=SOURCE_DB_USERNAME,
        password=SOURCE_DB_PASSWORD,
        port=SOURCE_PORT,
    )

    extract_template_environment = Environment(
        loader=FileSystemLoader("/Users/alexheston/dec-project-1/project/sql/extract")
    )

    extract_load_from_source(
        template_env=extract_template_environment,
        source_postgresql_client=source_postgresql_client,
        target_postgresql_client=target_postgresql_client
    )

    transform_template_environment = Environment(
        loader=FileSystemLoader("/Users/alexheston/dec-project-1/project/sql/transform")
    )


    staging_travel_time_raw = SQLTransform(
        table_name="travel_time_staging",
        postgresql_client=source_postgresql_client,
        environment=extract_template_environment,
    )

    serving_travel_time_transformed = SQLTransform(
        table_name="travel_time_transform",
        postgresql_client=target_postgresql_client,
        environment=transform_template_environment,
    )
    print(serving_travel_time_transformed)


    dag = TopologicalSorter()
    dag.add(staging_travel_time_raw)
    dag.add(serving_travel_time_transformed, staging_travel_time_raw)

    # run transform
    transform(dag=dag)
