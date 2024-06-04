from dotenv import load_dotenv
import os
from connectors.travel_time_api import TravelTimeApiClient
from assets.travel_time import extract_travel_time
from assets.travel_time import add_columns
from assets.travel_time import load
from assets.travel_time import transform
from connectors.postgresql import PostgreSqlClient
from sqlalchemy import Table, MetaData, Column, Integer, String, DateTime
from assets.pipeline_logging import PipelineLogging
import yaml
from pathlib import Path
from assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
import schedule
import time
from jinja2 import Environment, FileSystemLoader, Template
from sqlalchemy.engine import URL, Engine
from sqlalchemy import create_engine, Table, MetaData, Column


def pipeline(config: dict, pipeline_logging: PipelineLogging):
    pipeline_logging.logger.info("Starting pipeline run")
    pipeline_logging.logger.info("Getting pipeline environment variables")

    #get environment variables from .env file - used to create instance of client class to write dataframe to postgresql
    API_KEY = os.environ.get("API_KEY")
    APP_ID = os.environ.get("APP_ID")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    PORT = os.environ.get("PORT")

    pipeline_logging.logger.info("Creating Travel Time API client")
    
    #creates instance of travelTimeApiClient class to connect to API and get data returned as JSON.
    travel_time_api_client = TravelTimeApiClient(api_key = API_KEY,app_id = APP_ID)

    #Calls API endpoint with driving parameter. get_data function is in connectors/time_travel_api. Returns JSON object.
    pipeline_logging.logger.info("Extracting data from Travel Time API")
    data = travel_time_api_client.get_data(type="driving")

    # Calls extract_travel_time passing through JSON object from above. Loops through JSON object to create a dictionary and then converts to dataframe.
    df_travel_time = extract_travel_time(data)
    pipeline_logging.logger.info("Adding load_timestamp and load_id columns to dataframe")

    #Adds load_timestamp and load_id columns to the dataframe.
    df_with_timestamp = add_columns(df_travel_time)


    pipeline_logging.logger.info("Loading data to postgres")
    #create instance of postgresqlclient class
    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )

    metadata = MetaData()
    table = Table(
        "travel_time_raw",
        metadata,
        Column("search_id", String),
        Column("location_id", String),
        Column("travel_time", Integer),
        Column("load_timestamp", DateTime),
        Column("load_id", String, primary_key=True)
    )
    load(df=df_with_timestamp, postgresql_client=postgresql_client, table=table, metadata=metadata, load_method="upsert")
    pipeline_logging.logger.info("Pipeline load to raw table successful")

    # pipeline_logging.logger.info("Transforming data")
    # transform_env = Environment(loader=FileSystemLoader("project/sql/transform"))
    # transform_table_name = "travel_time_transform"
    # transform_sql_template = transform_env.get_template(
    #     f"{transform_table_name}.sql"
    # )

    # transform(
    #     engine = engine,
    #     sql_template = transform_sql_template,
    #     table_name = transform_table_name
    # )
    # pipeline_logging.logger.info("Transformation and load to transform table complete")
    # pipeline_logging.logger.info("Pipeline run successful")


def run_pipeline(
    pipeline_name: str,
    postgresql_logging_client: PostgreSqlClient,
    pipeline_config: dict,
):
    pipeline_logging = PipelineLogging(
        pipeline_name="travel_time_source",
        log_folder_path=config.get("log_folder_path")
    )

    metadata_logger = MetaDataLogging(
        pipeline_name=PIPELINE_NAME,
        postgresql_client=postgresql_logging_client,
        config=config,
    )
    try:
        metadata_logger.log() 
        pipeline(
            config=pipeline_config.get("config"), pipeline_logging=pipeline_logging
        )
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_SUCCESS, logs=pipeline_logging.get_logs()
        )  # log end 
        pipeline_logging.logger.handlers.clear()
    except BaseException as e:
        pipeline_logging.logger.error(f"Pipeline run failed. See detailed logs: {e}")
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_FAILURE, logs=pipeline_logging.get_logs()
        )
        pipeline_logging.logger.handlers.clear()




if __name__ == "__main__":
    #get environment variables from .env file - used to create logger instance of client class to write logger data to table
    load_dotenv()
    LOGGING_SERVER_NAME = os.environ.get("LOGGING_SERVER_NAME")
    LOGGING_DATABASE_NAME = os.environ.get("LOGGING_DATABASE_NAME")
    LOGGING_USERNAME = os.environ.get("LOGGING_DB_USERNAME")
    LOGGING_PASSWORD = os.environ.get("LOGGING_DB_PASSWORD")
    LOGGING_PORT = os.environ.get("LOGGING_PORT")
    
    # get config variables, used to get file path for logging
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipeline_config = yaml.safe_load(yaml_file)
            config = pipeline_config.get("config")
            PIPELINE_NAME = pipeline_config.get("name")
    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
        )

    postgresql_logging_client = PostgreSqlClient(
        server_name=LOGGING_SERVER_NAME,
        database_name=LOGGING_DATABASE_NAME,
        username=LOGGING_USERNAME,
        password=LOGGING_PASSWORD,
        port=LOGGING_PORT,
    )
    
    target_connection = URL.create(
        drivername = "postgresql+pg8000",
        username = LOGGING_USERNAME,
        password = LOGGING_PASSWORD,
        host = LOGGING_SERVER_NAME,
        port = LOGGING_PORT,
        database = LOGGING_DATABASE_NAME
    )
    engine = create_engine(target_connection)
    # set schedule
    schedule.every(pipeline_config.get("schedule").get("run_seconds")).seconds.do(
        run_pipeline,
        pipeline_name=PIPELINE_NAME,
        postgresql_logging_client=postgresql_logging_client,
        pipeline_config=pipeline_config,
    )

    while True:
        schedule.run_pending()
        time.sleep(pipeline_config.get("schedule").get("poll_seconds"))
    