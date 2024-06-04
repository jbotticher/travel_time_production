import pandas as pd
from datetime import datetime
from project.connectors.postgresql import PostgreSqlClient
from sqlalchemy.engine import URL, Engine
from sqlalchemy import create_engine, Table, MetaData, Column
from sqlalchemy.dialects import postgresql
from jinja2 import Environment, FileSystemLoader, Template
from sqlalchemy import inspect



def extract_travel_time(response_data: dict)->pd.DataFrame:
    # Extract data from the JSON response which we want and convert to dataframe
    data = []
    for result in response_data['results']:
        for location in result['locations']:
            data.append({
                'search_id': result['search_id'],
                'location_id': location['id'],
                'travel_time': location['properties'][0]['travel_time']
            })

    return pd.DataFrame(data)
     

# Function to remove spaces and special characters
def _remove_special_characters(text):
    # Replace special characters and spaces with empty string
    cleaned_text = ''.join(e for e in text if e.isalnum())
    return cleaned_text


def add_columns(df: pd.DataFrame) -> pd.DataFrame:
    df['load_timestamp'] = datetime.now()
    df['load_id'] = df['location_id'] + df['travel_time'].astype(str) + df['load_timestamp'].astype(str)
    df['load_id'] = df['load_id'].apply(_remove_special_characters)
    return df
    

def load(
df: pd.DataFrame,
postgresql_client: PostgreSqlClient,
table: Table,
metadata: MetaData,
load_method: str
) -> None:

    if load_method == "insert":
        postgresql_client.insert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "upsert":
        postgresql_client.upsert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "overwrite":
        postgresql_client.overwrite(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    else:
        raise Exception(
            "Please specify a correct load method: [insert, upsert, overwrite]"
        )
    

def transform(engine: Engine, sql_template: Template, table_name: str):
    extract_type = sql_template.make_module().config.get("extract_type")

    if extract_type == "full":
        full_sql = f"""
        drop table if exists {table_name};
        create table {table_name} as (
        {sql_template.render()}
        )"""
        engine.execute(full_sql)
    elif extract_type == "incremental":
        # source_table_name = sql_template.make_module().config.get("source_table_name")
        if inspect(engine).has_table(table_name):
            incremental_column = sql_template.make_module().config.get(
                "incremental_column"
            )
            sql_result = [
                dict(row)
                for row in engine.execute(
                    f"select max({incremental_column}) as incremental_value from {table_name}"
                ).all()
            ]
            incremental_value = sql_result[0].get("incremental_value")
            inc_sql = sql_template.render(
                is_incremental=True, incremental_value=incremental_value
            )

            insert_sql = f"""
            insert into {table_name} (
            {inc_sql}
            )"""
            engine.execute(insert_sql)
        else:
            inc_sql = sql_template.render(is_incremental=False)
            insert_sql = f"""
            create table {table_name} as (
            {inc_sql}
            )"""
            engine.execute(insert_sql)
    else:
        raise Exception(
            f"Extract type {extract_type} is not supported. Please use either 'full' or 'incremental' extract type."
    )

