from dotenv import load_dotenv
from project.connectors.travel_time_api import TravelTimeApiClient
from project.assets.travel_time import extract_travel_time
from project.assets.travel_time import add_columns
import os
import pytest
import pandas as pd


@pytest.fixture
def setup():
    load_dotenv()


def test_first_dataframe(setup):
    API_KEY = os.environ.get("API_KEY")
    APP_ID = os.environ.get("APP_ID")
    api_client = TravelTimeApiClient(api_key=API_KEY, app_id = APP_ID)
    data = api_client.get_data(type="driving")
    df_travel_time = extract_travel_time(data)
    col1 = df_travel_time.columns
    df_with_timestamp = add_columns(df_travel_time)
    
    assert len(col1) == 3
    

def test_second_dataframe(setup):
    API_KEY = os.environ.get("API_KEY")
    APP_ID = os.environ.get("APP_ID")
    api_client = TravelTimeApiClient(api_key=API_KEY, app_id = APP_ID)
    data = api_client.get_data(type="driving")
    df_travel_time = extract_travel_time(data)
    df_with_timestamp = add_columns(df_travel_time)
    col2 = df_with_timestamp.columns

    assert len(col2) == 5


def test_no_special_characters_or_spaces(setup):
    API_KEY = os.environ.get("API_KEY")
    APP_ID = os.environ.get("APP_ID")
    api_client = TravelTimeApiClient(api_key=API_KEY, app_id = APP_ID)
    data = api_client.get_data(type="driving")
    df_travel_time = extract_travel_time(data)
    df_with_timestamp = add_columns(df_travel_time)
    column = df_with_timestamp.get('load_id')
    column_list = column.tolist()

    for i in column_list:
        test = i.isalnum()
        assert test == True