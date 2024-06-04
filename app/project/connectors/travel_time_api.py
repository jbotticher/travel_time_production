import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Penn Station(destination): lat=40.7500792, lng=-73.9913481
# Hoboken: lat=40.7433066, lng=-74.0323752
# Stamford: lat=41.0534302, lng=-73.5387341
# Hackensack: lat=40.8871438, lng=-74.0410865

class TravelTimeApiClient:

    def __init__(self, api_key, app_id):
        if api_key is None:
            raise Exception("API key cannot be set to None.")
        if app_id is None:
            raise Exception("App ID cannot be set to None.")
        self.api_key = api_key
        self.app_id = app_id

    def get_data(self, type):
        # Make a request to get data. Searches based on an arrival time. Arrive at the location no earlier than the given time.
        # Specify multiple departure locations and one arrival location in each search
        current_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        url = f"https://api.traveltimeapp.com/v4/time-filter?type={type}&arrival_time={current_timestamp}&search_lat=40.7500792&search_lng=-73.9913481&locations=40.7433066_-74.0323752,41.0534302_-73.5387341,40.8871438_-74.0410865&app_id={self.app_id}&api_key={self.api_key}"
        response = requests.get(url, verify=False)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                        f"Failed to extract data from Travel Time API. Status Code: {response.status_code}. Response: {response.text}"
                    )
