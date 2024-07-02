import os
import requests
import pandas as pd
from datetime import datetime

def request_api_dollar(url):
    """
    This function sends a GET request to the provided URL and retrieves the current dollar high value.

    Parameters:
        url (str): The URL of the API endpoint that provides the dollar high value.

    Returns:
        float: The current dollar high value if the request is successful and the data is available.
            Returns None if the request fails or the data is not available.
    """
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        if len(data) > 0:
            high_rate = float(data[0]['high'])
            print(f"Taxa de compra (high): {high_rate}")

def data_insert(df):
    """
    This function adds a new column 'dt_insert' to the provided DataFrame,
    containing the current date and time in the format 'YYYY-MM-DD HH:MM:SS'.

    Parameters:
        df (pandas.DataFrame): The DataFrame to which the new column will be added.

    Returns:
        pandas.DataFrame: The DataFrame with the new 'dt_insert' column added.
    """
    df['dt_insert'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['dt_insert'] = pd.to_datetime(df['dt_insert'])

    return df
