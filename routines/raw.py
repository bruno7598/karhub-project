import os
import logging
import pandas as pd
from settings import Settings

class Raw:

    def __init__(self):
        self.cfg = Settings()

    def routine_raw(self, filename):
        """
        Converts a CSV file to a Feather file.

        Parameters:
            csv_filename (str): The name of the CSV file to be converted.
        """
        csv_path = os.path.join(self.cfg.csv_directory, filename + '.csv')
        feather_filename = os.path.splitext(filename)[0] + '.feather'
        feather_path = os.path.join(self.cfg.feather_directory, feather_filename)

        try:
            df = pd.read_csv(csv_path, encoding='latin1')
            df.to_feather(feather_path)
            logging.info(f"Arquivo CSV '{filename}.csv' convertido com sucesso para '{feather_filename}'.")
        except Exception as e:
            logging.error(f"Erro ao converter arquivo CSV '{filename}.csv' para Feather: {str(e)}")
