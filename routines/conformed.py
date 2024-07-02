import os
import logging
import pandas as pd
from settings import Settings
from modules.utils import request_api_dollar, data_insert

class Conformed:

    def __init__(self):
        self.cfg = Settings()


    def routines_conformed(self, filename):
        """
        This function reads feather files from a specified directory, processes the data,
        and returns a merged DataFrame.
        """
        try:
            feather_path = os.path.join(self.cfg.feather_directory, filename + '.feather')
            df_final = pd.read_feather(feather_path)

            high_rate = request_api_dollar(self.cfg.api_dollar)

            if "Despesas" in filename:
                df_result = self.process_data(df_final, high_rate, "Liquidado", "Despesa")
            elif "Receita" in filename:
                df_result = self.process_data(df_final, high_rate, "Arrecadado", "Receita")

            df_result.to_feather(self.cfg.feather_directory_conformed + filename + '.feather')
            logging.info(f"O arquivo '{feather_path} foi tratado corretamente e salvo'.")
        except Exception as e:
            logging.error(f"Erro ao ler ou converter arquivo Feather: {str(e)}")
            return None

    def process_data(self, df, high_rate, type_column_value, type_column_details):
        """
        Processes the DataFrame by extracting resource ID and name,
        converting a specified column to float, grouping by ID and name,
        and multiplying the grouped column by the bid rate.

        Parameters:
            df (DataFrame): Input DataFrame.
            high_rate (float): Bid rate to apply.
            type_column_value (str): Column name to process.

        Returns:
            grouped_df (DataFrame): Processed DataFrame.
        """
        df[['ID Fonte Recurso', 'Nome Fonte Recurso']] = df['Fonte de Recursos'].str.extract(r'^(\d+) - (.*)$')

        df[type_column_value] = df[type_column_value].str.replace('.', '').str.replace(',', '.').astype(float)

        grouped_df = df.groupby(['ID Fonte Recurso', 'Nome Fonte Recurso', f'{type_column_details}']).agg({
            type_column_value: 'sum'
        }).reset_index()

        grouped_df[type_column_value] = grouped_df[type_column_value] * high_rate

        grouped_df = data_insert(grouped_df)

        return grouped_df
