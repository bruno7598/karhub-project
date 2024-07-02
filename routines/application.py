import logging
import pandas as pd
from modules.bigquery import BigQuery
from settings import Settings


class Application:

    def __init__(self):
        self.cfg = Settings()
        self.bigquery = BigQuery(self.cfg.credentials)


    def routines_application(self, filename):
        """
        This function reads a feather file, merges it with other data sources,
        and inserts the merged data into a BigQuery table.
        """
        try:
            df_insert = pd.read_feather(self.cfg.feather_directory_conformed + filename + '.feather')

            if "Despesas" in filename:
                table_id = self.cfg.table_id_despesas
            elif "Receita" in filename:
                table_id = self.cfg.table_id_receita

            self.bigquery.insert_data_bigquery(df_insert, table_id)
            logging.info(f"Os dados do arquivo '{filename}' foram inseridos na tabela '{table_id}'.")
        except Exception as e:
            logging.error(f"Error ao inserir os dados na tabela: {str(e)}")
