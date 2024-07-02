import logging

from google.cloud import bigquery
from google.oauth2 import service_account
from settings import Settings

class BigQuery:

    def __init__(self, gcp_credential_path=None):
        self.cfg = Settings()
        self.service = credentials = service_account.Credentials.from_service_account_file(
            gcp_credential_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.bigquery = bigquery
        self.client = self.bigquery.Client(project=self.cfg.project_name, credentials=credentials)


    def insert_data_bigquery(self, df, table_id):
        """
        This function is responsible for inserting a pandas DataFrame into a specified BigQuery table.

        Parameters:
            df (pandas.DataFrame): The DataFrame containing the data to be inserted.
            table_id (str): The ID of the BigQuery table where the data will be inserted. The table ID is in the format 'project_id.dataset_id.table_id'.
        """
        job_config = self.bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE')

        try:
            job = self.client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            job.result()
        except Exception as e:
            logging(f"Erro ao carregar dados para o BigQuery: {e}")
