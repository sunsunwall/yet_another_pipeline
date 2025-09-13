import dotenv
import os
from google.cloud import bigquery

class BQManager:

    def __init__(self) -> None:
        
        dotenv.load_dotenv()
        self.project_id = os.getenv("GCP_PROJECT_ID")
        self.dataset_id = os.getenv("BIGQUERY_DATASET_ID")
        self.table_id = os.getenv("BIGQUERY_TABLE_ID")

    def connect_to_bq(self, gcp_project_id, bq_dataset_id, bq_table_id):

        