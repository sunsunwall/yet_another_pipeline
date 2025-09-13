from fastapi import FastAPI
from src.bq_manager.bq_manager import BQManager
import os

app = FastAPI()

bq_manager = BQManager(
    os.getenv("GCP_PROJECT_ID"),
    os.getenv("BIGQUERY_DATASET_ID"),
    os.getenv("BIGQUERY_TABLE_ID")
)

@app.post("/insert")
def insert_metadata(image_metadata: list):
    bq_manager.insert_metadata(image_metadata)
    return {"status": "inserted"}