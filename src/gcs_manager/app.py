from fastapi import FastAPI
from src.gcs_manager.gcs_manager import GCSManager
import os

app = FastAPI()

gcs_manager = GCSManager(
    os.getenv("GCP_PROJECT_ID"),
    os.getenv("GCS_BUCKET_NAME")
)

@app.post("/upload")
def upload_images(image_urls: list[str], image_filenames: list[str]):
    result = gcs_manager.upload_images(image_urls, image_filenames)
    return result