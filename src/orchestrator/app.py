from fastapi import FastAPI
import requests
import os

app = FastAPI()

API_CLIENT_URL = os.getenv("API_CLIENT_URL", "http://api-client:8080")
BQ_MANAGER_URL = os.getenv("BQ_MANAGER_URL", "http://bq-manager:8080")
GCS_MANAGER_URL = os.getenv("GCS_MANAGER_URL", "http://gcs-manager:8080")

@app.post("/run-pipeline/{subreddit}")
def run_pipeline(subreddit: str, limit: int = 25):
    # Fetch from API client
    response = requests.get(f"{API_CLIENT_URL}/fetch/{subreddit}?limit={limit}")
    data = response.json()
    image_metadata = data["image_posts"]
    image_urls = data["image_urls"]
    image_filenames = data["image_filenames"]
    
    # Upload to GCS
    requests.post(f"{GCS_MANAGER_URL}/upload", json={"image_urls": image_urls, "image_filenames": image_filenames})
    
    # Insert to BQ
    requests.post(f"{BQ_MANAGER_URL}/insert", json=image_metadata)
    
    return {"status": "pipeline completed"}