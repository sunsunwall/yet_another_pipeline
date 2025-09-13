from fastapi import FastAPI
from src.api_client.api_client import APIClient

app = FastAPI()

@app.get("/fetch/{subreddit}")
def fetch_images(subreddit: str, limit: int = 25):
    client = APIClient()
    image_posts, image_urls, image_filenames = client.get_subreddit_images(subreddit, limit)
    return {"image_posts": image_posts, "image_urls": image_urls, "image_filenames": image_filenames}