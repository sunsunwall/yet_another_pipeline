from src.api_client.api_client import APIClient
from src.bq_manager.bq_manager import BQManager
from src.gcs_manager.gcs_manager import GCSManager
import dotenv
import os
from src.shared.logger import setup_logger

logger = setup_logger(__name__)

dotenv.load_dotenv()

## GCP Config
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BIGQUERY_DATASET_ID = os.getenv("BIGQUERY_DATASET_ID")
BIGQUERY_TABLE_ID = os.getenv("BIGQUERY_TABLE_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

## Reddit API Config
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT")

api_client = APIClient(REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT)
bq_manager = BQManager(GCP_PROJECT_ID, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_ID)
gcs_manager = GCSManager(GCP_PROJECT_ID, GCS_BUCKET_NAME)

logger.info("Initialized APIClient, BQManager, and GCSManager")

def fetch_and_store_images(subreddit: str, limit: int = 25):

    if not subreddit:
        logger.error("Subreddit name not provided")
    
    if not all([GCP_PROJECT_ID, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_ID, GCS_BUCKET_NAME]):
        logger.error("Missing GCP configuration in environment variables")

    
    logger.info(f"Fetching images from subreddit: {subreddit} with limit: {limit}")
    # Fetch image metadata from Reddit
    image_metadata, image_urls, image_filenames = api_client.get_subreddit_images(subreddit, limit=limit, save_json=True)
    logger.info(f"Fetched {len(image_metadata)} images from subreddit: {subreddit}")

    if not image_metadata:
        logger.info(f"No images found in subreddit: {subreddit}")
        return

    logger.info(f"Fetched {len(image_metadata)} images from subreddit: {subreddit}")
    
    # Insert metadata into BigQuery
    bq_manager.insert_metadata(image_metadata)
    # Upload images to GCS
    gcs_manager.upload_images(image_urls, image_filenames)

    print(f"Processed {len(image_metadata)} images from r/{subreddit} and stored metadata in BigQuery.")

if __name__ == "__main__":
    # Example usage
    fetch_and_store_images("funnymemes", limit=200)
