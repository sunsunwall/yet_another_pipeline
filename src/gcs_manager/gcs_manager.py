import os
import dotenv
import requests
from google.cloud import storage
from google.api_core.exceptions import BadRequest, Forbidden, NotFound, Conflict, InternalServerError, GoogleAPIError
from src.shared.kebab_case import to_kebab_case
from src.shared.logger import setup_logger

logger = setup_logger(__name__)

# Load environment variables from .env file
dotenv.load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "reddit_images_bucket")

class GCSManager:

    def __init__(self, gcp_project_id: str, gcs_bucket_name: str):
        
        self.client = storage.Client(project=gcp_project_id)
        self.bucket_name = gcs_bucket_name
        self.bucket = self.client.bucket(gcs_bucket_name)

    def upload_images(self, image_urls: list, image_filenames: list):

        if not image_urls or not image_filenames or len(image_urls) != len(image_filenames):
            logger.error("Image URLs and filenames must be provided and have the same length")
            
        try:

            for url, filename in zip(image_urls, image_filenames):

                response = requests.get(url, stream=True)
                response.raise_for_status()
                logger.info(f"Response Status Code: {response.status_code} for URL: {url}")

                content_type = response.headers.get('content-type', '')
                image = response.content
                logger.info(f"Downloading image from: {url}")

                gcs_filename = to_kebab_case(filename)
                blob = self.bucket.blob(gcs_filename)

                blob.upload_from_string(image, content_type=content_type)
                logger.info(f"Uploaded {filename} --> gs://{self.bucket_name}/{gcs_filename}")

        except BadRequest as e:
            logger.error(f"Bad request error during GCS upload (e.g., invalid data or bucket name): {e}")
        except Forbidden as e:
            logger.error(f"Forbidden error during GCS upload (e.g., insufficient permissions): {e}")
        except NotFound as e:
            logger.error(f"Not found error during GCS upload (e.g., bucket does not exist): {e}")
        except Conflict as e:
            logger.error(f"Conflict error during GCS upload (e.g., object already exists): {e}")
        except InternalServerError as e:
            logger.error(f"Internal server error during GCS upload (transient GCP issue): {e}")
        except GoogleAPIError as e:
            logger.error(f"Other Google API error during GCS upload: {e}")
        except Exception as e:
            logger.error(f"Unexpected error uploading images to GCS: {e}")

if __name__ == "__main__":
    gcs_manager = GCSManager(GCP_PROJECT_ID, GCS_BUCKET_NAME)
    sample_urls = [
        "https://i.redd.it/abcd1234xyz.jpg",
        "https://i.redd.it/wxyz5678abc.png"
    ]
    sample_filenames = ["abcd1234xyz.jpg", "wxyz5678abc.png"]
    gcs_manager.upload_images(sample_urls, sample_filenames)