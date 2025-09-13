import datetime as dt
import dotenv
import os
import time
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError, BadRequest, Forbidden, NotFound, Conflict, InternalServerError

from src.shared.logger import setup_logger

logger = setup_logger(__name__)

# Environment variables
dotenv.load_dotenv()
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BIGQUERY_DATASET_ID = os.getenv('BIGQUERY_DATASET_ID', 'metadata')
BIGQUERY_TABLE_ID = os.getenv('BIGQUERY_TABLE_ID', 'reddit_images_metadata')

class BQManager:

    def __init__(self, gcp_project_id: str, bq_dataset_id: str, bq_table_id: str) :
        self.client = bigquery.Client(project=gcp_project_id)
        self.project_id = gcp_project_id
        self.dataset_id = bq_dataset_id
        self.table_id = bq_table_id
        self.table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"

    def insert_metadata(self, image_metadata: list) -> None:
        
        try:
            rows_to_insert = []
            ingestion_timestamp = dt.datetime.now(dt.timezone.utc).isoformat()

            for metadata in image_metadata:
            
                required_fields = ['id', 'title', 'author', 'subreddit', 'score', 'url', 'filename']
                missing_fields = [field for field in required_fields if field not in metadata or metadata[field] is None]
                
                if missing_fields:
                    
                    logger.error(f"Skipping row due to missing required fields: {missing_fields} for metadata: {metadata}")
                    continue

                row = {
                    "id": metadata["id"],
                    "title": metadata["title"],
                    "author": metadata["author"],
                    "subreddit": metadata["subreddit"],
                    "score": metadata["score"],
                    "url": metadata["url"],
                    "filename": metadata["filename"],
                    "upload_timestamp": ingestion_timestamp,
                    "gcs_filename": metadata.get("gcs_filename"),
                    "gcs_url": metadata.get("gcs_url"),
                    "file_size_bytes": metadata.get("file_size_bytes"),
                    "content_type": metadata.get("content_type"),
                }
                logger.info(f"Prepared row for insertion: id={row['id']}, filename={row['filename']}")
                rows_to_insert.append(row)

            logger.info(f"Attempting to insert {len(rows_to_insert)} rows into table {self.table_ref}")
            
            errors = self.client.insert_rows_json(self.table_ref, rows_to_insert)
            
            if errors:
            
                logger.error(f"Encountered {len(errors)} errors while inserting rows into {self.table_ref}")
                for idx, error in enumerate(errors):
                    logger.error(f"Row {idx}: {error}")
            
            else:
            
                logger.info(f"Inserted {len(rows_to_insert)} rows into {self.table_ref}")
        
        except BadRequest as e:
            logger.error(f"Bad request error during BigQuery insertion (e.g., invalid data or schema): {e}")
        except Forbidden as e:
            logger.error(f"Forbidden error during BigQuery insertion (e.g., insufficient permissions): {e}")
        except NotFound as e:
            logger.error(f"Not found error during BigQuery insertion (e.g., table or dataset does not exist): {e}")
        except Conflict as e:
            logger.error(f"Conflict error during BigQuery insertion (e.g., duplicate or concurrent modification): {e}")
        except InternalServerError as e:
            logger.error(f"Internal server error during BigQuery insertion (transient GCP issue): {e}")
        except GoogleAPIError as e:
            logger.error(f"Other Google API error during BigQuery insertion: {e}")
        except Exception as e:
            logger.error(f"Unexpected error inserting metadata into BigQuery: {e}")

if __name__ == "__main__":
    # Example usage
    bq_manager = BQManager(GCP_PROJECT_ID, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_ID)
    sample_metadata = [
        {
            "id": "abc123",
            "title": "Sample Image",
            "author": "test_user",
            "subreddit": "test_subreddit",
            "score": 100,
            "url": "http://example.com/image.jpg",
            "filename": "image.jpg",
            "gcs_filename": "gcs_image.jpg",
            "gcs_url": "gs://bucket/gcs_image.jpg",
            "file_size_bytes": 2048,
            "content_type": "image/jpeg"
        }
    ]
    bq_manager.insert_metadata(sample_metadata)