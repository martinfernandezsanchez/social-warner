import logging
from google.cloud import storage

def load_file_from_bucket(storage_client, bucket_name, source_blob_name):
    """
    Loads a file from a Google Cloud Storage bucket into memory.

    Args:
        storage_client (storage.Client): Google Cloud Storage client.
        bucket_name (str): Name of the bucket.
        source_blob_name (str): Name of the file in the bucket.

    Returns:
        str: The content of the file as a string.
    """
    try:
        # Get the bucket
        bucket = storage_client.bucket(bucket_name)

        # Get the blob (file) from the bucket
        blob = bucket.blob(source_blob_name)

        # Download the file content as text
        file_content = blob.download_as_text()
        logging.info(f"File {source_blob_name} loaded from {bucket_name} into memory.")
        return file_content
    except Exception as e:
        logging.error(f"Error loading file {source_blob_name} from {bucket_name}: {e}")
        raise