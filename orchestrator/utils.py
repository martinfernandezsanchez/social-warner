import json
from typing import Optional
from datetime import datetime, timedelta
from google.cloud import storage, bigquery_datatransfer_v1

def initialize_storage():
    """
    Initialize and return the Google Cloud Storage client.
    
    Returns:
        storage.Client: Initialized Storage client.
    """
    storage_client = storage.Client()
    print(json.dumps({
        "severity": "DEBUG",
        "message": "Google Cloud Storage client initialized."
    }))
    return storage_client


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
        print(json.dumps({
            "severity": "INFO",
            "message": f"File {source_blob_name} loaded from {bucket_name} into memory."
        }))
        return file_content
    except Exception as e:
        print(json.dumps({
            "severity": "ERROR",
            "message": f"Error loading file {source_blob_name} from {bucket_name}: {e}"
        }))
        raise

def call_bq_rutine():
    # Initialize the client
    client = bigquery_datatransfer_v1.DataTransferServiceClient()
    transfer_config_name = f"projects/923134489511/locations/us/transferConfigs/67678662-0000-2917-a9b6-14223bc6ebde"

    # Define the time window for the Transfer Run
    # For a complete immediate run, you can set start_time = end_time = now
    now = datetime.utcnow()
    start_time = now

    # Create the Transfer Run request
    request = bigquery_datatransfer_v1.StartManualTransferRunsRequest(
        parent=transfer_config_name,
        requested_run_time=start_time
    )

    # Start the manual transfer run
    response = client.start_manual_transfer_runs(request=request)

    # Assuming there's only one run being triggered
    transfer_run = response.runs[0]

    print(f"Triggered Transfer Run:")
    print(f"Run Name: {transfer_run.name}")
    print(f"Run State: {transfer_run.state}")
    return "Cloud Function ejecutada correctamente."


def parse_date_input(date_input: str) -> Optional[str]:
    """
    Parses the date input which can be either in 'YYYY-MM-DD' format or in the '{{nDaysAgo N}}' format.
    
    Args:
        date_input (str): The date input string.
    
    Returns:
        Optional[str]: The parsed date in 'YYYY-MM-DD' format if successful, else None.
    """
    template_prefix = "{{nDaysAgo "
    template_suffix = "}}"

    if date_input.startswith(template_prefix) and date_input.endswith(template_suffix):
        try:
            # Extract the number of days ago
            n_days_str = date_input[len(template_prefix):-len(template_suffix)].strip()
            n_days = int(n_days_str)
            # Compute the date N days ago
            computed_date = (datetime.utcnow() - timedelta(days=n_days)).strftime('%Y-%m-%d')
            return computed_date
        except ValueError:
            # Invalid number format
            return None
    else:
        # Attempt to parse as standard date format
        try:
            datetime.strptime(date_input, '%Y-%m-%d')
            return date_input
        except ValueError:
            return None